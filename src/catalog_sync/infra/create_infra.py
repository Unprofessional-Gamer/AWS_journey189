import boto3
import yaml
import json
import os

# Load configuration
with open("C:/Users/seera/AWS/src/catalog_sync/config/config.yaml", "r") as f:
    CONFIG = yaml.safe_load(f)

REGION = CONFIG["aws"]["region"]
BUCKET = CONFIG["aws"]["bucket_name"]
ROLE_NAME = CONFIG["aws"]["role_name"]
LANDING_DB = CONFIG["glue"]["landing_db"]
FDP_DB = CONFIG["glue"]["fdp_db"]
LOG_DB = CONFIG.get("glue", {}).get("log_db", "log_db")

# Initialize AWS clients
s3 = boto3.client("s3", region_name=REGION)
glue = boto3.client("glue", region_name=REGION)
iam = boto3.client("iam", region_name=REGION)

# ---- Step 1: Create S3 bucket ----
try:
    s3.create_bucket(
        Bucket=BUCKET,
        CreateBucketConfiguration={"LocationConstraint": REGION}
    )
    print(f"✅ Created bucket: {BUCKET}")
except s3.exceptions.BucketAlreadyOwnedByYou:
    print(f"ℹ️ Bucket '{BUCKET}' already exists.")

# Create S3 folders
folders = [
    CONFIG["s3_paths"]["source_prefix"],
    CONFIG["s3_paths"]["target_prefix"],
    CONFIG["s3_paths"]["script_prefix"],
    "temp/"
]
for folder in folders:
    s3.put_object(Bucket=BUCKET, Key=(folder))
print("✅ S3 structure prepared.")

# ---- Upload sample data files into source prefix so landing tables have data ----
sample_objects = {
    "sample_data.csv": (
        "customer_id,name,email,age,salary,department,is_active,joining_date\n"
        "CSV001,Anna Blue,anna.blue@example.com,30,72000.00,Engineering,true,2021-04-01\n"
        "CSV002,Greg White,greg.white@example.com,40,88000.00,Sales,true,2019-08-12\n"
        "CSV003,Linda Green,linda.green@example.com,27,61000.00,Marketing,false,2022-01-25\n"
    ),
    "sample_data.json": (
        "[\n"
        "  {\"id\": \"JSON-A\", \"product\": \"Widget\", \"qty\": 10, \"price\": 9.99},\n"
        "  {\"id\": \"JSON-B\", \"product\": \"Gadget\", \"qty\": 5, \"price\": 19.95},\n"
        "  {\"id\": \"JSON-C\", \"product\": \"Thingamajig\", \"qty\": 2, \"price\": 199.99}\n"
        "]\n"
    ),
    "sample_data.xml": (
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        "<orders>\n"
        "  <order>\n"
        "    <order_id>XML-1001</order_id>\n"
        "    <customer>Alpha</customer>\n"
        "    <amount>250.75</amount>\n"
        "  </order>\n"
        "  <order>\n"
        "    <order_id>XML-1002</order_id>\n"
        "    <customer>Beta</customer>\n"
        "    <amount>99.50</amount>\n"
        "  </order>\n"
        "</orders>\n"
    ),
}

for fname, content in sample_objects.items():
    key = CONFIG["s3_paths"]["source_prefix"] + fname
    try:
        s3.put_object(Bucket=BUCKET, Key=key, Body=content.encode("utf-8"))
        print(f"✅ Uploaded sample file to s3://{BUCKET}/{key}")
    except Exception as e:
        print(f"⚠️ Failed to upload sample file {fname} to S3: {e}")

# ---- Step 2: Create IAM Role for Glue ----
trust_policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {"Service": "glue.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }
    ]
}

permissions_policy = {
    "Version": "2012-10-17",
    "Statement": [
        {"Effect": "Allow", "Action": "s3:*", "Resource": [f"arn:aws:s3:::{BUCKET}", f"arn:aws:s3:::{BUCKET}/*"]},
        {"Effect": "Allow", "Action": "glue:*", "Resource": "*"},
        {"Effect": "Allow", "Action": "logs:*", "Resource": "*"}
    ]
}

try:
    iam.create_role(
        RoleName=ROLE_NAME,
        AssumeRolePolicyDocument=json.dumps(trust_policy),
        Description="Role for Glue ETL full access"
    )
    iam.put_role_policy(
        RoleName=ROLE_NAME,
        PolicyName="GlueFullAccessPolicy",
        PolicyDocument=json.dumps(permissions_policy)
    )
    print(f"✅ Created IAM role: {ROLE_NAME}")
except iam.exceptions.EntityAlreadyExistsException:
    print(f"ℹ️ IAM role '{ROLE_NAME}' already exists.")

# ---- Step 3: Create Glue Databases ----
for db_name in [LANDING_DB, FDP_DB, LOG_DB]:
    try:
        glue.create_database(DatabaseInput={"Name": db_name})
        print(f"✅ Created Glue database: {db_name}")
    except glue.exceptions.AlreadyExistsException:
        print(f"ℹ️ Glue database '{db_name}' already exists.")


# ---- Step 4: Create sample landing tables with naming convention ----
def make_table_name(prefix: str, system_code: str, batch_id: str, filename: str) -> str:
    # sanitize filename (remove extension)
    base = os.path.splitext(os.path.basename(filename))[0]
    return f"{prefix}_{system_code}_{batch_id}_{base}"

system_code = CONFIG.get("meta", {}).get("system_code", "SYS01")
from datetime import datetime
batch_id = datetime.now().strftime("%Y%m%d%H%M%S")

# Define columns for each landing table based on sample data
table_schemas = {
    "sample_data.csv": [
        {"Name": "customer_id", "Type": "string"},
        {"Name": "name", "Type": "string"},
        {"Name": "email", "Type": "string"},
        {"Name": "age", "Type": "int"},
        {"Name": "salary", "Type": "double"},
        {"Name": "department", "Type": "string"},
    ],
    "sample_data.json": [
        {"Name": "id", "Type": "string"},
        {"Name": "product", "Type": "string"},
        {"Name": "qty", "Type": "int"},
        {"Name": "price", "Type": "double"},
        {"Name": "category", "Type": "string"},
        {"Name": "in_stock", "Type": "string"},
    ],
    "sample_data.xml": [
        {"Name": "order_id", "Type": "string"},
        {"Name": "customer", "Type": "string"},
        {"Name": "amount", "Type": "double"},
        {"Name": "status", "Type": "string"},
        {"Name": "order_date", "Type": "string"},
        {"Name": "region", "Type": "string"},
    ],
}

# choose three sample files (if present) to create landing tables
sample_files = ["sample_data.csv", "sample_data.json", "sample_data.xml"]
created_tables = []
for fname in sample_files:
    table_name = make_table_name("landing", system_code, batch_id, fname)
    columns = table_schemas.get(fname, [{"Name": "raw", "Type": "string"}])
    try:
        glue.create_table(
            DatabaseName=LANDING_DB,
            TableInput={
                "Name": table_name,
                "StorageDescriptor": {
                    "Columns": columns,
                    "Location": f"s3://{BUCKET}/{CONFIG['s3_paths']['source_prefix']}",
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                        "Parameters": {"field.delim": ","},
                    },
                },
                "TableType": "EXTERNAL_TABLE",
            },
        )
        print(f"✅ Created landing table: {LANDING_DB}.{table_name} with {len(columns)} columns")
        created_tables.append(table_name)
    except glue.exceptions.AlreadyExistsException:
        print(f"ℹ️ Landing table '{LANDING_DB}.{table_name}' already exists.")

# ---- Step 5: Create Log DB batch log table ----
try:
    log_table_name = CONFIG.get("glue", {}).get("log_table", "batch_log")
    glue.create_table(
        DatabaseName=LOG_DB,
        TableInput={
            "Name": log_table_name,
            "StorageDescriptor": {
                "Columns": [
                    {"Name": "batch_id", "Type": "string"},
                    {"Name": "source_file", "Type": "string"},
                    {"Name": "table_name", "Type": "string"},
                    {"Name": "status", "Type": "string"},
                    {"Name": "started_at", "Type": "string"},
                    {"Name": "finished_at", "Type": "string"},
                    {"Name": "message", "Type": "string"},
                ],
                "Location": f"s3://{BUCKET}/logs/",
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                    "Parameters": {"field.delim": ","},
                },
            },
            "TableType": "EXTERNAL_TABLE",
        },
    )
    print(f"✅ Created log table: {LOG_DB}.{log_table_name}")
except glue.exceptions.AlreadyExistsException:
    print(f"ℹ️ Log table '{LOG_DB}.{log_table_name}' already exists.")

# ---- Step 6: Create FDP DB tables (mirror of landing tables) ----
print("\n📋 Creating FDP tables...")
for fname in sample_files:
    fdp_table_name = make_table_name("fdp", system_code, batch_id, fname)
    fdp_location = f"s3://{BUCKET}/{CONFIG['s3_paths']['target_prefix']}{os.path.splitext(fname)[0]}/"
    columns = table_schemas.get(fname, [{"Name": "raw", "Type": "string"}])
    
    try:
        glue.create_table(
            DatabaseName=FDP_DB,
            TableInput={
                "Name": fdp_table_name,
                "StorageDescriptor": {
                    "Columns": columns,
                    "Location": fdp_location,
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                        "Parameters": {"field.delim": ","},
                    },
                },
                "TableType": "EXTERNAL_TABLE",
            },
        )
        print(f"✅ Created FDP table: {FDP_DB}.{fdp_table_name} with {len(columns)} columns")
    except glue.exceptions.AlreadyExistsException:
        print(f"ℹ️ FDP table '{FDP_DB}.{fdp_table_name}' already exists.")

# ---- Step 7: Copy sample data from landing to FDP zone in S3 ----
print("\n📦 Copying sample data from landing to FDP zone...")
source_prefix = CONFIG["s3_paths"]["source_prefix"]
target_prefix = CONFIG["s3_paths"]["target_prefix"]

for fname in sample_objects.keys():
    source_key = source_prefix + fname
    base_name = os.path.splitext(fname)[0]
    target_key = target_prefix + base_name + "/" + fname
    
    try:
        # Copy object from landing to FDP zone
        copy_source = {"Bucket": BUCKET, "Key": source_key}
        s3.copy_object(CopySource=copy_source, Bucket=BUCKET, Key=target_key)
        print(f"✅ Copied s3://{BUCKET}/{source_key} → s3://{BUCKET}/{target_key}")
    except Exception as e:
        print(f"⚠️ Failed to copy {fname} to FDP zone: {e}")

print("\n🎉 Infrastructure setup complete!")
print(f"Landing DB: {LANDING_DB} (3 tables)")
print(f"FDP DB: {FDP_DB} (3 tables)")
print(f"Log DB: {LOG_DB} (1 table)")
print(f"Sample data copied to FDP zone under s3://{BUCKET}/{target_prefix}")

