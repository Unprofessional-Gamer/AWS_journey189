import boto3
import yaml
import json
import os
from datetime import datetime

# -------------------------
# Load configuration
# -------------------------
CONFIG_PATH = "C:/Users/seera/AWS/src/catalog_sync/config/config.yaml"
with open(CONFIG_PATH, "r") as f:
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

# -------------------------
# Step 1: Create S3 bucket
# -------------------------
try:
    # Note: For us-east-1, CreateBucketConfiguration must be omitted.
    if REGION == "us-east-1":
        s3.create_bucket(Bucket=BUCKET)
    else:
        s3.create_bucket(
            Bucket=BUCKET,
            CreateBucketConfiguration={"LocationConstraint": REGION}
        )
    print(f"✅ Created bucket: {BUCKET}")
except s3.exceptions.BucketAlreadyOwnedByYou:
    print(f"ℹ️ Bucket '{BUCKET}' already exists.")
except Exception as e:
    print(f"⚠️ Error creating bucket {BUCKET}: {e}")

# Create S3 folders (prefixes)
folders = [
    CONFIG["s3_paths"]["source_prefix"],
    CONFIG["s3_paths"]["target_prefix"],
    CONFIG["s3_paths"]["script_prefix"],
    "temp/"
]
for folder in folders:
    try:
        # Put a zero-byte object to create a prefix "folder"
        s3.put_object(Bucket=BUCKET, Key=(folder if folder.endswith("/") else folder))
    except Exception as e:
        print(f"⚠️ Failed to ensure folder {folder} in bucket {BUCKET}: {e}")
print("✅ S3 structure prepared.")

# -------------------------
# Upload sample data files into source prefix so landing tables have data
# -------------------------
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

source_prefix = CONFIG["s3_paths"]["source_prefix"]

for fname, content in sample_objects.items():
    key = source_prefix + fname
    try:
        s3.put_object(Bucket=BUCKET, Key=key, Body=content.encode("utf-8"))
        print(f"✅ Uploaded sample file to s3://{BUCKET}/{key}")
    except Exception as e:
        print(f"⚠️ Failed to upload sample file {fname} to S3: {e}")

# -------------------------
# Step 2: Create IAM Role for Glue
# -------------------------
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
except Exception as e:
    print(f"⚠️ Error creating IAM role {ROLE_NAME}: {e}")

# -------------------------
# Step 3: Create Glue Databases
# -------------------------
for db_name in [LANDING_DB, FDP_DB, LOG_DB]:
    try:
        glue.create_database(DatabaseInput={"Name": db_name})
        print(f"✅ Created Glue database: {db_name}")
    except glue.exceptions.AlreadyExistsException:
        print(f"ℹ️ Glue database '{db_name}' already exists.")
    except Exception as e:
        print(f"⚠️ Error creating Glue database {db_name}: {e}")

# -------------------------
# Step 4: Create the TWO Landing Tables (clean environment first)
# -------------------------
print("\n🗑️ Cleaning up old landing tables...")
try:
    existing_tables = glue.get_tables(DatabaseName=LANDING_DB).get("TableList", [])
    for existing_table in existing_tables:
        try:
            glue.delete_table(DatabaseName=LANDING_DB, Name=existing_table["Name"])
            print(f"✅ Deleted old landing table: {LANDING_DB}.{existing_table['Name']}")
        except Exception as e:
            print(f"⚠️ Failed to delete landing table {existing_table['Name']}: {e}")
except Exception as e:
    print(f"ℹ️ No existing landing tables to delete or error accessing: {e}")

# Define the two landing tables exactly as confirmed
landing_tables = {
    "landing_sys01_2025": [
        {"Name": "col1", "Type": "string"},
        {"Name": "col2", "Type": "int"},
        {"Name": "col3", "Type": "string"},
        {"Name": "col4", "Type": "double"},
        {"Name": "col5", "Type": "string"},
    ],
    "landing_sys01_2026": [
        {"Name": "col1", "Type": "string"},
        {"Name": "col2", "Type": "string"},
        {"Name": "col3", "Type": "int"},
        {"Name": "col4", "Type": "double"},
        {"Name": "col5", "Type": "string"},
        {"Name": "col6", "Type": "string"},
        {"Name": "col7", "Type": "string"},
    ]
}

print("\n📌 Creating required landing tables...")
for table_name, columns in landing_tables.items():
    try:
        glue.create_table(
            DatabaseName=LANDING_DB,
            TableInput={
                "Name": table_name,
                "StorageDescriptor": {
                    "Columns": columns,
                    "Location": f"s3://{BUCKET}/{source_prefix}",
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
        print(f"✅ Created landing table: {LANDING_DB}.{table_name} ({len(columns)} columns)")
    except glue.exceptions.AlreadyExistsException:
        print(f"ℹ️ Landing table '{LANDING_DB}.{table_name}' already exists.")
    except Exception as e:
        print(f"⚠️ Failed to create landing table {LANDING_DB}.{table_name}: {e}")

# -------------------------
# Step 5: Create Log DB batch log table
# -------------------------
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
except Exception as e:
    print(f"⚠️ Failed to create log table {LOG_DB}.{log_table_name}: {e}")

# -------------------------
# Step 6: Copy sample data from landing to FDP zone in S3
# -------------------------
print("\n📦 Copying sample data from landing to FDP zone...")
target_prefix = CONFIG["s3_paths"]["target_prefix"]

for fname in sample_objects.keys():
    source_key = source_prefix + fname
    base_name = os.path.splitext(fname)[0]
    target_key = target_prefix + base_name + "/" + fname

    try:
        copy_source = {"Bucket": BUCKET, "Key": source_key}
        s3.copy_object(CopySource=copy_source, Bucket=BUCKET, Key=target_key)
        print(f"✅ Copied s3://{BUCKET}/{source_key} → s3://{BUCKET}/{target_key}")
    except Exception as e:
        print(f"⚠️ Failed to copy {fname} to FDP zone: {e}")

print("\n🎉 Infrastructure setup complete!")
print(f"Landing DB: {LANDING_DB} (created tables: {', '.join(landing_tables.keys())})")
print(f"Log DB: {LOG_DB} (table: {log_table_name})")
print(f"Sample data available at s3://{BUCKET}/{source_prefix}")


