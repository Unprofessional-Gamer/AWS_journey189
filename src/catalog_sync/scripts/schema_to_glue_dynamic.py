import boto3
import yaml
import pandas as pd
import io
import os
import json
import time

# ---------------- CONFIG LOAD ----------------
with open("C:/Users/seera/AWS/src/catalog_sync/config/config.yaml", "r") as f:
    CONFIG = yaml.safe_load(f)

REGION = CONFIG["aws"]["region"]
ROLE_ARN = CONFIG["aws"]["role_arn"]
BUCKET = CONFIG["aws"]["bucket_name"]
SOURCE_PREFIX = CONFIG["s3_paths"]["source_prefix"]
TARGET_PREFIX = CONFIG["s3_paths"]["target_prefix"]
LANDING_DB = CONFIG["glue"]["landing_db"]
FDP_DB = CONFIG["glue"]["fdp_db"]
GLUE_VERSION = CONFIG["glue"]["glue_version"]
TEMP_DIR = CONFIG["glue"]["temp_dir"]
JOB_NAME = CONFIG["glue"]["dynamic_job_name"]

s3 = boto3.client("s3", region_name=REGION)
glue = boto3.client("glue", region_name=REGION)

# ---------------- STEP 1: READ CSV FROM S3 ----------------
def read_csv_from_s3(bucket, prefix):
    print(f"🔍 Fetching CSV file from s3://{bucket}/{prefix}")
    obj = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files = [item["Key"] for item in obj.get("Contents", []) if item["Key"].endswith(".csv")]
    if not files:
        raise Exception("❌ No CSV files found in source path.")
    key = files[0]
    file_obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(file_obj["Body"].read()))
    print(f"✅ Loaded file: {key} — Rows: {len(df)} | Columns: {list(df.columns)}")
    return df, key

df, key = read_csv_from_s3(BUCKET, SOURCE_PREFIX)
table_name = os.path.splitext(os.path.basename(key))[0]

# ---------------- STEP 2: CREATE LANDING_DB TABLE ----------------
columns = []
for col, dtype in zip(df.columns, df.dtypes):
    glue_type = "string"
    if "int" in str(dtype):
        glue_type = "int"
    elif "float" in str(dtype):
        glue_type = "double"
    columns.append({"Name": col, "Type": glue_type})

try:
    glue.create_table(
        DatabaseName=LANDING_DB,
        TableInput={
            "Name": table_name,
            "StorageDescriptor": {
                "Columns": columns,
                "Location": f"s3://{BUCKET}/{SOURCE_PREFIX}",
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
    print(f"✅ Created Glue table: {LANDING_DB}.{table_name}")
except glue.exceptions.AlreadyExistsException:
    print(f"ℹ️ Table already exists: {LANDING_DB}.{table_name}")

# ---------------- STEP 3: GENERATE GLUE JOB DYNAMICALLY ----------------
landing_table = glue.get_table(DatabaseName=LANDING_DB, Name=table_name)
source_path = landing_table["Table"]["StorageDescriptor"]["Location"]
print(f"📁 Source path (from Glue Catalog): {source_path}")

# Dynamic Glue job script
job_script_code = f"""
import sys, boto3, json
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Read job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Glue job
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args['JOB_NAME'], args)

print('🚀 Starting FDP data copy process...')

# Read data from landing source
source_path = '{source_path}'
df = spark.read.option('header', True).csv(source_path)
print(f"✅ Data read from: {{source_path}} | Records: {{df.count()}}")

# Write data to FDP zone
target_path = 's3://{BUCKET}/{TARGET_PREFIX}'
df.write.mode('overwrite').option('header', True).csv(target_path)
print(f"✅ Data copied to FDP path: {{target_path}}")

# Create FDP Glue Catalog Table
glue = boto3.client('glue', region_name='{REGION}')
columns = {json.dumps(columns)}

try:
    glue.create_table(
        DatabaseName='{FDP_DB}',
        TableInput={{
            'Name': '{table_name}',
            'StorageDescriptor': {{
                'Columns': columns,
                'Location': target_path,
                'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                'SerdeInfo': {{
                    'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                    'Parameters': {{'field.delim': ','}}
                }}
            }},
            'TableType': 'EXTERNAL_TABLE'
        }}
    )
    print('✅ Created FDP Glue table: {FDP_DB}.{table_name}')
except glue.exceptions.AlreadyExistsException:
    print('ℹ️ FDP table already exists.')

job.commit()
print('🎉 FDP data copy + catalog creation completed successfully.')
"""

# Upload dynamic Glue job script to S3
script_s3_key = f"scripts/{table_name}_fdp_writer.py"
s3.put_object(Bucket=BUCKET, Key=script_s3_key, Body=job_script_code.encode("utf-8"))
script_location = f"s3://{BUCKET}/{script_s3_key}"
print(f"✅ Uploaded dynamic Glue job script: {script_location}")

# Ensure FDP target path exists
s3.put_object(Bucket=BUCKET, Key=(TARGET_PREFIX))
print(f"📁 Ensured FDP target path exists: s3://{BUCKET}/{TARGET_PREFIX}")

# ---------------- STEP 4: CREATE GLUE JOB IF NOT EXISTS ----------------
try:
    glue.get_job(JobName=JOB_NAME)
    print(f"ℹ️ Glue job '{JOB_NAME}' already exists, skipping creation.")
except glue.exceptions.EntityNotFoundException:
    glue.create_job(
        Name=JOB_NAME,
        Role=ROLE_ARN,
        ExecutionProperty={"MaxConcurrentRuns": 1},
        Command={"Name": "glueetl", "ScriptLocation": script_location, "PythonVersion": "3"},
        DefaultArguments={
            "--TempDir": TEMP_DIR,
            "--job-language": "python",
        },
        GlueVersion=GLUE_VERSION,
    )
    print(f"✅ Created new Glue job: {JOB_NAME}")

# ---------------- STEP 5: RUN GLUE JOB ----------------
run_id = glue.start_job_run(
    JobName=JOB_NAME,
    Arguments={
        "--JOB_NAME": JOB_NAME,
        "--TempDir": TEMP_DIR,
    },
)
print(f"🚀 Started Glue job: {JOB_NAME} | Run ID: {run_id['JobRunId']}")

# Wait for job completion
while True:
    job_run = glue.get_job_run(JobName=JOB_NAME, RunId=run_id["JobRunId"])
    status = job_run["JobRun"]["JobRunState"]
    print(f"⌛ Job status: {status}")
    if status in ["SUCCEEDED", "FAILED", "STOPPED"]:
        print(f"🏁 Job finished with status: {status}")
        if "ErrorMessage" in job_run["JobRun"]:
            print(f"❌ Error Message: {job_run['JobRun']['ErrorMessage']}")
        break
    time.sleep(10)
