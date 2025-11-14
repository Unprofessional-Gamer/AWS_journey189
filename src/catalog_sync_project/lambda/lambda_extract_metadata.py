import boto3
import csv
import json

s3 = boto3.client('s3')
glue = boto3.client('glue')
glue_client = boto3.client("glue")

BUCKET = "catalog-sync-testing"
LANDING_DB = "landing_db"
SOURCE_PREFIX = "source_catalog_file/"

def lambda_handler(event, context):
    try:
        # 1️⃣ Get S3 file details
        s3_info = event['Records'][0]['s3']
        key = s3_info['object']['key']

        if not key.startswith(SOURCE_PREFIX) or not key.endswith(".csv"):
            print(f"Skipping non-CSV file: {key}")
            return

        print(f"✅ File detected: {key}")

        # 2️⃣ Read first row (header) to infer schema
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        header = obj['Body'].read().decode('utf-8').splitlines()[0].split(',')

        schema = [{"Name": col, "Type": "string"} for col in header]

        table_name = key.split('/')[-1].replace('.csv', '').lower()

        # 3️⃣ Create Glue Table in landing_db
        glue.create_table(
            DatabaseName=LANDING_DB,
            TableInput={
                "Name": table_name,
                "StorageDescriptor": {
                    "Columns": schema,
                    "Location": f"s3://{BUCKET}/{SOURCE_PREFIX}",
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.serde2.OpenCSVSerde",
                        "Parameters": {"separatorChar": ",", "skip.header.line.count": "1"}
                    }
                },
                "TableType": "EXTERNAL_TABLE"
            }
        )

        print(f"✅ Glue table created: {LANDING_DB}.{table_name}")

        # 4️⃣ Trigger Glue Sync Job
        glue_client.start_job_run(JobName="sync_copy_job")
        print(f"🚀 Triggered Glue Job: sync_copy_job")

    except Exception as e:
        print(f"❌ Error: {e}")
