import boto3
import json
import os
import zipfile

# ✅ Resolve base directory so we can run script from anywhere
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, "config", "settings.json")

# ✅ Load settings from config/settings.json
with open(CONFIG_PATH) as f:
    config = json.load(f)

REGION = config["aws_region"]
BUCKET = config["s3_bucket"]
PREFIX_SCRIPTS = config["prefixes"]["scripts"]

GLUE_DB_LANDING = config["glue"]["landing_db"]
GLUE_DB_FDP = config["glue"]["fdp_db"]
GLUE_ROLE_NAME = config["glue"]["glue_role_name"]
GLUE_JOB_NAME = config["glue"]["glue_job_name"]

LAMBDA_FUNCTION_NAME = config["lambda"]["function_name"]
LAMBDA_ROLE_NAME = config["lambda"]["role_name"]

# ✅ AWS Clients
s3 = boto3.client("s3", region_name=REGION)
lambda_client = boto3.client("lambda", region_name=REGION)
glue = boto3.client("glue", region_name=REGION)
sts = boto3.client("sts")

# ✅ Automatically fetch AWS Account ID
ACCOUNT_ID = sts.get_caller_identity()["Account"]
print(f"✅ AWS Account ID detected: {ACCOUNT_ID}")

# =========================================================
# 1. ZIP & Upload Lambda Function
# =========================================================
def package_lambda():
    lambda_zip = os.path.join(BASE_DIR, "lambda_deploy.zip")
    lambda_file = os.path.join(BASE_DIR, "lambda", "lambda_extract_metadata.py")

    with zipfile.ZipFile(lambda_zip, "w") as zipf:
        zipf.write(lambda_file, arcname="lambda_function.py")

    print("✅ Lambda zipped:", lambda_zip)
    return lambda_zip

def create_or_update_lambda():
    zip_file = package_lambda()
    with open(zip_file, "rb") as f:
        code = f.read()

    lambda_arn = f"arn:aws:iam::{ACCOUNT_ID}:role/{LAMBDA_ROLE_NAME}"

    try:
        lambda_client.get_function(FunctionName=LAMBDA_FUNCTION_NAME)
        lambda_client.update_function_code(
            FunctionName=LAMBDA_FUNCTION_NAME,
            ZipFile=code
        )
        print(f"ℹ️ Lambda updated: {LAMBDA_FUNCTION_NAME}")
    except lambda_client.exceptions.ResourceNotFoundException:
        lambda_client.create_function(
            FunctionName=LAMBDA_FUNCTION_NAME,
            Runtime="python3.9",
            Handler="lambda_function.lambda_handler",
            Role=lambda_arn,
            Code={"ZipFile": code},
            Timeout=60,
            MemorySize=256
        )
        print(f"✅ Lambda created: {LAMBDA_FUNCTION_NAME}")

# =========================================================
# 2. Upload Glue Script & Create Glue Job
# =========================================================
def upload_glue_script():
    source_path = os.path.join(BASE_DIR, "glue_jobs", "glue_sync_copy_job.py")
    s3_key = f"{PREFIX_SCRIPTS}glue_sync_copy_job.py"
    s3.upload_file(source_path, BUCKET, s3_key)
    print("✅ Uploaded Glue script to S3:", s3_key)

def create_glue_job():
    script_path = f"s3://{BUCKET}/{PREFIX_SCRIPTS}glue_sync_copy_job.py"
    try:
        glue.get_job(JobName=GLUE_JOB_NAME)
        print(f"ℹ️ Glue job {GLUE_JOB_NAME} already exists (skipping)")
    except glue.exceptions.EntityNotFoundException:
        glue.create_job(
            Name=GLUE_JOB_NAME,
            Role=GLUE_ROLE_NAME,
            Command={
                "Name": "glueetl",
                "ScriptLocation": script_path,
                "PythonVersion": "3"
            },
            GlueVersion="3.0",
            DefaultArguments={
                "--enable-glue-datacatalog": "true"
            }
        )
        print("✅ Glue job created:", GLUE_JOB_NAME)

# =========================================================
# 3. S3 Event Trigger for Lambda
# =========================================================
def create_s3_trigger():
    lambda_arn = (
        f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{LAMBDA_FUNCTION_NAME}"
    )
    try:
        lambda_client.add_permission(
            FunctionName=LAMBDA_FUNCTION_NAME,
            StatementId="AllowS3Invoke",
            Action="lambda:InvokeFunction",
            Principal="s3.amazonaws.com",
            SourceArn=f"arn:aws:s3:::{BUCKET}"
        )
    except:
        pass  # Ignore if already exists

    s3.put_bucket_notification_configuration(
        Bucket=BUCKET,
        NotificationConfiguration={
            "LambdaFunctionConfigurations": [
                {
                    "LambdaFunctionArn": lambda_arn,
                    "Events": ["s3:ObjectCreated:*"],
                    "Filter": {
                        "Key": {
                            "FilterRules": [
                                {"Name": "prefix", "Value": "source_catalog_file/"}
                            ]
                        }
                    }
                }
            ]
        }
    )
    print("✅ S3 → Lambda trigger configured successfully")

# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    print("\n🚀 Deploying AWS Catalog Sync Pipeline...\n")

    upload_glue_script()
    create_or_update_lambda()
    create_glue_job()
    create_s3_trigger()

    print("\n🎉 Deployment Complete! System is LIVE.\n")
    print("✅ Upload a CSV file to:")
    print("   s3://catalog-sync-testing/source_catalog_file/")
    print("👉 Lambda will auto-create Glue Table, then Glue Job copies to FDP.")
