import argparse
import boto3
import csv
import io
import json
import logging
import os
import sys
import time
from datetime import datetime
from typing import List, Dict
import yaml


# ------------------------- Logging -------------------------
def setup_logging():
    os.makedirs("logs", exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join("logs", f"run_{ts}.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(log_path, encoding="utf-8")],
    )
    logging.info(f"Logging to {log_path}")

# ------------------------- Config --------------------------
def load_config(env: str) -> dict:
    cfg_path = f"C:/Users/seera/AWS/src/catalog_sync/config/config_{env}.yaml"
    if not os.path.exists(cfg_path):
        cfg_path = "config/config.yaml"
    with open(cfg_path, "r") as f:
        cfg = yaml.safe_load(f)
    logging.info(f"Loaded config from {cfg_path}")
    return cfg

# --------------------- AWS Clients -------------------------
def clients(region: str):
    s3 = boto3.client("s3", region_name=region)
    glue = boto3.client("glue", region_name=region)
    sts = boto3.client("sts", region_name=region)
    return s3, glue, sts

# ----------------- Auto IAM Role Handling ------------------
def get_execution_role(cfg, sts):
    role_arn = cfg["aws"].get("role_arn")
    if role_arn:
        logging.info(f"Using role from config: {role_arn}")
        return role_arn
    try:
        account_id = sts.get_caller_identity()["Account"]
        role_arn = f"arn:aws:iam::{account_id}:role/AWSGlueServiceRole-DataEngineering"
        logging.info(f"Auto-detected role ARN: {role_arn}")
        return role_arn
    except Exception as e:
        logging.warning(f"Failed to auto-detect role, please verify config. Error: {e}")
        raise

# --------------- Streaming Schema Inference ----------------
def _infer_type(value: str) -> str:
    if value == "" or value is None:
        return "string"
    v = value.strip()
    if v.isdigit() or (v.startswith("-") and v[1:].isdigit()):
        return "int"
    try:
        float(v)
        return "double"
    except Exception:
        pass
    if v.lower() in {"true", "false"}:
        return "string"
    return "string"

def merge_types(t1: str, t2: str) -> str:
    if t1 == t2:
        return t1
    if "string" in (t1, t2):
        return "string"
    if {"int", "double"} == {t1, t2}:
        return "double"
    return "string"

def infer_schema_from_s3_csv_head(s3, bucket: str, key: str, sample_kb: int = 256) -> List[Dict[str, str]]:
    logging.info(f"Streaming head from s3://{bucket}/{key} (~{sample_kb}KB)")
    obj = s3.get_object(Bucket=bucket, Key=key)
    head_bytes = obj["Body"].read(sample_kb * 1024)
    try:
        head_bytes += obj["Body"].read(64 * 1024)
    except Exception:
        pass
    text = head_bytes.decode("utf-8", errors="ignore")
    reader = csv.reader(io.StringIO(text))
    rows = list(reader)
    if not rows:
        raise ValueError("CSV appears empty or unreadable.")
    headers = rows[0]
    col_types = ["string"] * len(headers)
    for r in rows[1: min(len(rows), 50)]:
        for i, val in enumerate(r + [""] * (len(headers) - len(r))):
            t = _infer_type(val)
            col_types[i] = merge_types(col_types[i], t)
    columns = [{"Name": h.strip() or f"col_{i+1}", "Type": t} for i, (h, t) in enumerate(zip(headers, col_types))]
    logging.info(f"Inferred columns: {columns}")
    return columns

# --------------------- Glue Catalog Ops --------------------
def ensure_landing_table(glue, database: str, table_name: str, s3_location: str, columns: List[Dict[str, str]]):
    try:
        glue.get_table(DatabaseName=database, Name=table_name)
        logging.info(f"Landing table exists: {database}.{table_name} (skipping create)")
        return
    except glue.exceptions.EntityNotFoundException:
        pass

    glue.create_table(
        DatabaseName=database,
        TableInput={
            "Name": table_name,
            "StorageDescriptor": {
                "Columns": columns,
                "Location": s3_location,
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
    logging.info(f"✅ Created landing table: {database}.{table_name} → {s3_location}")

# -------------------- Glue Job Creation --------------------
def upload_dynamic_job_script(s3, bucket: str, key: str, script_code: str):
    s3.put_object(Bucket=bucket, Key=key, Body=script_code.encode("utf-8"))
    logging.info(f"Uploaded Glue job script to s3://{bucket}/{key}")

def ensure_glue_job(glue, job_name: str, role_arn: str, script_location: str, glue_version: str, temp_dir: str):
    try:
        glue.get_job(JobName=job_name)
        logging.info(f"Glue job '{job_name}' exists; skipping creation.")
        return
    except glue.exceptions.EntityNotFoundException:
        pass
    glue.create_job(
        Name=job_name,
        Role=role_arn,
        ExecutionProperty={"MaxConcurrentRuns": 1},
        Command={"Name": "glueetl", "ScriptLocation": script_location, "PythonVersion": "3"},
        DefaultArguments={"--TempDir": temp_dir, "--job-language": "python"},
        GlueVersion=glue_version,
    )
    logging.info(f"✅ Created Glue job: {job_name}")

def run_and_wait(glue, job_name: str, temp_dir: str, poll_sec: int = 10) -> str:
    run = glue.start_job_run(JobName=job_name, Arguments={"--JOB_NAME": job_name, "--TempDir": temp_dir})
    run_id = run["JobRunId"]
    logging.info(f"🚀 Started Glue job: {job_name} | RunId: {run_id}")
    while True:
        jr = glue.get_job_run(JobName=job_name, RunId=run_id)["JobRun"]
        state = jr["JobRunState"]
        logging.info(f"[{job_name}] Status: {state}")
        if state in {"SUCCEEDED", "FAILED", "STOPPED"}:
            if "ErrorMessage" in jr:
                logging.error(f"[{job_name}] Error: {jr['ErrorMessage']}")
            break
        time.sleep(poll_sec)
    return state

# ------------------------ Main Flow ------------------------
def process_all_files(env: str):
    setup_logging()
    cfg = load_config(env)
    region = cfg["aws"]["region"]
    bucket = cfg["aws"]["bucket_name"]
    landing_db = cfg["glue"]["landing_db"]
    fdp_db = cfg["glue"]["fdp_db"]
    glue_version = cfg["glue"]["glue_version"]
    temp_dir = cfg["glue"]["temp_dir"]
    source_prefix = cfg["s3_paths"]["source_prefix"]
    target_prefix_root = cfg["s3_paths"]["target_prefix"]
    scripts_prefix = cfg["s3_paths"].get("script_prefix", "scripts/")

    s3, glue, sts = clients(region)
    role_arn = get_execution_role(cfg, sts)

    # List all CSVs under source prefix
    logging.info(f"Listing CSV files under s3://{bucket}/{source_prefix}")
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=source_prefix)
    contents = resp.get("Contents", [])
    csv_keys = [o["Key"] for o in contents if o["Key"].lower().endswith(".csv")]

    if not csv_keys:
        logging.warning("⚠️ No CSV files found in source path.")
        return

    summary = []
    for key in csv_keys:
        try:
            table_name = os.path.splitext(os.path.basename(key))[0]
            logging.info(f"==== Processing file: s3://{bucket}/{key} → table '{table_name}' ====")

            # 1️⃣ Infer schema
            columns = infer_schema_from_s3_csv_head(s3, bucket, key)

            # 2️⃣ Ensure landing table
            landing_location = f"s3://{bucket}/{source_prefix}"
            ensure_landing_table(glue, landing_db, table_name, landing_location, columns)

            # 3️⃣ Build dynamic Glue job
            fdp_target_prefix = os.path.join(target_prefix_root, table_name) + "/"

            dynamic_script = f"""
import sys, boto3, json
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args['JOB_NAME'], args)

source_path = '{landing_location}'
print(f"Reading source: {{source_path}}")
df = spark.read.option('header', True).csv(source_path)
print(f"Read records: {{df.count()}}")

target_path = 's3://{bucket}/{fdp_target_prefix}'
df.write.mode('overwrite').option('header', True).csv(target_path)
print(f"✅ Data copied to FDP: {{target_path}}")

glue_client = boto3.client('glue', region_name='{region}')
columns = {json.dumps(columns)}

try:
    glue_client.get_table(DatabaseName='{fdp_db}', Name='{table_name}')
    print('ℹ️ FDP table exists (skipping create).')
except glue_client.exceptions.EntityNotFoundException:
    glue_client.create_table(
        DatabaseName='{fdp_db}',
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
    print('✅ Created FDP table: {fdp_db}.{table_name}')
job.commit()
"""

            # 4️⃣ Upload dynamic job script
            script_key = f"{scripts_prefix}{table_name}_fdp_writer.py"
            upload_dynamic_job_script(s3, bucket, script_key, dynamic_script)
            script_location = f"s3://{bucket}/{script_key}"

            # 5️⃣ Ensure Glue job exists
            job_name = f"fdp_writer_{table_name}"
            ensure_glue_job(glue, job_name, role_arn, script_location, glue_version, temp_dir)

            # 6️⃣ Run and monitor
            state = run_and_wait(glue, job_name, temp_dir)
            summary.append((table_name, state))
        except Exception as e:
            logging.exception(f"❌ Failed processing {key}: {e}")
            summary.append((os.path.basename(key), f"FAILED: {e}"))

    # ✅ Summary
    logging.info("======== SUMMARY ========")
    for t, st in summary:
        logging.info(f"{t}: {st}")

# ------------------------ Entrypoint ------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Enhanced metadata-driven S3 → Glue Catalog → FDP loader")
    parser.add_argument("--env", default="dev", help="Environment key: dev|test|prod")
    args = parser.parse_args()
    process_all_files(args.env)
