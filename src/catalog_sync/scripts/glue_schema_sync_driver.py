#!/usr/bin/env python3
"""
glue_schema_sync_driver.py

- Scans landing Glue DB for tables with configured prefix
- For each table: generates a Glue job script (metadata-only), uploads it to S3
- Creates Glue job (if not exists) and starts the job
- Reports job run ids & S3 temp report location at the end

Usage:
python scripts/glue_schema_sync_driver.py --env dev
"""

import argparse
import boto3
import json
import logging
import os
import sys
import time
from datetime import datetime
import yaml

# ---------- Logging ----------
def setup_logging():
    os.makedirs("logs", exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%SZ")
    logfile = f"logs/driver_{ts}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(logfile, encoding="utf-8")]
    )
    logging.info("Driver logging -> %s", logfile)

# ---------- Config ----------
def load_config(env):
    path = f"config/config_{env}.yaml"
    if not os.path.exists(path):
        path = "config/config.yaml"
    with open(path, "r") as f:
        return yaml.safe_load(f)

# ---------- AWS clients ----------
def clients(region):
    session = boto3.session.Session(region_name=region)
    s3 = session.client("s3")
    glue = session.client("glue")
    sts = session.client("sts")
    return s3, glue, sts

# ---------- Helpers ----------
def upload_script(s3, bucket, key, content):
    s3.put_object(Bucket=bucket, Key=key, Body=content.encode("utf-8"))
    return f"s3://{bucket}/{key}"

def ensure_glue_job(glue, job_name, role_arn, script_location, glue_version, temp_dir):
    try:
        glue.get_job(JobName=job_name)
        logging.info("Glue job exists: %s", job_name)
    except glue.exceptions.EntityNotFoundException:
        glue.create_job(
            Name=job_name,
            Role=role_arn,
            ExecutionProperty={"MaxConcurrentRuns": 1},
            Command={"Name": "glueetl", "ScriptLocation": script_location, "PythonVersion": "3"},
            DefaultArguments={"--TempDir": temp_dir, "--job-language": "python"},
            GlueVersion=glue_version,
        )
        logging.info("Created Glue job: %s", job_name)

def start_job(glue, job_name, temp_dir):
    run = glue.start_job_run(JobName=job_name, Arguments={"--JOB_NAME": job_name, "--TempDir": temp_dir})
    logging.info("Started job %s runId=%s", job_name, run["JobRunId"])
    return run["JobRunId"]

def list_tables_with_prefix(glue, database, prefix):
    paginator = glue.get_paginator("get_tables")
    tables = []
    for page in paginator.paginate(DatabaseName=database):
        for t in page.get("TableList", []):
            if t["Name"].startswith(prefix):
                tables.append(t["Name"])
    return tables

# ---------- Glue job template builder ----------
def build_job_script_template(bucket, temp_report_prefix, landing_db, fdp_db, target_prefix_root, region):
    """
    Returns a template string where {table_name} will be replaced per table.
    The Glue job is metadata-only: compare schemas, write temp report, update/create FDP table metadata (no data copy).
    """
    template = '''\
#!/usr/bin/env python3
# Glue job script (metadata-only) generated automatically
import sys, json, logging
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
import boto3
import csv
import io
from datetime import datetime

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('glue_meta_sync')

s3 = boto3.client('s3')
glue = boto3.client('glue', region_name='{region}')

# Parameters for this run:
TABLE_NAME = "{table_name}"  # placeholder will be replaced by driver before upload
LANDING_DB = "{landing_db}"
FDP_DB = "{fdp_db}"
BUCKET = "{bucket}"
TEMP_PREFIX = "{temp_report_prefix}".rstrip('/') + '/'
FDP_TARGET_ROOT = "{target_prefix_root}".rstrip('/') + '/'

def get_table(db, name):
    try:
        return glue.get_table(DatabaseName=db, Name=name)['Table']
    except glue.exceptions.EntityNotFoundException:
        return None

def get_prev_schema(db, name):
    try:
        resp = glue.get_table_versions(DatabaseName=db, TableName=name, MaxResults=10)
        versions = resp.get('TableVersions', [])
        if len(versions) > 1:
            return versions[1]['Table']['StorageDescriptor'].get('Columns', [])
        else:
            return []
    except Exception as e:
        logger.exception("get_prev_schema error")
        return []

def normalize(cols):
    return [(c.get('Name','').strip(), c.get('Type','string').lower()) for c in (cols or [])]

def schema_changed(prev, curr):
    return normalize(prev) != normalize(curr)

def write_temp_report(rows):
    # rows: list of dicts -> write CSV to S3
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%SZ")
    key = TEMP_PREFIX + "schema_changes_" + ts + ".csv"
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(['Table Name','Database','Schema Change','New Schema'])
    for r in rows:
        w.writerow([r['table_name'], r['database'], r['schema_change'], json.dumps(r['new_schema'])])
    s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue().encode('utf-8'))
    logger.info("WROTE temp report s3://%s/%s", BUCKET, key)
    return key

def create_or_update_fdp(table_name, columns, fdp_location):
    # If FDP exists -> update schema & set Location to fdp_location (placeholder)
    try:
        glue.get_table(DatabaseName=FDP_DB, Name=table_name)
        # update
        glue.update_table(
            DatabaseName=FDP_DB,
            TableInput={
                'Name': table_name,
                'StorageDescriptor': {
                    'Columns': columns,
                    'Location': fdp_location,
                    'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                        'Parameters': {'field.delim': ','}
                    }
                },
                'TableType': 'EXTERNAL_TABLE'
            }
        )
        logger.info("Updated FDP table schema/location for %s", table_name)
    except glue.exceptions.EntityNotFoundException:
        # create metadata-only table (no data copy)
        glue.create_table(
            DatabaseName=FDP_DB,
            TableInput={
                'Name': table_name,
                'StorageDescriptor': {
                    'Columns': columns,
                    'Location': fdp_location,
                    'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                        'Parameters': {'field.delim': ','}
                    }
                },
                'TableType': 'EXTERNAL_TABLE'
            }
        )
        logger.info("Created FDP metadata-only table for %s", table_name)


# ----- Main logic -----
logger.info("Starting metadata sync for table %s", TABLE_NAME)
rows_for_report = []

landing_tbl = get_table(LANDING_DB, TABLE_NAME)
if not landing_tbl:
    logger.error("Landing table not found: %s.%s", LANDING_DB, TABLE_NAME)
    job.commit()
    sys.exit(1)

curr_cols = landing_tbl.get('StorageDescriptor', {}).get('Columns', [])
prev_cols = get_prev_schema(LANDING_DB, TABLE_NAME)
changed = schema_changed(prev_cols, curr_cols)
rows_for_report.append({
    'table_name': TABLE_NAME,
    'database': LANDING_DB,
    'schema_change': 'YES' if changed else 'NO',
    'new_schema': curr_cols
})

# fdp placeholder location (manager/ops will change later)
fdp_location = f"s3://{BUCKET}/{FDP_TARGET_ROOT}{TABLE_NAME}/"

# If schema changed -> update FDP schema (if exists) else create FDP metadata
if changed:
    logger.info("Schema changed for %s -> update/create FDP metadata", TABLE_NAME)
    create_or_update_fdp(TABLE_NAME, curr_cols, fdp_location)
else:
    logger.info("No schema change for %s", TABLE_NAME)
    # If FDP missing -> create metadata-only table
    fdp_tbl = get_table(FDP_DB, TABLE_NAME)
    if not fdp_tbl:
        logger.info("FDP table missing -> creating metadata-only for %s", TABLE_NAME)
        create_or_update_fdp(TABLE_NAME, curr_cols, fdp_location)
    else:
        logger.info("FDP exists -> no action for %s", TABLE_NAME)

# Write temp S3 report for auditing
report_key = write_temp_report(rows_for_report)
logger.info("Report written: s3://%s/%s", BUCKET, report_key)

job.commit()
logger.info("Job completed for %s", TABLE_NAME)
'''
    return template.format(
        region=region,
        table_name="{table_name}",
        landing_db=landing_db,
        fdp_db=fdp_db,
        bucket=bucket,
        temp_report_prefix=temp_report_prefix,
        target_prefix_root=target_prefix_root
    )

# ---------- Main ----------
def main(env):
    setup_logging()
    cfg = load_config(env)
    region = cfg['aws']['region']
    bucket = cfg['aws']['bucket_name']
    landing_db = cfg['glue']['landing_db']
    fdp_db = cfg['glue']['fdp_db']
    table_prefix = cfg['glue'].get('table_prefix', '')
    scripts_prefix = cfg['s3_paths'].get('script_prefix', 'scripts/')
    temp_report_prefix = cfg['s3_paths'].get('temp_prefix', 'temp/')
    target_prefix_root = cfg['s3_paths']['target_prefix']
    glue_version = cfg['glue'].get('glue_version', '4.0')
    temp_dir = cfg['glue']['temp_dir']
    role_arn = cfg['aws'].get('role_arn')

    s3, glue, sts = clients(region)
    if not role_arn:
        account = sts.get_caller_identity()['Account']
        role_arn = f"arn:aws:iam::{account}:role/AWSGlueServiceRole-DataEngineering"
        logging.info("Auto-assigned role_arn=%s", role_arn)

    # discover tables
    tables = list_tables_with_prefix(glue, landing_db, table_prefix)
    if not tables:
        logging.info("No tables discovered with prefix %s in %s", table_prefix, landing_db)
        return

    template = build_job_script_template(bucket, temp_report_prefix, landing_db, fdp_db, target_prefix_root, region)

    summary = []
    for t in tables:
        logging.info("Preparing job for table: %s", t)
        # replace placeholder in template for TABLE_NAME
        script_text = template.replace('{table_name}', t)
        script_key = os.path.join(scripts_prefix.rstrip('/'), f"{t}_meta_sync.py")
        script_loc = upload_script(s3, bucket, script_key, script_text)
        job_name = f"meta_sync_{t}"
        ensure_glue_job(glue, job_name, role_arn, script_loc, glue_version, temp_dir)
        run_id = start_job(glue, job_name, temp_dir)
        summary.append({'table': t, 'job': job_name, 'run_id': run_id, 'script': script_loc})

    logging.info("All jobs started. Summary:")
    for s in summary:
        logging.info(json.dumps(s))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="dev")
    args = parser.parse_args()
    main(args.env)
