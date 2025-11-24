#!/usr/bin/env python3
"""
Final production-grade Glue Schema Sync Driver (Option A + Timestamp)

Outputs CSV at: <TEMP_PREFIX>/schema_report_<TABLE>_<UTC_TS>.csv
CSV header: Table,Database,SchemaChanged,Timestamp,Details
Details contains only the schema JSON array.

Placeholders (config) live in the if __name__ == '__main__' block.
"""

from __future__ import annotations
import argparse
import boto3
import botocore
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
import yaml
from typing import List

# --------------------- Helpers & Logging ---------------------
LOG_DIR = "logs"


def utc_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")


def setup_logging(level=logging.INFO) -> None:
    os.makedirs(LOG_DIR, exist_ok=True)
    ts = utc_ts()
    logfile = os.path.join(LOG_DIR, f"driver_{ts}.log")
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(logfile, encoding="utf-8")],
    )
    logging.info("Logging initialized -> %s", logfile)


# --------------------- Config & AWS Clients ---------------------
def load_config(path: str) -> dict:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config not found: {path}")
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def aws_clients(region: str):
    session = boto3.session.Session(region_name=region)
    s3 = session.client("s3")
    glue = session.client("glue")
    sts = session.client("sts")
    return s3, glue, sts


# --------------------- Retry wrapper ---------------------
def retry(fn, retries: int = 3, backoff: int = 3):
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except botocore.exceptions.ClientError as e:
            logging.warning("AWS ClientError attempt %s/%s: %s", attempt, retries, e)
            if attempt == retries:
                logging.exception("Max retries reached")
                raise
            time.sleep(backoff * attempt)
        except Exception as e:
            logging.warning("Transient error attempt %s/%s: %s", attempt, retries, e)
            if attempt == retries:
                logging.exception("Max retries reached")
                raise
            time.sleep(backoff * attempt)


# --------------------- Glue/Workflow Helpers ---------------------
def list_tables_with_prefix(glue_client, database: str, prefix: str) -> List[str]:
    paginator = glue_client.get_paginator("get_tables")
    tables: List[str] = []
    for page in paginator.paginate(DatabaseName=database):
        for t in page.get("TableList", []):
            name = t.get("Name")
            if name and name.startswith(prefix):
                tables.append(name)
    return tables


def upload_script_to_s3(s3_client, bucket: str, key: str, content: str) -> str:
    logging.info("Uploading script to s3://%s/%s", bucket, key)
    retry(lambda: s3_client.put_object(Bucket=bucket, Key=key, Body=content.encode("utf-8")))
    return f"s3://{bucket}/{key}"


def ensure_glue_job(glue_client, job_name: str, role_arn: str, script_location: str, glue_version: str, temp_dir: str) -> None:
    try:
        glue_client.get_job(JobName=job_name)
        logging.info("Glue job exists: %s", job_name)
    except glue_client.exceptions.EntityNotFoundException:
        glue_client.create_job(
            Name=job_name,
            Role=role_arn,
            ExecutionProperty={"MaxConcurrentRuns": 1},
            Command={"Name": "glueetl", "ScriptLocation": script_location, "PythonVersion": "3"},
            DefaultArguments={"--TempDir": temp_dir, "--job-language": "python"},
            GlueVersion=glue_version,
        )
        logging.info("Created Glue job: %s", job_name)


def start_glue_job_and_get_run_id(glue_client, job_name: str, temp_dir: str) -> str:
    resp = glue_client.start_job_run(JobName=job_name, Arguments={"--JOB_NAME": job_name, "--TempDir": temp_dir})
    run_id = resp["JobRunId"]
    logging.info("Started Glue job %s runId=%s", job_name, run_id)
    return run_id


# --------------------- Safe Glue Job Template (full logic) ---------------------
def build_glue_job_script_safe(
    table_name: str,
    region: str,
    bucket: str,
    landing_db: str,
    fdp_db: str,
    temp_prefix: str,
    target_prefix_root: str,
) -> str:
    """
    Build a safe raw-template (no f-strings, no .format) with custom placeholders.
    Template uses @@PLACEHOLDER@@ tokens replaced by the driver.
    """

    template = r'''#!/usr/bin/env python3
import sys, json, logging, csv, io
from datetime import datetime, timezone
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
import boto3

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('meta_sync_job')

# placeholders replaced by driver
TABLE_NAME = '@@TABLE_NAME@@'
LANDING_DB = '@@LANDING_DB@@'
FDP_DB = '@@FDP_DB@@'
BUCKET = '@@BUCKET@@'
REGION = '@@REGION@@'
TEMP_PREFIX = '@@TEMP_PREFIX@@'
FDP_ROOT = '@@FDP_ROOT@@'

s3 = boto3.client('s3')
glue = boto3.client('glue', region_name=REGION)

# global timestamp for this job run (same for all report rows)
GLOBAL_TS = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%SZ')

def get_table(db, name):
    try:
        return glue.get_table(DatabaseName=db, Name=name)['Table']
    except glue.exceptions.EntityNotFoundException:
        return None

def get_prev_schema(db, name):
    try:
        resp = glue.get_table_versions(DatabaseName=db, TableName=name, MaxResults=5)
        versions = resp.get('TableVersions', [])
        if len(versions) > 1:
            return versions[1]['Table']['StorageDescriptor'].get('Columns', [])
        return []
    except Exception:
        return []

def normalize(cols):
    items = []
    for c in (cols or []):
        name = (c.get('Name') or '').strip().lower()
        dtype = (c.get('Type') or '').strip().lower()
        items.append((name, dtype))
    return sorted(items, key=lambda x: (x[0], x[1]))

def schema_changed(a, b):
    return normalize(a) != normalize(b)

def write_report(rows):
    key = TEMP_PREFIX.rstrip('/') + '/schema_report_' + TABLE_NAME + '_' + GLOBAL_TS + '.csv'
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(['Table','Database','SchemaChanged','Timestamp','Details'])
    for r in rows:
        details_json = json.dumps(r['schema'])
        w.writerow([r['table'], r['db'], r['schema_changed'], GLOBAL_TS, details_json])
    s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue().encode('utf-8'))
    logger.info('Wrote report: s3://%s/%s', BUCKET, key)
    return key

logger.info('Processing TABLE=%s', TABLE_NAME)

landing = get_table(LANDING_DB, TABLE_NAME)
if not landing:
    logger.error('Landing table missing -> stop')
    job.commit()
    sys.exit(1)

landing_curr = landing.get('StorageDescriptor', {}).get('Columns', [])
landing_prev = get_prev_schema(LANDING_DB, TABLE_NAME)
landing_evolved = schema_changed(landing_prev, landing_curr)

fdp_tbl = get_table(FDP_DB, TABLE_NAME)
fdp_exists = fdp_tbl is not None
fdp_curr = fdp_tbl.get('StorageDescriptor', {}).get('Columns', []) if fdp_exists else []

fdp_mismatch = False
if fdp_exists:
    fdp_mismatch = schema_changed(landing_curr, fdp_curr)

action = 'NONE'
schema_changed_flag = 'NO'

fdp_loc = 's3://' + BUCKET.rstrip('/') + '/' + FDP_ROOT.rstrip('/') + '/' + TABLE_NAME + '/'

if landing_evolved:
    schema_changed_flag = 'YES'
    try:
        glue.get_table(DatabaseName=FDP_DB, Name=TABLE_NAME)
        glue.update_table(
            DatabaseName=FDP_DB,
            TableInput={
                'Name': TABLE_NAME,
                'StorageDescriptor': {
                    'Columns': landing_curr,
                    'Location': fdp_loc,
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
        action = 'UPDATE_FDP'
        logger.info('Updated FDP table metadata due to landing evolution')
    except glue.exceptions.EntityNotFoundException:
        glue.create_table(
            DatabaseName=FDP_DB,
            TableInput={
                'Name': TABLE_NAME,
                'StorageDescriptor': {
                    'Columns': landing_curr,
                    'Location': fdp_loc,
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
        action = 'CREATE_FDP'
        logger.info('Created FDP metadata table due to landing evolution')

else:
    if fdp_exists:
        if fdp_mismatch:
            glue.update_table(
                DatabaseName=FDP_DB,
                TableInput={
                    'Name': TABLE_NAME,
                    'StorageDescriptor': {
                        'Columns': landing_curr,
                        'Location': fdp_loc,
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
            action = 'SYNC_FDP_SCHEMA'
            schema_changed_flag = 'YES'
            logger.info('FDP existed and mismatched -> updated FDP to match landing schema')
        else:
            action = 'NO_OP'
            schema_changed_flag = 'NO'
            logger.info('FDP exists & matches landing -> no action')
    else:
        glue.create_table(
            DatabaseName=FDP_DB,
            TableInput={
                'Name': TABLE_NAME,
                'StorageDescriptor': {
                    'Columns': landing_curr,
                    'Location': fdp_loc,
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
        action = 'CREATE_FDP'
        schema_changed_flag = 'NO'
        logger.info('FDP missing -> created metadata-only table (no data copy)')

report_rows = [{
    'table': TABLE_NAME,
    'db': LANDING_DB,
    'schema_changed': schema_changed_flag,
    'schema': landing_curr
}]
report_key = write_report(report_rows)
logger.info('Report key: %s', report_key)

job.commit()
logger.info('Job completed for %s | action=%s | schema_changed=%s', TABLE_NAME, action, schema_changed_flag)
'''

    out = (
        template
        .replace('@@TABLE_NAME@@', table_name)
        .replace('@@LANDING_DB@@', landing_db)
        .replace('@@FDP_DB@@', fdp_db)
        .replace('@@BUCKET@@', bucket)
        .replace('@@REGION@@', region)
        .replace('@@TEMP_PREFIX@@', temp_prefix)
        .replace('@@FDP_ROOT@@', target_prefix_root)
    )
    return out


# --------------------- Main (placeholders only here) ---------------------
if __name__ == '__main__':
    setup_logging()

    parser = argparse.ArgumentParser(description='Glue Schema Sync Driver')
    parser.add_argument('--env', default='dev')
    args = parser.parse_args()

    # --------------------- DEFINE ALL PLACEHOLDERS / CONFIG HERE ---------------------
    CONFIG_PATH = f"C:/Users/seera/AWS/src/catalog_sync/config/config_{args.env}.yaml"  # adjust if required

    cfg = load_config(CONFIG_PATH)

    REGION = cfg['aws']['region']
    BUCKET = cfg['aws']['bucket_name']
    ROLE_ARN = cfg['aws'].get('role_arn')  # recommended but optional

    LANDING_DB = cfg['glue']['landing_db']
    FDP_DB = cfg['glue']['fdp_db']
    TABLE_PREFIX = cfg['glue'].get('table_prefix', '')
    GLUE_VERSION = cfg['glue'].get('glue_version', '4.0')
    TEMP_DIR = cfg['glue']['temp_dir']

    SCRIPTS_PREFIX = cfg['s3_paths'].get('script_prefix', 'scripts/')
    TEMP_REPORT_PREFIX = cfg['s3_paths'].get('temp_prefix', 'temp/')
    TARGET_PREFIX_ROOT = cfg['s3_paths']['target_prefix']

    # --------------------- AWS clients ---------------------
    s3, glue, sts = aws_clients(REGION)

    if not ROLE_ARN:
        account = sts.get_caller_identity()["Account"]
        ROLE_ARN = f"arn:aws:iam::{account}:role/AWSGlueServiceRole-DataEngineering"
        logging.info('Auto-assigned ROLE_ARN=%s', ROLE_ARN)

    # --------------------- Discover and process tables ---------------------
    tables = list_tables_with_prefix(glue, LANDING_DB, TABLE_PREFIX)
    logging.info('Discovered tables: %s', tables)

    if not tables:
        logging.info('No tables found for prefix %s in %s', TABLE_PREFIX, LANDING_DB)
        sys.exit(0)

    summary = []
    for tbl in tables:
        logging.info('Preparing table: %s', tbl)

        try:
            script_body = build_glue_job_script_safe(
                table_name=tbl,
                region=REGION,
                bucket=BUCKET,
                landing_db=LANDING_DB,
                fdp_db=FDP_DB,
                temp_prefix=TEMP_REPORT_PREFIX,
                target_prefix_root=TARGET_PREFIX_ROOT,
            )

            script_key = f"{SCRIPTS_PREFIX.rstrip('/')}/{tbl}_meta_sync.py"
            script_s3 = upload_script_to_s3(s3, BUCKET, script_key, script_body)

            job_name = f"meta_sync_{tbl}"
            ensure_glue_job(glue, job_name, ROLE_ARN, script_s3, GLUE_VERSION, TEMP_DIR)
            run_id = start_glue_job_and_get_run_id(glue, job_name, TEMP_DIR)

            summary.append({"table": tbl, "job": job_name, "run_id": run_id, "script": script_s3})

        except Exception as err:
            logging.exception('Failed processing table %s: %s', tbl, err)
            summary.append({"table": tbl, "error": str(err)})

    logging.info('Summary: %s', json.dumps(summary, indent=2))
