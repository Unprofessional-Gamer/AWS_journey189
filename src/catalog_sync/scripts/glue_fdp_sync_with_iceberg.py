#!/usr/bin/env python3
"""
FDP Schema Sync + Iceberg Creation + Per-step Batch Logging (append-only)

- Reads config via --config (S3 or local)
- Discovers tables in landing_db with table_prefix
- For each table:
    - CREATE/UPDATE FDP table metadata (adds audit columns only on creation)
    - CREATE ICEBERG table pointing to same FDP s3 location
    - Log each step to audit S3 path as a parquet file (one-row append)
"""

from __future__ import annotations
import sys
import uuid
import json
import boto3
import yaml
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import Row


# --------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)


def load_config(path: str, s3_client):
    if path.startswith("s3://"):
        bucket, key = path.replace("s3://", "").split("/", 1)
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        return yaml.safe_load(obj["Body"].read().decode("utf-8"))
    else:
        with open(path, "r", encoding="utf-8") as fh:
            return yaml.safe_load(fh)


def normalize_columns(cols: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    out = []
    for c in cols or []:
        name = (c.get("Name") or "").strip().lower()
        dtype = (c.get("Type") or "string").strip().lower()
        if "(" in dtype:
            dtype = dtype.split("(", 1)[0]
        out.append({"name": name, "type": dtype})
    return out


def columns_equal(a, b):
    if len(a) != len(b):
        return False
    key = lambda x: (x["name"], x["type"])
    return sorted(a, key=key) == sorted(b, key=key)


def to_glue_cols(cols):
    return [{"Name": c["name"], "Type": c["type"]} for c in cols]


# --------------------------------------------------------------------
# FDP create/update
# --------------------------------------------------------------------
def create_or_update_fdp_table(glue_client, fdp_db, table, columns, s3_location):
    table_input = {
        "Name": table,
        "TableType": "EXTERNAL_TABLE",
        "StorageDescriptor": {
            "Columns": to_glue_cols(columns),
            "Location": s3_location,
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "Parameters": {}
            }
        },
        "Parameters": {"EXTERNAL": "TRUE"}
    }

    try:
        glue_client.get_table(DatabaseName=fdp_db, Name=table)
        glue_client.update_table(DatabaseName=fdp_db, TableInput=table_input)
        return "UPDATED"
    except glue_client.exceptions.EntityNotFoundException:
        glue_client.create_table(DatabaseName=fdp_db, TableInput=table_input)
        return "CREATED"


# --------------------------------------------------------------------
# Audit logging
# --------------------------------------------------------------------
def log_event_append_parquet(spark, audit_s3_path, record):
    try:
        df = spark.createDataFrame([Row(**record)])
        df.write.mode("append").parquet(audit_s3_path)
        logging.info("Audit logged: %s", json.dumps(record))
    except Exception as e:
        logging.exception("Failed writing audit record: %s", e)


def glue_table_exists(glue_client, db, table):
    try:
        glue_client.get_table(DatabaseName=db, Name=table)
        return True
    except glue_client.exceptions.EntityNotFoundException:
        return False
    except:
        return False


# --------------------------------------------------------------------
# Iceberg creation
# --------------------------------------------------------------------
def create_iceberg_table(spark, iceberg_db, table, location):
    sql = f"""
        CREATE TABLE IF NOT EXISTS glue_catalog.{iceberg_db}.{table}
        USING iceberg
        LOCATION '{location}'
    """
    logging.info("Executing Iceberg DDL: %s", sql)
    spark.sql(sql)
    logging.info("Iceberg created for: %s.%s", iceberg_db, table)


# --------------------------------------------------------------------
# Main
# --------------------------------------------------------------------
def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "config"])
    job_name = args["JOB_NAME"]
    config_path = args["config"]

    # Spark context
    sc = SparkContext()
    glue_ctx = GlueContext(sc)
    spark = glue_ctx.spark_session

    setup_logging()

    s3_client = boto3.client("s3")
    glue_client = boto3.client("glue")

    cfg = load_config(config_path, s3_client)

    # Config
    landing_db = cfg["glue"]["landing_db"]
    fdp_db = cfg["glue"]["fdp_db"]
    prefix = cfg["glue"]["table_prefix"]

    fdp_s3_base = cfg["s3"]["fdp_s3_base"]
    iceberg_db = cfg["iceberg"]["iceberg_db"]

    # Audit
    audit_cfg = cfg.get("audit", {})
    audit_db = audit_cfg.get("audit_db", "audit_db")
    audit_table = audit_cfg.get("audit_table", "fdp_batch_log")
    audit_s3_path = audit_cfg.get("audit_s3_path", "s3://aws-catalog-prep/audit/fdp_batch_log/")

    # Extra landing columns
    extras = cfg["extras"]
    ts_col = extras["timestamp_col"].lower()
    src_col = extras["source_filename_col"].lower()
    glue_col = extras["glue_job_name_col"].lower()
    status_col = extras["status_col"].lower()

    batch_id = str(uuid.uuid4())

    logging.info("JOB START: %s | batch_id=%s", job_name, batch_id)

    # Discover tables in landing
    tables = []
    paginator = glue_client.get_paginator("get_tables")
    for page in paginator.paginate(DatabaseName=landing_db):
        for t in page.get("TableList", []):
            name = t["Name"]
            if name.startswith(prefix):
                tables.append(name)

    logging.info("Discovered %d tables", len(tables))

    if not tables:
        rec = {
            "batch_id": batch_id,
            "table_name": None,
            "step": "NO_TABLES_FOUND",
            "status": "SUCCESS",
            "timestamp": utc_now_iso(),
            "message": f"No tables found with prefix {prefix}",
            "job_name": job_name,
            "config_path": config_path
        }
        log_event_append_parquet(spark, audit_s3_path, rec)
        return

    # Process Each Table
    for tbl in tables:
        try:
            log_event_append_parquet(spark, audit_s3_path, {
                "batch_id": batch_id,
                "table_name": tbl,
                "step": "START_TABLE",
                "status": "RUNNING",
                "timestamp": utc_now_iso(),
                "message": f"Processing table {tbl}",
                "job_name": job_name,
                "config_path": config_path
            })

            landing_cols_raw = glue_client.get_table(DatabaseName=landing_db, Name=tbl)["Table"]["StorageDescriptor"]["Columns"]
            landing_cols = normalize_columns(landing_cols_raw)

            # FDP Exists?
            try:
                fdp_tbl_obj = glue_client.get_table(DatabaseName=fdp_db, Name=tbl)["Table"]
                fdp_exists = True
                fdp_cols = normalize_columns(fdp_tbl_obj["StorageDescriptor"]["Columns"])
                fdp_location = fdp_tbl_obj["StorageDescriptor"]["Location"]
            except glue_client.exceptions.EntityNotFoundException:
                fdp_exists = False
                fdp_cols = []
                fdp_location = f"{fdp_s3_base.rstrip('/')}/{tbl}/"

            # 1️⃣ FDP create/update logic
            if fdp_exists:
                if columns_equal(landing_cols, fdp_cols):
                    step = "FDP_NOOP"
                    msg = "Schema matches; no action."
                    logging.info(msg)
                else:
                    step = "FDP_UPDATE"
                    result = create_or_update_fdp_table(glue_client, fdp_db, tbl, landing_cols, fdp_location)
                    msg = f"FDP updated: {result}"
                    logging.info(msg)
            else:
                step = "FDP_CREATE"
                cols_with_extras = list(landing_cols) + [
                    {"name": ts_col, "type": "string"},
                    {"name": src_col, "type": "string"},
                    {"name": glue_col, "type": "string"},
                    {"name": status_col, "type": "string"}
                ]
                result = create_or_update_fdp_table(glue_client, fdp_db, tbl, cols_with_extras, fdp_location)
                msg = f"FDP created: {result}"
                logging.info(msg)

            # Log FDP result
            log_event_append_parquet(spark, audit_s3_path, {
                "batch_id": batch_id,
                "table_name": tbl,
                "step": step,
                "status": "SUCCESS",
                "timestamp": utc_now_iso(),
                "message": msg,
                "job_name": job_name,
                "config_path": config_path
            })

            # 2️⃣ Iceberg creation
            try:
                create_iceberg_table(spark, iceberg_db, tbl, fdp_location)
                log_event_append_parquet(spark, audit_s3_path, {
                    "batch_id": batch_id,
                    "table_name": tbl,
                    "step": "ICEBERG_CREATE",
                    "status": "SUCCESS",
                    "timestamp": utc_now_iso(),
                    "message": f"Iceberg table available at {fdp_location}",
                    "job_name": job_name,
                    "config_path": config_path
                })
            except Exception as e:
                logging.exception("Iceberg creation failed")
                log_event_append_parquet(spark, audit_s3_path, {
                    "batch_id": batch_id,
                    "table_name": tbl,
                    "step": "ICEBERG_FAILED",
                    "status": "FAILED",
                    "timestamp": utc_now_iso(),
                    "message": str(e),
                    "job_name": job_name,
                    "config_path": config_path
                })

        except Exception as ex:
            logging.exception("Failure for table %s", tbl)
            log_event_append_parquet(spark, audit_s3_path, {
                "batch_id": batch_id,
                "table_name": tbl,
                "step": "ERROR",
                "status": "FAILED",
                "timestamp": utc_now_iso(),
                "message": str(ex),
                "job_name": job_name,
                "config_path": config_path
            })

    # Job completion
    log_event_append_parquet(spark, audit_s3_path, {
        "batch_id": batch_id,
        "table_name": None,
        "step": "JOB_COMPLETE",
        "status": "SUCCESS",
        "timestamp": utc_now_iso(),
        "message": "FDP Sync job completed.",
        "job_name": job_name,
        "config_path": config_path
    })

    logging.info("Job finished successfully.")


if __name__ == "__main__":
    main()
