#!/usr/bin/env python3
"""
catalog_sync_single_driver.py

Single Glue driver to:
 - List tables from landing Glue DB by prefix
 - For each table:
    - If FDP table exists -> compare schema -> if mismatch update FDP metadata
    - If FDP table does NOT exist -> create FDP table with landing columns + audit columns and set Location
    - Create an Iceberg table in FDP catalog with the same table (same location)
 - Log every step and write an audit entry per step to an existing audit Glue table
 - Config provided via --config (supports s3:// and local file paths)

How to run (Glue job):
 - Upload this script to S3
 - Create Glue job manually and point to script
 - In Glue Job arguments provide: --config s3://path/to/config.yaml

Author: (generated)
"""
from __future__ import annotations
import argparse
import boto3
import botocore
import csv
import io
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Tuple

try:
    import yaml
except Exception:
    # Glue environment usually has pyyaml; if not, fall back to json-only config
    yaml = None

LOG_DIR = "/tmp/logs"
os.makedirs(LOG_DIR, exist_ok=True)


def utcnow_ts() -> str:
    return datetime.now(timezone.utc).isoformat()


def setup_logging(level=logging.INFO):
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")
    logfile = os.path.join(LOG_DIR, f"catalog_sync_{ts}.log")
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(logfile, encoding="utf-8")],
    )
    logging.info("Logging initialized -> %s", logfile)


# ------------------------- util: load config (s3 or local) -------------------------
def load_config(path: str, boto3_session=None) -> Dict:
    """
    Load YAML or JSON config. Accepts:
     - s3://bucket/key
     - /local/path/config.yaml
    """
    data = None
    if path.startswith("s3://"):
        if not boto3_session:
            boto3_session = boto3.session.Session()
        s3 = boto3_session.client("s3")
        # parse
        parts = path[5:].split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""
        logging.info("Loading config from S3: s3://%s/%s", bucket, key)
        obj = s3.get_object(Bucket=bucket, Key=key)
        content = obj["Body"].read().decode("utf-8")
    else:
        logging.info("Loading config from local path: %s", path)
        with open(path, "r", encoding="utf-8") as fh:
            content = fh.read()

    # Parse YAML if available, else JSON
    if yaml:
        data = yaml.safe_load(content)
    else:
        data = json.loads(content)
    return data


# ------------------------- AWS clients -------------------------
def aws_clients(region_name: str = None):
    session = boto3.session.Session(region_name=region_name)
    glue = session.client("glue")
    s3 = session.client("s3")
    sts = session.client("sts")
    return session, glue, s3, sts


# ------------------------- retry wrapper -------------------------
def retry(fn, retries: int = 3, backoff: int = 2):
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


# ------------------------- Glue helpers -------------------------
def list_tables_with_prefix(glue_client, database: str, prefix: str) -> List[str]:
    logging.info("Listing tables in database=%s with prefix=%s", database, prefix)
    paginator = glue_client.get_paginator("get_tables")
    names: List[str] = []
    for page in paginator.paginate(DatabaseName=database):
        for t in page.get("TableList", []):
            name = t.get("Name")
            if not name:
                continue
            if prefix == "" or name.startswith(prefix):
                names.append(name)
    logging.info("Found %s tables", len(names))
    return names


def get_table(glue_client, database: str, table_name: str) -> Dict | None:
    try:
        resp = glue_client.get_table(DatabaseName=database, Name=table_name)
        return resp.get("Table")
    except glue_client.exceptions.EntityNotFoundException:
        return None


def normalize_columns(cols: List[Dict]) -> List[Tuple[str, str]]:
    """
    Convert columns list from Glue StorageDescriptor to normalized list of tuples: (name_lower, type_lower).
    """
    out = []
    for c in (cols or []):
        n = (c.get("Name") or "").strip().lower()
        t = (c.get("Type") or "").strip().lower()
        out.append((n, t))
    return sorted(out)


def schemas_equal(cols_a: List[Dict], cols_b: List[Dict]) -> bool:
    return normalize_columns(cols_a) == normalize_columns(cols_b)


def build_fdp_location(base_s3: str, table_name: str) -> str:
    # ensure trailing slash behavior
    base = base_s3.rstrip("/")
    return f"{base}/{table_name}/"


# ------------------------- Audit writer -------------------------
def write_audit_row_to_audit_table_s3(
    s3_client,
    glue_client,
    audit_db: str,
    audit_table: str,
    row: Dict,
    compress: bool = False,
):
    """
    Generic audit writer:
      - Finds audit table location from Glue Catalog
      - Writes a small CSV (header + single row) into that location with unique filename
    This keeps audit as a simple append-only S3 row (works for Athena / Glue reads).
    """
    try:
        tbl = glue_client.get_table(DatabaseName=audit_db, Name=audit_table)["Table"]
        loc = tbl.get("StorageDescriptor", {}).get("Location")
        if not loc:
            raise RuntimeError("Audit table has no S3 location set in Glue Catalog.")
        # parse s3://bucket/keyprefix
        assert loc.startswith("s3://")
        parts = loc[5:].split("/", 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""
        filename = f"audit_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}.csv"
        key = (prefix.rstrip("/") + "/" + filename).lstrip("/")
        # create CSV string
        buf = io.StringIO()
        writer = csv.writer(buf)
        # header in stable order
        headers = ["process_id", "table", "step", "status", "message", "timestamp", "meta"]
        writer.writerow(headers)
        writer.writerow([
            row.get("process_id"),
            row.get("table"),
            row.get("step"),
            row.get("status"),
            row.get("message"),
            row.get("timestamp"),
            json.dumps(row.get("meta", {}), default=str),
        ])
        body = buf.getvalue().encode("utf-8")
        logging.info("Writing audit row to s3://%s/%s", bucket, key)
        retry(lambda: s3_client.put_object(Bucket=bucket, Key=key, Body=body))
    except Exception as e:
        logging.exception("Failed to write audit row: %s", e)
        # Do not raise — audit must not stop main pipeline


# ------------------------- FDP metadata create/update -------------------------
def create_fdp_table_from_landing(
    glue_client,
    landing_db: str,
    landing_tbl_name: str,
    fdp_db: str,
    fdp_tbl_name: str,
    fdp_s3_base: str,
    extra_cols: List[Dict],
    iceberg_parameters: Dict,
) -> None:
    """
    Create new table in FDP DB with landing columns + extra_cols and location set to fdp_s3_base/<table>/
    """
    landing_tbl = glue_client.get_table(DatabaseName=landing_db, Name=landing_tbl_name)["Table"]
    landing_cols = landing_tbl.get("StorageDescriptor", {}).get("Columns", [])
    merged_cols = (landing_cols or []) + extra_cols

    location = build_fdp_location(fdp_s3_base, fdp_tbl_name)
    table_input = {
        "Name": fdp_tbl_name,
        "StorageDescriptor": {
            "Columns": merged_cols,
            "Location": location,
            # Using simple text serde for CSV/compatibility. Adjust if you want parquet.
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                "Parameters": {"field.delim": ","},
            },
        },
        "TableType": "EXTERNAL_TABLE",
    }

    # attach iceberg params if provided (keeps table present for Iceberg)
    if iceberg_parameters:
        table_input.setdefault("Parameters", {}).update(iceberg_parameters)

    logging.info("Creating FDP table %s.%s at %s", fdp_db, fdp_tbl_name, location)
    retry(lambda: glue_client.create_table(DatabaseName=fdp_db, TableInput=table_input))


def update_fdp_table_schema(
    glue_client,
    fdp_db: str,
    fdp_tbl_name: str,
    new_columns: List[Dict],
    location: str = None,
    iceberg_parameters: Dict = None,
) -> None:
    """
    Update an existing FDP table's StorageDescriptor columns (and optional location/params)
    """
    tbl = glue_client.get_table(DatabaseName=fdp_db, Name=fdp_tbl_name)["Table"]
    table_input = {
        "Name": fdp_tbl_name,
        "StorageDescriptor": tbl.get("StorageDescriptor", {}).copy(),
        "TableType": tbl.get("TableType", "EXTERNAL_TABLE"),
    }
    table_input["StorageDescriptor"]["Columns"] = new_columns
    if location:
        table_input["StorageDescriptor"]["Location"] = location
    if iceberg_parameters:
        table_input.setdefault("Parameters", {}).update(iceberg_parameters)
    logging.info("Updating FDP table %s.%s schema", fdp_db, fdp_tbl_name)
    retry(lambda: glue_client.update_table(DatabaseName=fdp_db, TableInput=table_input))


# ------------------------- Create Iceberg table helper -------------------------
def create_iceberg_table(
    glue_client,
    fdp_db: str,
    table_name: str,
    s3_location: str,
    iceberg_properties: Dict | None = None,
):
    """
    Create an Iceberg table in Glue Catalog. Implementation is conservative and sets
    table parameters indicating Iceberg table. Actual behavior depends on Athena/Glue integration and must be validated in your environment.

    We will create a table with:
      - TableInput.Parameters['table_type']='ICEBERG'
      - StorageDescriptor.Location = s3_location
      - Additional iceberg properties if provided
    """
    logging.info("Creating Iceberg table: %s.%s at %s", fdp_db, table_name, s3_location)
    # Try fetch existing; if exists, update parameters
    existing = None
    try:
        existing = glue_client.get_table(DatabaseName=fdp_db, Name=table_name)["Table"]
    except glue_client.exceptions.EntityNotFoundException:
        existing = None

    iceberg_params = {"table_type": "ICEBERG"}
    if iceberg_properties:
        iceberg_params.update(iceberg_properties)

    if existing:
        # update parameters and keep columns & storage descriptor
        tbl = existing
        tbl_input = {
            "Name": table_name,
            "StorageDescriptor": tbl.get("StorageDescriptor", {}),
            "TableType": tbl.get("TableType", "EXTERNAL_TABLE"),
        }
        tbl_input.setdefault("Parameters", {}).update(iceberg_params)
        tbl_input["StorageDescriptor"]["Location"] = s3_location
        retry(lambda: glue_client.update_table(DatabaseName=fdp_db, TableInput=tbl_input))
        logging.info("Updated existing table parameters for Iceberg")
    else:
        # we create a minimal table; columns should already exist in FDP table
        tbl_input = {
            "Name": table_name,
            "StorageDescriptor": {"Columns": [], "Location": s3_location},
            "Parameters": iceberg_params,
            "TableType": "EXTERNAL_TABLE",
        }
        retry(lambda: glue_client.create_table(DatabaseName=fdp_db, TableInput=tbl_input))
        logging.info("Created Iceberg catalog entry")


# ------------------------- Main orchestration -------------------------
def process_table(
    session,
    glue_client,
    s3_client,
    landing_db: str,
    fdp_db: str,
    table_name: str,
    fdp_s3_base: str,
    extra_columns: List[Dict],
    audit_db: str,
    audit_table: str,
    iceberg_properties: Dict | None,
    process_id: str,
):
    """
    Process a single table end-to-end with audit entries and per-step statuses.
    Steps:
      1. Check FDP table presence
      2. If exists: compare schemas; update if mismatch
      3. If not exists: create FDP table (landing_columns + extra_columns)
      4. Create Iceberg table entry for the table
    """

    ts = utcnow_ts()
    step_meta = {"process_id": process_id, "table": table_name}

    # --- Step: START processing table ---
    logging.info("=== START processing table %s ===", table_name)
    write_audit_row_to_audit_table_s3(
        s3_client,
        glue_client,
        audit_db,
        audit_table,
        {"process_id": process_id, "table": table_name, "step": "PROCESS_START", "status": "STARTED", "message": "", "timestamp": ts, "meta": {}},
    )

    # get landing table
    landing_tbl = get_table(glue_client, landing_db, table_name)
    if not landing_tbl:
        msg = f"Landing table {landing_db}.{table_name} not found"
        logging.error(msg)
        write_audit_row_to_audit_table_s3(
            s3_client, glue_client, audit_db, audit_table, {"process_id": process_id, "table": table_name, "step": "PROCESS", "status": "FAILED", "message": msg, "timestamp": utcnow_ts(), "meta": {}}
        )
        return

    landing_cols = landing_tbl.get("StorageDescriptor", {}).get("Columns", [])

    # check fdp table
    fdp_tbl = get_table(glue_client, fdp_db, table_name)
    fdp_exists = fdp_tbl is not None

    # STEP: FDP CHECK
    write_audit_row_to_audit_table_s3(
        s3_client, glue_client, audit_db, audit_table, {"process_id": process_id, "table": table_name, "step": "FDP_CHECK", "status": "STARTED", "message": "", "timestamp": utcnow_ts(), "meta": {"fdp_exists": fdp_exists}},
    )

    try:
        if fdp_exists:
            fdp_cols = fdp_tbl.get("StorageDescriptor", {}).get("Columns", [])
            if schemas_equal(landing_cols, fdp_cols):
                logging.info("Schemas match for %s -> NO_OP", table_name)
                write_audit_row_to_audit_table_s3(
                    s3_client, glue_client, audit_db, audit_table, {"process_id": process_id, "table": table_name, "step": "SCHEMA_COMPARE", "status": "COMPLETED", "message": "No schema change", "timestamp": utcnow_ts(), "meta": {}},
                )
            else:
                logging.info("Schema mismatch detected for %s -> updating FDP metadata", table_name)
                # Update FDP to match landing columns
                fdp_location = fdp_tbl.get("StorageDescriptor", {}).get("Location")
                update_fdp_table_schema(glue_client, fdp_db, table_name, landing_cols, location=fdp_location, iceberg_parameters=iceberg_properties)
                write_audit_row_to_audit_table_s3(
                    s3_client, glue_client, audit_db, audit_table, {"process_id": process_id, "table": table_name, "step": "SCHEMA_UPDATE", "status": "COMPLETED", "message": "Updated FDP schema from landing", "timestamp": utcnow_ts(), "meta": {}},
                )
        else:
            logging.info("FDP table %s does not exist. Creating from landing.", table_name)
            # Create FDP table (landing cols + extra)
            create_fdp_table_from_landing(
                glue_client,
                landing_db,
                table_name,
                fdp_db,
                table_name,
                fdp_s3_base,
                extra_columns,
                iceberg_properties,
            )
            write_audit_row_to_audit_table_s3(
                s3_client, glue_client, audit_db, audit_table, {"process_id": process_id, "table": table_name, "step": "FDP_CREATE", "status": "COMPLETED", "message": "Created FDP table", "timestamp": utcnow_ts(), "meta": {"fdp_location": build_fdp_location(fdp_s3_base, table_name)}},
            )
    except Exception as e:
        logging.exception("Error during FDP sync for %s: %s", table_name, e)
        write_audit_row_to_audit_table_s3(
            s3_client, glue_client, audit_db, audit_table, {"process_id": process_id, "table": table_name, "step": "FDP_SYNC", "status": "FAILED", "message": str(e), "timestamp": utcnow_ts(), "meta": {}},
        )
        return

    # STEP: Create/Update Iceberg table
    write_audit_row_to_audit_table_s3(
        s3_client, glue_client, audit_db, audit_table, {"process_id": process_id, "table": table_name, "step": "ICEBERG_CREATE", "status": "STARTED", "message": "", "timestamp": utcnow_ts(), "meta": {}},
    )
    try:
        iceberg_location = build_fdp_location(fdp_s3_base, table_name)
        create_iceberg_table(glue_client, fdp_db, table_name, iceberg_location, iceberg_properties)
        write_audit_row_to_audit_table_s3(
            s3_client, glue_client, audit_db, audit_table, {"process_id": process_id, "table": table_name, "step": "ICEBERG_CREATE", "status": "COMPLETED", "message": "Iceberg table created/updated", "timestamp": utcnow_ts(), "meta": {}},
        )
    except Exception as e:
        logging.exception("Failed creating Iceberg table for %s: %s", table_name, e)
        write_audit_row_to_audit_table_s3(
            s3_client, glue_client, audit_db, audit_table, {"process_id": process_id, "table": table_name, "step": "ICEBERG_CREATE", "status": "FAILED", "message": str(e), "timestamp": utcnow_ts(), "meta": {}},
        )
        return

    # FINAL: success
    write_audit_row_to_audit_table_s3(
        s3_client, glue_client, audit_db, audit_table, {"process_id": process_id, "table": table_name, "step": "PROCESS_END", "status": "COMPLETED", "message": "", "timestamp": utcnow_ts(), "meta": {}},
    )
    logging.info("=== COMPLETED processing table %s ===", table_name)


def main():
    parser = argparse.ArgumentParser(description="Catalog Sync Single Driver")
    parser.add_argument("--config", required=True, help="S3 or local path to YAML/JSON config file")
    parser.add_argument("--env", default="dev", help="Environment label (for logging)")
    args, _ = parser.parse_known_args()

    setup_logging()

    session, glue_client, s3_client, sts = aws_clients()
    cfg = load_config(args.config, boto3_session=session)

    # mandatory config keys (basic validation)
    landing_db = cfg["glue"]["landing_db"]
    fdp_db = cfg["glue"]["fdp_db"]
    table_prefix = cfg["glue"].get("table_prefix", "")
    fdp_s3_base = cfg["s3_paths"]["fdp_base_path"]
    audit_db = cfg["audit"]["db"]
    audit_table = cfg["audit"]["table"]
    iceberg_properties = cfg.get("iceberg", {}).get("properties", {})

    # extra audit columns to add on table creation
    ts_col = cfg.get("extra_columns", {}).get("timestamp_col", "ingestion_timestamp")
    source_col = cfg.get("extra_columns", {}).get("source_filename_col", "source_filename")
    job_col = cfg.get("extra_columns", {}).get("glue_job_name_col", "glue_job_name")
    status_col = cfg.get("extra_columns", {}).get("status_col", "record_status")

    extra_columns = [
        {"Name": ts_col, "Type": "string"},
        {"Name": source_col, "Type": "string"},
        {"Name": job_col, "Type": "string"},
        {"Name": status_col, "Type": "string"},
    ]

    # discover tables
    tables = list_tables_with_prefix(glue_client, landing_db, table_prefix)
    if not tables:
        logging.info("No tables found to process. Exiting.")
        return

    # process id for this run (used in audit rows)
    process_id = f"proc_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    logging.info("Starting processing run %s for %s tables", process_id, len(tables))

    # iterate tables serially (can be parallelized by running multiple Glue jobs if needed)
    for tbl in tables:
        try:
            process_table(
                session,
                glue_client,
                s3_client,
                landing_db,
                fdp_db,
                tbl,
                fdp_s3_base,
                extra_columns,
                audit_db,
                audit_table,
                iceberg_properties,
                process_id,
            )
        except Exception as e:
            logging.exception("Unhandled error while processing %s: %s", tbl, e)
            write_audit_row_to_audit_table_s3(
                s3_client, glue_client, audit_db, audit_table, {"process_id": process_id, "table": tbl, "step": "PROCESS", "status": "FAILED", "message": str(e), "timestamp": utcnow_ts(), "meta": {}},
            )

    logging.info("Run %s completed", process_id)


if __name__ == "__main__":
    main()
