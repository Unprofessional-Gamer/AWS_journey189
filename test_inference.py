"""
Test script to validate schema inference on local sample files.
This tests the infer_schema_from_s3_head logic without requiring AWS access.
"""

import os
import sys
import json
import csv
import io
import xml.etree.ElementTree as ET
from typing import List, Dict

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Copy the inference functions from the main script
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

def infer_schema_from_file(file_path: str) -> List[Dict[str, str]]:
    """
    Local version of schema inference (doesn't use S3).
    Tests the same inference logic on local files.
    """
    _, ext = os.path.splitext(file_path)
    ext = ext.lower()
    
    with open(file_path, 'r', encoding='utf-8') as f:
        text = f.read()
    
    if ext == ".json":
        lines = [l for l in text.splitlines() if l.strip()]
        sample_objs = []
        if lines:
            try:
                for ln in lines[:50]:
                    obj = json.loads(ln)
                    if isinstance(obj, dict):
                        sample_objs.append(obj)
            except Exception:
                sample_objs = []
        if not sample_objs:
            try:
                doc = json.loads(text)
                if isinstance(doc, list) and doc:
                    sample_objs = [o for o in doc[:50] if isinstance(o, dict)]
                elif isinstance(doc, dict):
                    sample_objs = [doc]
            except Exception:
                pass

        if not sample_objs:
            raise ValueError("JSON appears empty or unreadable.")

        keys = {}
        for o in sample_objs:
            for k, v in o.items():
                t = _infer_type(str(v) if v is not None else "")
                if k in keys:
                    keys[k] = merge_types(keys[k], t)
                else:
                    keys[k] = t
        columns = [{"Name": k, "Type": keys[k]} for k in keys]
        return columns

    elif ext == ".csv":
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
        return columns
    
    elif ext == ".tsv":
        delimiter = "\t"
        reader = csv.reader(io.StringIO(text), delimiter=delimiter)
        rows = list(reader)
        if not rows:
            raise ValueError("TSV appears empty or unreadable.")
        headers = rows[0]
        col_types = ["string"] * len(headers)
        for r in rows[1: min(len(rows), 50)]:
            for i, val in enumerate(r + [""] * (len(headers) - len(r))):
                t = _infer_type(val)
                col_types[i] = merge_types(col_types[i], t)
        columns = [{"Name": h.strip() or f"col_{i+1}", "Type": t} for i, (h, t) in enumerate(zip(headers, col_types))]
        return columns
    
    elif ext == ".xml":
        try:
            root = ET.fromstring(text)
            candidate = None
            for child in root:
                candidate = child
                break
            if candidate is None:
                candidate = root
            keys = {}
            for c in candidate:
                name = c.tag
                t = _infer_type((c.text or "").strip())
                keys[name] = merge_types(keys.get(name, t), t)
            columns = [{"Name": k, "Type": keys[k]} for k in keys]
            return columns
        except Exception:
            return [{"Name": "line", "Type": "string"}]
    
    elif ext == ".txt":
        return [{"Name": "line", "Type": "string"}]
    
    else:
        raise ValueError(f"Unsupported file type: {ext}")

# Test cases
if __name__ == "__main__":
    test_files = [
        ("sample_data.json", "JSON array of customer objects"),
        ("sample_data.csv", "CSV with header and data rows"),
        ("sample_data.tsv", "TSV with header and data rows"),
        ("sample_data.xml", "XML with nested customer records"),
        ("sample_data.txt", "Plain text file (one column)"),
    ]
    
    print("=" * 80)
    print("Schema Inference Test")
    print("=" * 80)
    
    for file_name, description in test_files:
        file_path = os.path.join(os.path.dirname(__file__), file_name)
        
        if not os.path.exists(file_path):
            print(f"\n⚠️  File not found: {file_path}")
            continue
        
        print(f"\n✅ Testing: {file_name}")
        print(f"   Description: {description}")
        print(f"   Path: {file_path}")
        
        try:
            columns = infer_schema_from_file(file_path)
            print(f"   Inferred columns:")
            for col in columns:
                print(f"      - {col['Name']:<20} : {col['Type']}")
            print(f"\n   ✅ Schema inference successful! ({len(columns)} columns)")
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    print("\n" + "=" * 80)
    print("Test Complete")
    print("=" * 80)
