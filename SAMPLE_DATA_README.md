# Sample Test Data Files

This directory contains sample data files in multiple formats to test the schema inference and multi-format support of the enhanced Glue script.

## Files

### sample_data.json
- **Format:** JSON (array of objects)
- **Description:** Customer records in a JSON array format (NDJSON and JSON arrays both supported)
- **Records:** 5 customer objects
- **Inferred Schema:** 8 columns (customer_id, name, email, age, salary, department, is_active, joining_date)
- **Type Detection:** Automatically infers int (age), double (salary), and string types

### sample_data.csv
- **Format:** CSV (comma-separated values)
- **Description:** Customer records in standard CSV format with header row
- **Records:** 5 rows + 1 header
- **Inferred Schema:** 8 columns
- **Type Detection:** All types inferred as string (typical for CSV header inference)

### sample_data.tsv
- **Format:** TSV (tab-separated values)
- **Description:** Customer records in TSV format with header row
- **Records:** 5 rows + 1 header
- **Inferred Schema:** 8 columns
- **Type Detection:** All types inferred as string

### sample_data.xml
- **Format:** XML
- **Description:** Customer records in XML format with nested elements
- **Records:** 3 customer elements with child tags
- **Inferred Schema:** 8 columns
- **Type Detection:** Automatically infers int (age) and double (salary) from text content

### sample_data.txt
- **Format:** Plain text
- **Description:** Plain text file (no structured format)
- **Records:** 5 lines of text
- **Inferred Schema:** Single column named "line" with type "string"

## Testing

Run the schema inference test:

```bash
python test_inference.py
```

This will validate that the schema inference logic correctly processes all supported file formats and outputs the inferred columns and types.

## Schema Inference Rules

1. **Type Detection:** Values are checked in order: integer → double → string
2. **Type Merging:** When sampling multiple rows:
   - If types match → keep that type
   - If "string" is involved → resolve to "string"
   - If types are {int, double} → resolve to "double"
3. **CSV/TSV Headers:** First row is treated as column names
4. **JSON:** Keys from objects become column names; values are type-checked
5. **XML:** Child element tags become column names; text content is type-checked
6. **TXT:** Treated as single-column "line" type

## Usage in Glue Jobs

When the enhanced script processes these files:
- **CSV/TSV:** Read with appropriate delimiter, written as CSV to FDP
- **JSON:** Read as JSON, written as CSV to FDP
- **XML:** Read using spark-xml format (requires spark-xml jar), written as CSV
- **TXT:** Read as text, written as text to FDP

Note: XML reading in Glue requires the `com.databricks:spark-xml` package in the Glue job classpath.
