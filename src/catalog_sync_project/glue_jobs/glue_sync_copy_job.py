import boto3

s3 = boto3.client("s3")
glue = boto3.client("glue")

BUCKET = "catalog-sync-testing"
SOURCE = "source_catalog_file/"
TARGET = "Target_catalog_file/"
FDP_DB = "fdp_db"

def main():
    # Find latest CSV file
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=SOURCE)
    files = [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".csv")]
    
    if not files:
        print("No CSV files found. Exiting.")
        return

    latest = sorted(files, key=lambda x: x.split('/')[-1])[-1]
    target_path = TARGET + latest.split("/")[-1]

    # Copy to FDP zone
    s3.copy_object(
        Bucket=BUCKET,
        CopySource={'Bucket': BUCKET, 'Key': latest},
        Key=target_path
    )
    print(f"✅ File copied to FDP: {target_path}")

    # Create FDP Glue Table
    table_name = "fdp_" + latest.split("/")[-1].replace(".csv", "")
    glue.create_table(
        DatabaseName=FDP_DB,
        TableInput={
            "Name": table_name,
            "StorageDescriptor": {
                "Columns": [{"Name": "data", "Type": "string"}],
                "Location": f"s3://{BUCKET}/{TARGET}",
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.serde2.OpenCSVSerde"}
            }
        }
    )
    print(f"✅ Glue table created in FDP: {FDP_DB}.{table_name}")

if __name__ == "__main__":
    main()
