import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame

# ✅ Required job parameters
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "SOURCE_DB",
        "SOURCE_TABLE",
        "TARGET_DB",
        "TARGET_TABLE",
        "WRITE_MODE",
        "TARGET_FORMAT"
    ],
)

# ✅ Initialize Glue + Spark
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = glueContext.get_logger()
logger.info("🔹 Starting Glue ETL job...")

# ✅ Read arguments
SOURCE_DB = args["SOURCE_DB"]
SOURCE_TABLE = args["SOURCE_TABLE"]
TARGET_DB = args["TARGET_DB"]
TARGET_TABLE = args["TARGET_TABLE"]
WRITE_MODE = args["WRITE_MODE"]  # append / overwrite
TARGET_FORMAT = args["TARGET_FORMAT"]  # csv

logger.info(f"✅ Using Source: {SOURCE_DB}.{SOURCE_TABLE}")
logger.info(f"✅ Using Target: {TARGET_DB}.{TARGET_TABLE}")
logger.info(f"✅ Output Format: {TARGET_FORMAT}, Mode: {WRITE_MODE}")

# ✅ Read data from source Data Catalog table
source_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DB,
    table_name=SOURCE_TABLE,
    transformation_ctx="source_dyf"
)

record_count = source_dyf.count()
logger.info(f"📥 Source record count: {record_count}")

if record_count == 0:
    logger.warn("⚠ Source table is empty. Exiting job gracefully.")
    job.commit()
    sys.exit(0)

# ✅ Convert to DataFrame and clean column names
df = source_dyf.toDF()
df = df.toDF(*[c.strip().lower().replace(" ", "_") for c in df.columns])

target_dyf = DynamicFrame.fromDF(df, glueContext, "target_dyf")

# Convert DynamicFrame to DataFrame for Iceberg write
df = target_dyf.toDF()

# ✅ Iceberg table full name
iceberg_table_name = f"{TARGET_DB}.{TARGET_TABLE}"

logger.info(f"🧊 Writing to Iceberg Table: {iceberg_table_name}")

(
    df.writeTo(iceberg_table_name)
    .option("path", "s3://aws-catalog-prep/Target_catalog_file/")  # Iceberg warehouse
    .tableProperty("format-version", "2")
    .tableProperty("write.format.default", "parquet")
    .createOrReplace()  # ✅ Correct! Replaces existing table
)


logger.info(f"✅ Successfully written data to Iceberg table: {iceberg_table_name}")


logger.info("✅ Data successfully written to Target Table in CSV format")

job.commit()
logger.info("🎉 Glue ETL job completed successfully!")
