import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame

# ✅ Required arguments from Glue Job parameters
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "SOURCE_DB",
        "SOURCE_TABLE",
        "TARGET_DB",
        "TARGET_TABLE"
    ],
)

# ✅ Initialize Glue + Spark session
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = glueContext.get_logger()
logger.info("🚀 Starting Glue ETL job to write Iceberg table...")

# ✅ Read job arguments
SOURCE_DB = args["SOURCE_DB"]
SOURCE_TABLE = args["SOURCE_TABLE"]
TARGET_DB = args["TARGET_DB"]
TARGET_TABLE = args["TARGET_TABLE"]
TARGET_S3_PATH = "s3://aws-catalog-prep/Target_catalog_file/"  # Iceberg warehouse location

logger.info(f"📂 Source Table: {SOURCE_DB}.{SOURCE_TABLE}")
logger.info(f"📁 Target Iceberg Table: {TARGET_DB}.{TARGET_TABLE}")
logger.info(f"📦 Target Location (S3): {TARGET_S3_PATH}")

# ✅ Step 1: Read source data from Glue Catalog
source_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DB,
    table_name=SOURCE_TABLE,
    transformation_ctx="source_dyf"
)

record_count = source_dyf.count()
logger.info(f"✅ Source record count: {record_count}")

if record_count == 0:
    logger.warn("⚠ Source DataFrame is empty. Terminating job.")
    job.commit()
    sys.exit(0)

# ✅ Step 2: Convert to DataFrame and normalize column names
df = source_dyf.toDF()
df = df.toDF(*[c.strip().lower().replace(" ", "_") for c in df.columns])

target_dyf = DynamicFrame.fromDF(df, glueContext, "target_dyf")

# ✅ Step 3: Write data to Iceberg table (create or replace)
iceberg_table_name = f"{TARGET_DB}.{TARGET_TABLE}"

logger.info(f"🧊 Writing to Iceberg Table: {iceberg_table_name}")

(
    df.writeTo(iceberg_table_name)
    .option("path", TARGET_S3_PATH)        # → Iceberg table storage location
    .tableProperty("format-version", "2")  # Iceberg V2
    .tableProperty("write.format.default", "parquet")
    .mode("overwrite")                     # Options: overwrite / append
    .createOrReplace()
)

logger.info("🎉 Data successfully written to Iceberg table!")

job.commit()
logger.info("✅ Glue ETL Job completed successfully!")
