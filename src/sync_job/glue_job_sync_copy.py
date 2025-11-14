import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# ✅ Get job arguments
args = getResolvedOptions(sys.argv,
 [
    "JOB_NAME",
    "SOURCE_DB",
    "SOURCE_TABLE",
    "TARGET_DB",
    "TARGET_TABLE",
    "TARGET_S3_PATH",
    "REGION"
 ])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ✅ Assign variables
src_db = args["SOURCE_DB"]
src_tbl = args["SOURCE_TABLE"]
tgt_db = args["TARGET_DB"]
tgt_tbl = args["TARGET_TABLE"]
tgt_path = args["TARGET_S3_PATH"]

logger = glueContext.get_logger()
logger.info(f"Reading from: {src_db}.{src_tbl}")

# ✅ Read data from Landing Zone (Glue Catalog)
dyf = glueContext.create_dynamic_frame.from_catalog(
    database=src_db,
    table_name=src_tbl
)

count = dyf.count()
logger.info(f"Total records found = {count}")

if count == 0:
    logger.warn("No records found. Exiting job.")
    job.commit()
    sys.exit(0)

# ✅ Convert to Spark DataFrame
df = dyf.toDF()

# ✅ Remove duplicate header row (Glue + original file header)
columns = df.columns
first_row = df.first()

# If the first row is identical to column names → drop it
if first_row and list(first_row) == columns:
    logger.info("Duplicate header row detected — removing it.")
    df = df.filter(~(df[columns[0]] == columns[0]))
else:
    logger.info("No duplicate header in data. Proceeding without modifications.")

# ✅ Convert back to DynamicFrame
clean_dyf = DynamicFrame.fromDF(df, glueContext, "clean_dyf")

# ✅ Write data to Target S3 + Register/Update FDP Glue Table
sink = glueContext.getSink(
    connection_type="s3",
    path=tgt_path,
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE"
)

sink.setFormat("csv")
sink.setCatalogInfo(
    catalogDatabase=tgt_db,
    catalogTableName=tgt_tbl
)

sink.writeFrame(clean_dyf)

logger.info(f"✅ Data written to {tgt_path} and table {tgt_db}.{tgt_tbl} updated.")
job.commit()
