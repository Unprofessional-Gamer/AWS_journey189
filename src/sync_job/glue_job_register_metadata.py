import sys, json
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import boto3

args = getResolvedOptions(sys.argv,
    ["JOB_NAME","S3_INPUT_PATH","GLUE_DB","GLUE_TABLE_NAME","REGION"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

s3_path = args["S3_INPUT_PATH"]
db = args["GLUE_DB"]
table = args["GLUE_TABLE_NAME"]
region = args["REGION"]

logger = glueContext.get_logger()
logger.info(f"Reading data from {s3_path}")

# Read a sample to infer schema
df = spark.read.option("header","true").option("inferSchema","true").csv(s3_path)
cols = df.schema

columns=[]
for f in cols:
    t=f.dataType.simpleString()
    glue_type="string"
    if "int" in t: glue_type="int"
    elif "long" in t: glue_type="bigint"
    elif "double" in t or "float" in t: glue_type="double"
    elif "boolean" in t: glue_type="boolean"
    columns.append({"Name":f.name,"Type":glue_type})

glue=boto3.client("glue",region_name=region)
try:
    glue.get_database(Name=db)
except glue.exceptions.EntityNotFoundException:
    glue.create_database(DatabaseInput={"Name":db})

sd={
  "Columns":columns,
  "Location":s3_path if s3_path.endswith("/") else s3_path.rsplit("/",1)[0]+"/",
  "InputFormat":"org.apache.hadoop.mapred.TextInputFormat",
  "OutputFormat":"org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
  "SerdeInfo":{
      "SerializationLibrary":"org.apache.hadoop.hive.serde2.OpenCSVSerde",
      "Parameters":{"separatorChar":",","quoteChar":"\""}
  }
}
table_input={"Name":table,"TableType":"EXTERNAL_TABLE",
             "Parameters":{"classification":"csv"},
             "StorageDescriptor":sd}

try:
    glue.get_table(DatabaseName=db,Name=table)
    glue.update_table(DatabaseName=db,TableInput=table_input)
    logger.info(f"Updated {db}.{table}")
except glue.exceptions.EntityNotFoundException:
    glue.create_table(DatabaseName=db,TableInput=table_input)
    logger.info(f"Created {db}.{table}")

job.commit()
