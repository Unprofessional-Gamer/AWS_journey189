import boto3
import yaml
import json
import os

# Load configuration
with open("C:/Users/seera/AWS/src/catalog_sync/config/config.yaml", "r") as f:
    CONFIG = yaml.safe_load(f)

REGION = CONFIG["aws"]["region"]
BUCKET = CONFIG["aws"]["bucket_name"]
ROLE_NAME = CONFIG["aws"]["role_name"]
LANDING_DB = CONFIG["glue"]["landing_db"]
FDP_DB = CONFIG["glue"]["fdp_db"]

# Initialize AWS clients
s3 = boto3.client("s3", region_name=REGION)
glue = boto3.client("glue", region_name=REGION)
iam = boto3.client("iam", region_name=REGION)

# ---- Step 1: Create S3 bucket ----
try:
    s3.create_bucket(
        Bucket=BUCKET,
        CreateBucketConfiguration={"LocationConstraint": REGION}
    )
    print(f"✅ Created bucket: {BUCKET}")
except s3.exceptions.BucketAlreadyOwnedByYou:
    print(f"ℹ️ Bucket '{BUCKET}' already exists.")

# Create S3 folders
folders = [
    CONFIG["s3_paths"]["source_prefix"],
    CONFIG["s3_paths"]["target_prefix"],
    CONFIG["s3_paths"]["script_prefix"],
    "temp/"
]
for folder in folders:
    s3.put_object(Bucket=BUCKET, Key=(folder))
print("✅ S3 structure prepared.")

# ---- Step 2: Create IAM Role for Glue ----
trust_policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {"Service": "glue.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }
    ]
}

permissions_policy = {
    "Version": "2012-10-17",
    "Statement": [
        {"Effect": "Allow", "Action": "s3:*", "Resource": [f"arn:aws:s3:::{BUCKET}", f"arn:aws:s3:::{BUCKET}/*"]},
        {"Effect": "Allow", "Action": "glue:*", "Resource": "*"},
        {"Effect": "Allow", "Action": "logs:*", "Resource": "*"}
    ]
}

try:
    iam.create_role(
        RoleName=ROLE_NAME,
        AssumeRolePolicyDocument=json.dumps(trust_policy),
        Description="Role for Glue ETL full access"
    )
    iam.put_role_policy(
        RoleName=ROLE_NAME,
        PolicyName="GlueFullAccessPolicy",
        PolicyDocument=json.dumps(permissions_policy)
    )
    print(f"✅ Created IAM role: {ROLE_NAME}")
except iam.exceptions.EntityAlreadyExistsException:
    print(f"ℹ️ IAM role '{ROLE_NAME}' already exists.")

# ---- Step 3: Create Glue Databases ----
for db_name in [LANDING_DB, FDP_DB]:
    try:
        glue.create_database(DatabaseInput={"Name": db_name})
        print(f"✅ Created Glue database: {db_name}")
    except glue.exceptions.AlreadyExistsException:
        print(f"ℹ️ Glue database '{db_name}' already exists.")

print("\n🎉 Infrastructure setup complete!")
print(f"Bucket: {BUCKET}")
print(f"Databases: {LANDING_DB}, {FDP_DB}")
print(f"Role: {ROLE_NAME}")
