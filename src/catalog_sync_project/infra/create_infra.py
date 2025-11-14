import boto3
import json
import time
import os

# ✅ Load config
config_path = os.path.join("config", "settings.json")
with open(config_path) as f:
    config = json.load(f)

REGION = config["aws_region"]
BUCKET = config["s3_bucket"]
PREFIXES = config["prefixes"]
GLUE_ROLE = config["glue"]["glue_role_name"]
LAMBDA_ROLE = config["lambda"]["role_name"]
LANDING_DB = config["glue"]["landing_db"]
FDP_DB = config["glue"]["fdp_db"]

# Clients
s3 = boto3.client("s3", region_name=REGION)
glue = boto3.client("glue", region_name=REGION)
iam = boto3.client("iam")

# IAM trust policy for Glue
glue_trust_policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": { "Service": "glue.amazonaws.com" },
            "Action": "sts:AssumeRole"
        }
    ]
}

# IAM trust policy for Lambda
lambda_trust_policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": { "Service": "lambda.amazonaws.com" },
            "Action": "sts:AssumeRole"
        }
    ]
}

# IAM Inline policy for accessing S3, Glue, Logs
def s3_glue_policy(bucket_name):
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:DeleteObject"
                ],
                "Resource": [
                    f"arn:aws:s3:::{bucket_name}",
                    f"arn:aws:s3:::{bucket_name}/*"
                ]
            },
            {
                "Effect": "Allow",
                "Action": [
                    "glue:*"
                ],
                "Resource": "*"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                "Resource": "*"
            }
        ]
    }

def create_s3_bucket():
    try:
        s3.create_bucket(
            Bucket=BUCKET,
            CreateBucketConfiguration={"LocationConstraint": REGION}
        )
        print(f"✅ Created S3 bucket: {BUCKET}")
    except s3.exceptions.BucketAlreadyOwnedByYou:
        print(f"ℹ️ Bucket {BUCKET} already exists (skipped)")
    except Exception as e:
        print(f"⚠ Error creating S3 bucket: {e}")

    # Create folder structure
    for p in PREFIXES.values():
        key = p.rstrip("/") + "/"
        s3.put_object(Bucket=BUCKET, Key=key)
    print("✅ S3 folders created:", ", ".join(PREFIXES.values()))

def create_iam_role(role_name, trust_policy):
    try:
        iam.get_role(RoleName=role_name)
        print(f"ℹ️ IAM Role {role_name} already exists (skipped)")
    except iam.exceptions.NoSuchEntityException:
        iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy)
        )
        print(f"✅ Created IAM Role: {role_name}")
        time.sleep(3)

def attach_policy_to_role(role_name, policy_name, policy_document):
    try:
        iam.put_role_policy(
            RoleName=role_name,
            PolicyName=policy_name,
            PolicyDocument=json.dumps(policy_document)
        )
        print(f"✅ Attached inline policy to role: {role_name}")
    except Exception as e:
        print(f"⚠ Error attaching policy: {e}")

def create_glue_database(db_name):
    try:
        glue.get_database(Name=db_name)
        print(f"ℹ️ Glue database {db_name} already exists (skipped)")
    except glue.exceptions.EntityNotFoundException:
        glue.create_database(DatabaseInput={"Name": db_name})
        print(f"✅ Created Glue database: {db_name}")

if __name__ == "__main__":
    print("\n🚀 Creating infrastructure...")

    create_s3_bucket()

    # Glue role
    create_iam_role(GLUE_ROLE, glue_trust_policy)
    attach_policy_to_role(GLUE_ROLE, "GlueS3AccessPolicy", s3_glue_policy(BUCKET))

    # Lambda role
    create_iam_role(LAMBDA_ROLE, lambda_trust_policy)
    attach_policy_to_role(LAMBDA_ROLE, "LambdaS3AccessPolicy", s3_glue_policy(BUCKET))

    # Glue databases
    create_glue_database(LANDING_DB)
    create_glue_database(FDP_DB)

    print("\n✅ Infrastructure setup complete!\n")
    print(f"➡ S3 Bucket: {BUCKET}")
    print(f"➡ IAM Roles: {GLUE_ROLE}, {LAMBDA_ROLE}")
    print(f"➡ Glue Databases: {LANDING_DB}, {FDP_DB}")
