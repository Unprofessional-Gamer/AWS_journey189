import boto3
import json
import time

# ✅ CONFIG (no need for settings.json)
CONFIG = {
    "aws_region": "eu-north-1",
    "s3_bucket": "catalog-sync-testing",
    "prefixes": ["source_catalog_file/", "Target_catalog_file/", "scripts/", "temp/"],
    "landing_db": "landing_db",
    "fdp_db": "fdp_db",
    "glue_role": "AWSGlueRole_AutoCatalog",
    "lambda_role": "LambdaCatalogRole"
}

# boto3 clients
s3 = boto3.client("s3", region_name=CONFIG["aws_region"])
glue = boto3.client("glue", region_name=CONFIG["aws_region"])
iam = boto3.client("iam", region_name=CONFIG["aws_region"])

# IAM trust policies
GLUE_TRUST = {
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Allow", "Principal": {"Service": "glue.amazonaws.com"}, "Action": "sts:AssumeRole"}]
}
LAMBDA_TRUST = {
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}]
}

# S3 policy for Glue/Lambda
def s3_policy(bucket):
    return {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": ["s3:*"], "Resource": [f"arn:aws:s3:::{bucket}", f"arn:aws:s3:::{bucket}/*"]},
            {"Effect": "Allow", "Action": "glue:*", "Resource": "*"},
            {"Effect": "Allow", "Action": ["logs:*"], "Resource": "*"}
        ]
    }

# Create S3 bucket & folders
def setup_s3():
    try:
        s3.create_bucket(Bucket=CONFIG["s3_bucket"], CreateBucketConfiguration={"LocationConstraint": CONFIG["aws_region"]})
        print("✅ Created S3 bucket")
    except:
        print("ℹ️ S3 bucket exists, skipping")
    for p in CONFIG["prefixes"]:
        s3.put_object(Bucket=CONFIG["s3_bucket"], Key=p)
    print("✅ S3 folders created")

# Create IAM role
def setup_iam(role_name, trust, policy_name):
    try:
        iam.get_role(RoleName=role_name)
        print(f"ℹ️ Role {role_name} exists")
    except:
        iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=json.dumps(trust))
        time.sleep(2)
        print(f"✅ Created role {role_name}")
    iam.put_role_policy(RoleName=role_name, PolicyName=policy_name, PolicyDocument=json.dumps(s3_policy(CONFIG["s3_bucket"])))

# Create Glue database
def setup_glue_db(db_name):
    try:
        glue.get_database(Name=db_name)
        print(f"ℹ️ Glue DB {db_name} exists")
    except:
        glue.create_database(DatabaseInput={"Name": db_name})
        print(f"✅ Created Glue DB: {db_name}")

if __name__ == "__main__":
    print("\n🚀 Setting up AWS Catalog Sync Pipeline Infrastructure...\n")
    setup_s3()
    setup_iam(CONFIG["glue_role"], GLUE_TRUST, "GlueS3Policy")
    setup_iam(CONFIG["lambda_role"], LAMBDA_TRUST, "LambdaS3Policy")
    setup_glue_db(CONFIG["landing_db"])
    setup_glue_db(CONFIG["fdp_db"])
    print("\n🎉 Setup Complete! AWS Resources Ready.")
    print(f"S3 Bucket: {CONFIG['s3_bucket']}")
    print(f"Glue DBs: {CONFIG['landing_db']}, {CONFIG['fdp_db']}")
    print(f"IAM Roles: {CONFIG['glue_role']}, {CONFIG['lambda_role']}")
