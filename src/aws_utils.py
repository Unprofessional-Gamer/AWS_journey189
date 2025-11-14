"""AWS utilities and configuration."""

import os
from dotenv import load_dotenv
import boto3

def get_aws_session(profile_name=None):
    """
    Create and return an AWS session.
    
    Args:
        profile_name (str, optional): AWS profile name to use. Defaults to None.
    
    Returns:
        boto3.Session: Configured AWS session
    """
    load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'config', '.env'))
    
    return boto3.Session(profile_name=profile_name)