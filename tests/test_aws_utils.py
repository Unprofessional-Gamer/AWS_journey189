"""Unit tests for AWS utilities."""

import os
import pytest
from src.aws_utils import get_aws_session

def test_get_aws_session():
    """Test AWS session creation."""
    session = get_aws_session()
    assert session is not None
    
    # Test with profile
    session_with_profile = get_aws_session(profile_name='default')
    assert session_with_profile is not None