try:
    from unittest.mock import MagicMock
except:
    from mock import MagicMock

import pytest
import boto3


@pytest.fixture
def boto_client(monkeypatch):
    """Create a fake boto client."""
    return MagicMock(spec=boto3.client('swf', region_name='us-east-1'))
