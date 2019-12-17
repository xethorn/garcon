try:
    from unittest.mock import MagicMock
except:
    from mock import MagicMock
import boto.exception as boto_exception
import datetime
import pytest
import json

from garcon import event, visualiser
from tests.fixtures import decider


def test_activity_summary():
    """Try to summarize an activity from events
    """

    summary = event.make_activity_summary(decider.history['events'])
    assert summary
