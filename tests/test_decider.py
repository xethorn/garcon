from __future__ import absolute_import
try:
    from unittest.mock import MagicMock
except:
    from mock import MagicMock
import boto.swf.layer2 as swf
from boto.swf import layer1
import pytest

from garcon import activity
from garcon import decider


def mock(monkeypatch):
    for base in [swf.Decider, swf.WorkflowType, swf.ActivityType, swf.Domain]:
        monkeypatch.setattr(base, '__init__', MagicMock(return_value=None))
        if base is not swf.Decider:
            monkeypatch.setattr(base, 'register', MagicMock())


def test_create_decider(monkeypatch):
    """Create a decider and check the behavior of the registration.
    """

    mock(monkeypatch)
    from tests.fixtures.flows import example

    d = decider.DeciderWorker(example)
    assert len(d.activities) == 4
    assert d.flow
    assert d.domain

    monkeypatch.setattr(decider.DeciderWorker, 'register', MagicMock())
    d = decider.DeciderWorker(example)
    assert d.register.called

    monkeypatch.setattr(decider.DeciderWorker, 'register', MagicMock())
    dec = decider.DeciderWorker(example, register=False)
    assert not dec.register.called

def test_get_workflow_execution_info(monkeypatch):
    """Check that the workflow execution info are properly extracted
    """

    mock(monkeypatch)
    from tests.fixtures.flows import example
    from tests.fixtures import decider as pool

    d = decider.DeciderWorker(example)

    # Test extracting workflow execution info
    assert d.get_workflow_execution_info(pool.history) == {
        'execution.domain': 'dev',
        'execution.run_id': '123abc=',
        'execution.workflow_id': 'test-workflow-id'}

    assert d.get_workflow_execution_info({}) is None