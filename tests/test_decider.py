from unittest.mock import MagicMock
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
