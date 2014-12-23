from unittest.mock import MagicMock
import msgpack
import pytest

from garcon import activity
from garcon import task


def activity_run(
        monkeypatch, poll=None, complete=None, fail=None, execute=None):
    """Create an activity.
    """

    monkeypatch.setattr(activity.Activity, '__init__', lambda self: None)

    current_activity = activity.Activity()
    poll = poll or dict()

    monkeypatch.setattr(
        current_activity, 'execute_activity',
        execute or MagicMock(return_value=dict()))

    monkeypatch.setattr(current_activity, 'poll',
        MagicMock(return_value=poll))

    monkeypatch.setattr(current_activity, 'complete',
        complete or MagicMock())

    monkeypatch.setattr(current_activity, 'fail',
        fail or MagicMock())

    return current_activity


@pytest.fixture
def poll():
    return dict(activityId='something')


def test_run_activity(monkeypatch, poll):
    """Run an activity.
    """

    current_activity = activity_run(monkeypatch, poll=poll)
    current_activity.run()

    assert current_activity.poll.called
    assert current_activity.execute_activity.called
    assert current_activity.complete.called


def test_run_activity_without_id(monkeypatch):
    """Run an activity without an activity id.
    """

    current_activity = activity_run(monkeypatch, dict())
    current_activity.run()

    assert current_activity.poll.called
    assert not current_activity.execute_activity.called
    assert not current_activity.complete.called


def test_run_activity_with_context(monkeypatch, poll):
    """Run an activity with a context.
    """

    context = dict(foo='bar')
    poll.update(input=msgpack.packb(context))

    current_activity = activity_run(monkeypatch, poll=poll)
    current_activity.run()

    activity_context = current_activity.execute_activity.call_args[0][0]
    assert activity_context == context


def test_run_activity_with_result(monkeypatch, poll):
    """Run an activity with a result.
    """

    resp = dict(foo='bar')
    mock = MagicMock(return_value=resp)
    current_activity = activity_run(monkeypatch, poll=poll, execute=mock)
    current_activity.run()

    assert current_activity.complete.call_args[0][0] == msgpack.packb(resp)


def test_task_failure(monkeypatch, poll):
    """Run an activity that has a bad task.
    """

    resp = dict(foo='bar')
    mock = MagicMock(return_value=resp)
    current_activity = activity_run(monkeypatch, poll=poll, execute=mock)
    current_activity.execute_activity.side_effect = Exception('fail')

    with pytest.raises(Exception):
        current_activity.run()

    assert current_activity.fail.called


def test_execute_activity(monkeypatch):
    """Test the execution of an activity.
    """

    monkeypatch.setattr(activity.Activity, '__init__', lambda self: None)
    resp = dict(task_resp='something')
    custom_task = MagicMock(return_value=resp)

    current_activity = activity.Activity()
    current_activity.tasks = task.SyncTasks(custom_task)

    val = current_activity.execute_activity(dict(foo='bar'))

    assert custom_task.called
    assert val == resp


def test_hydrate_activity(monkeypatch):
    """Test the hydratation of an activity.
    """

    monkeypatch.setattr(activity.Activity, '__init__', lambda self: None)
    current_activity = activity.Activity()
    current_activity.hydrate(dict(
        name='activity',
        domain='domain',
        requires=[],
        tasks=[lambda: dict('val')]))


def test_create_activity_worker(monkeypatch):
    """Test the creation of an activity worker.
    """

    monkeypatch.setattr(activity.Activity, '__init__', lambda self: None)
    from tests.fixtures.flows import example

    worker = activity.ActivityWorker(example)
    assert worker.flow is example
    assert len(worker.activities) == 4
    assert not worker.worker_activities


def test_worker_run(monkeypatch):
    """Test running the worker.
    """

    monkeypatch.setattr(activity.Activity, '__init__', lambda self: None)
    monkeypatch.setattr(activity.Activity, 'run', MagicMock(return_value=False))
    from tests.fixtures.flows import example

    worker = activity.ActivityWorker(example)
    worker.run()

    assert len(worker.activities) == 4
    for current_activity in worker.activities:
        assert current_activity.run.called
