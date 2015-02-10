from __future__ import absolute_import
try:
    from unittest.mock import MagicMock
except:
    from mock import MagicMock
import json
import pytest

from garcon import activity
from garcon import event
from garcon import runner
from garcon import task
from tests.fixtures import decider


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
    poll.update(input=json.dumps(context))

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
    result = current_activity.complete.call_args_list[0][1].get('result')
    assert result == json.dumps(resp)


def test_task_failure(monkeypatch, poll):
    """Run an activity that has a bad task.
    """

    resp = dict(foo='bar')
    mock = MagicMock(return_value=resp)
    current_activity = activity_run(monkeypatch, poll=poll, execute=mock)
    current_activity.execute_activity.side_effect = Exception('fail')
    current_activity.run()

    assert current_activity.fail.called


def test_execute_activity(monkeypatch):
    """Test the execution of an activity.
    """

    monkeypatch.setattr(activity.Activity, '__init__', lambda self: None)
    resp = dict(task_resp='something')
    custom_task = MagicMock(return_value=resp)

    current_activity = activity.Activity()
    current_activity.tasks = runner.Sync(custom_task)

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
    assert len(worker.activities) == 4

    assert worker.flow is example
    assert not worker.worker_activities


def test_worker_run(monkeypatch):
    """Test running the worker.
    """

    monkeypatch.setattr(activity.Activity, '__init__', lambda self: None)

    from tests.fixtures.flows import example

    worker = activity.ActivityWorker(example)
    assert len(worker.activities) == 4
    for current_activity in worker.activities:
            monkeypatch.setattr(
                current_activity, 'run', MagicMock(return_value=False))

    worker.run()

    assert len(worker.activities) == 4
    for current_activity in worker.activities:
        # this check was originally `assert current_activity.run.called`
        # for some reason this fails on py2.7, so we explicitly check for `called == 1`.
        assert current_activity.run.called == 1


def test_worker_run_with_skipped_activities(monkeypatch):
    """Test running the worker with defined activities.
    """

    monkeypatch.setattr(activity.Activity, '__init__', lambda self: None)
    monkeypatch.setattr(activity.Activity, 'run', MagicMock(return_value=False))
    from tests.fixtures.flows import example

    worker = activity.ActivityWorker(example, activities=['activity_1'])
    assert len(worker.worker_activities) == 1
    for current_activity in worker.activities:
            monkeypatch.setattr(
                current_activity, 'run', MagicMock(return_value=False))

    worker.run()

    for current_activity in worker.activities:
        if current_activity.name == 'activity_1':
            assert current_activity.run.called
        else:
            assert not current_activity.run.called


def test_worker_infinite_loop():
    """Test the worker runner.
    """

    spy = MagicMock()

    class Activity:
        def __init__(self):
            self.count = 0

        def run(self):
            spy()
            self.count = self.count + 1
            if self.count < 5:
                return True
            return False

    activity.worker_runner(Activity())
    assert spy.called
    assert spy.call_count == 5


def test_activity_launch_sequence():
    """Test available activities.
    """

    from tests.fixtures.flows import example

    # First available activity is the activity_1.
    history = event.activity_states_from_events(decider.history['events'][:1])
    activities = list(activity.find_available_activities(example, history))
    uncomplete = list(activity.find_uncomplete_activities(example, history))
    assert len(activities) == 1
    assert len(uncomplete) == 4
    assert activities[0] == example.activity_1

    # In between activities should not launch activities.
    history = event.activity_states_from_events(decider.history['events'][:5])
    activities = list(activity.find_available_activities(example, history))
    uncomplete = list(activity.find_uncomplete_activities(example, history))
    assert len(activities) == 0
    assert len(uncomplete) == 4

    # Two activities are launched in parallel: 2 and 3.
    history = event.activity_states_from_events(decider.history['events'][:7])
    activities = list(activity.find_available_activities(example, history))
    uncomplete = list(activity.find_uncomplete_activities(example, history))
    assert len(activities) == 2
    assert example.activity_1 not in uncomplete

    # Activity 3 completes before activity 2. Activity 4 depends on 2 and 3 to
    # complete.
    history = event.activity_states_from_events(decider.history['events'][:14])
    activities = list(activity.find_available_activities(example, history))
    uncomplete = list(activity.find_uncomplete_activities(example, history))
    assert len(activities) == 0
    assert example.activity_3 not in uncomplete

    # Activity 2 - 3 completed.
    history = event.activity_states_from_events(decider.history['events'][:22])
    activities = list(activity.find_available_activities(example, history))
    uncomplete = list(activity.find_uncomplete_activities(example, history))
    assert len(activities) == 1
    assert activities[0] == example.activity_4
    assert example.activity_1 not in uncomplete
    assert example.activity_2 not in uncomplete
    assert example.activity_3 not in uncomplete

    # Close
    history = event.activity_states_from_events(decider.history['events'][:25])
    activities = list(activity.find_available_activities(example, history))
    uncomplete = list(activity.find_uncomplete_activities(example, history))
    assert not activities
    assert not uncomplete
