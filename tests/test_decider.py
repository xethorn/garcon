from __future__ import absolute_import
try:
    from unittest.mock import MagicMock
except:
    from mock import MagicMock
import boto.swf.layer2 as swf
import json
import pytest

from garcon import decider
from garcon import activity
from tests.fixtures import decider as decider_events


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
    assert d.on_exception

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
    from tests.fixtures import decider as poll

    d = decider.DeciderWorker(example)

    # Test extracting workflow execution info
    assert d.get_workflow_execution_info(poll.history) == {
        'execution.domain': 'dev',
        'execution.run_id': '123abc=',
        'execution.workflow_id': 'test-workflow-id'}

    assert d.get_workflow_execution_info({}) is None


def test_get_history(monkeypatch):
    """Test the decider history
    """

    mock(monkeypatch)
    from tests.fixtures.flows import example

    events = decider_events.history.get('events')
    half = int(len(events) / 2)
    events = events[:half * 2]
    pool_1 = events[half:]
    pool_2 = events[:half]

    d = decider.DeciderWorker(example)
    d.poll = MagicMock(return_value={'events': pool_2})

    resp = d.get_history({'events': pool_1, 'nextPageToken': 'nextPage'})

    d.poll.assert_called_with(next_page_token='nextPage')
    assert len(resp) == len([
        evt for evt in events if evt['eventType'].startswith('Decision')])


def test_get_activity_states(monkeypatch):
    """Test get activity states from history.
    """

    mock(monkeypatch)
    from tests.fixtures.flows import example

    events = decider_events.history.get('events')
    d = decider.DeciderWorker(example)
    history = d.get_history({'events': events})
    states = d.get_activity_states(history)

    for activity_name, activity_instances in states.items():
        for activity_instance, activity_state in activity_instances.items():
            assert isinstance(activity_state, activity.ActivityState)


def test_running_workflow(monkeypatch):
    """Test running a workflow.
    """

    mock(monkeypatch)
    from tests.fixtures.flows import example

    d = decider.DeciderWorker(example)
    d.poll = MagicMock(return_value=decider_events.history)
    d.complete = MagicMock()
    d.create_decisions_from_flow = MagicMock()

    # Via flow.
    d.run()
    assert d.create_decisions_from_flow.called

    # Via custom decider
    spy = MagicMock()

    def custom_decider(schedule):
        spy()

    example.decider = custom_decider
    d.run()
    assert spy.called


def test_running_workflow_without_events(monkeypatch):
    """Test running a workflow without having any events.
    """

    mock(monkeypatch)
    from tests.fixtures.flows import example

    d = decider.DeciderWorker(example)
    d.poll = MagicMock(return_value={})
    d.get_history = MagicMock()
    d.run()

    assert not d.get_history.called


def test_schedule_context():
    """Test the schedule context.
    """

    context = decider.ScheduleContext()
    assert context.completed

    context.mark_uncompleted()
    assert not context.completed


def test_schedule_with_unscheduled_activity(monkeypatch):
    """Test the scheduling of an activity.
    """

    mock(monkeypatch)
    from tests.fixtures.flows import example

    monkeypatch.setattr(decider, 'schedule_activity_task', MagicMock())

    decisions = MagicMock()
    schedule_context = decider.ScheduleContext()
    history = {}
    current_activity = example.activity_1

    decider.schedule(
        decisions, schedule_context, history, {}, 'schedule_id',
        current_activity)

    assert decider.schedule_activity_task.called
    assert not schedule_context.completed


def test_schedule_with_scheduled_activity(monkeypatch):
    """Test the scheduling of an activity.
    """

    mock(monkeypatch)
    from tests.fixtures.flows import example

    monkeypatch.setattr(decider, 'schedule_activity_task', MagicMock())

    decisions = MagicMock()
    schedule_context = decider.ScheduleContext()
    instance_state = activity.ActivityState('activity_1')
    instance_state.add_state(activity.ACTIVITY_SCHEDULED)
    current_activity = example.activity_1
    history = {
        current_activity.name: {
            'workflow_name_activity_1-1-schedule_id': instance_state
        }
    }

    resp = decider.schedule(
        decisions, schedule_context, history, {}, 'schedule_id',
        current_activity)

    assert not decider.schedule_activity_task.called
    assert not schedule_context.completed
    assert resp.get_last_state() == activity.ACTIVITY_SCHEDULED

    with pytest.raises(activity.ActivityInstanceNotReadyException):
        resp.result.get('foo')


def test_schedule_with_completed_activity(monkeypatch):
    """Test the scheduling of an activity.
    """

    mock(monkeypatch)
    from tests.fixtures.flows import example

    monkeypatch.setattr(decider, 'schedule_activity_task', MagicMock())

    decisions = MagicMock()
    schedule_context = decider.ScheduleContext()
    instance_state = activity.ActivityState('activity_1')
    instance_state.add_state(activity.ACTIVITY_COMPLETED)
    current_activity = example.activity_1
    history = {
        current_activity.name: {
            'workflow_name_activity_1-1-schedule_id': instance_state
        }
    }

    resp = decider.schedule(
        decisions, schedule_context, history, {}, 'schedule_id',
        current_activity)

    assert not decider.schedule_activity_task.called
    assert resp.get_last_state() == activity.ACTIVITY_COMPLETED
    assert schedule_context.completed
    resp.result.get('foo')


def test_schedule_requires_with_incomplete_activities():
    """Test the scheduler.
    """

    activity_state = activity.ActivityState('activity_name')
    with pytest.raises(activity.ActivityInstanceNotReadyException):
        decider.ensure_requirements([activity_state])

    with pytest.raises(activity.ActivityInstanceNotReadyException):
        decider.ensure_requirements([None])

    activity_state.add_state(activity.ACTIVITY_COMPLETED)
    decider.ensure_requirements(requires=[activity_state])


def test_schedule_activity_task(monkeypatch):
    """Test scheduling an activity task.
    """

    mock(monkeypatch)
    from tests.fixtures.flows import example

    instance = list(example.activity_1.instances({}))[0]
    decisions = MagicMock()
    decider.schedule_activity_task(decisions, instance)
    decisions.schedule_activity_task.assert_called_with(
        instance.id,
        instance.activity_name,
        '1.0',
        task_list=instance.activity_worker.task_list,
        input=json.dumps(instance.create_execution_input()),
        heartbeat_timeout=str(instance.heartbeat_timeout),
        start_to_close_timeout=str(instance.timeout),
        schedule_to_start_timeout=str(instance.schedule_to_start),
        schedule_to_close_timeout=str(instance.schedule_to_close))


def test_schedule_activity_task_with_version(monkeypatch):
    """Test scheduling an activity task with a version.
    """

    mock(monkeypatch)
    from tests.fixtures.flows import example

    instance = list(example.activity_1.instances({}))[0]
    decisions = MagicMock()
    version = '2.0'
    decider.schedule_activity_task(decisions, instance, version=version)
    decisions.schedule_activity_task.assert_called_with(
        instance.id,
        instance.activity_name,
        version,
        task_list=instance.activity_worker.task_list,
        input=json.dumps(instance.create_execution_input()),
        heartbeat_timeout=str(instance.heartbeat_timeout),
        start_to_close_timeout=str(instance.timeout),
        schedule_to_start_timeout=str(instance.schedule_to_start),
        schedule_to_close_timeout=str(instance.schedule_to_close))


def test_schedule_activity_task_with_version(monkeypatch):
    """Test scheduling an activity task with a custom id.
    """

    mock(monkeypatch)
    from tests.fixtures.flows import example

    instance = list(example.activity_1.instances({}))[0]
    decisions = MagicMock()
    custom_id = 'special_id'
    decider.schedule_activity_task(decisions, instance, id=custom_id)
    decisions.schedule_activity_task.assert_called_with(
        custom_id,
        instance.activity_name,
        '1.0',
        task_list=instance.activity_worker.task_list,
        input=json.dumps(instance.create_execution_input()),
        heartbeat_timeout=str(instance.heartbeat_timeout),
        start_to_close_timeout=str(instance.timeout),
        schedule_to_start_timeout=str(instance.schedule_to_start),
        schedule_to_close_timeout=str(instance.schedule_to_close))
