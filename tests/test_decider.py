from unittest.mock import MagicMock
import json

import pytest

from garcon import decider
from garcon import activity
from tests.fixtures import decider as decider_events


def test_create_decider(monkeypatch):
    """Create a decider and check the behavior of the registration.
    """

    from tests.fixtures.flows import example

    d = decider.DeciderWorker(example)
    assert d.client
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


def test_get_history(monkeypatch):
    """Test the decider history
    """

    from tests.fixtures.flows import example

    events = decider_events.history.get('events')
    half = int(len(events) / 2)
    events = events[:half * 2]
    pool_1 = events[half:]
    pool_2 = events[:half]
    identity = 'identity'

    d = decider.DeciderWorker(example)
    d.client.poll_for_decision_task = MagicMock(return_value={'events': pool_2})

    resp = d.get_history(
        identity, {'events': pool_1, 'nextPageToken': 'nextPage'})

    d.client.poll_for_decision_task.assert_called_with(
        domain=example.domain,
        nextPageToken='nextPage',
        identity=identity,
        taskList=dict(name=d.task_list))
    assert len(resp) == len([
        evt for evt in events if evt['eventType'].startswith('Decision')])


def test_get_activity_states(monkeypatch):
    """Test get activity states from history.
    """

    from tests.fixtures.flows import example

    identity= 'identity'
    events = decider_events.history.get('events')
    d = decider.DeciderWorker(example)
    history = d.get_history(identity, {'events': events})
    states = d.get_activity_states(history)

    for activity_name, activity_instances in states.items():
        for activity_instance, activity_state in activity_instances.items():
            assert isinstance(activity_state, activity.ActivityState)


def test_running_workflow(monkeypatch):
    """Test running a workflow.
    """

    from tests.fixtures.flows import example

    d = decider.DeciderWorker(example, register=False)
    d.client.poll_for_decision_task = MagicMock(
        return_value=decider_events.history)
    d.client.respond_decision_task_completed = MagicMock()
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


def test_running_workflow_identity(monkeypatch):
    """Test running a workflow with and without identity.
    """

    from tests.fixtures.flows import example

    d = decider.DeciderWorker(example, register=False)
    d.client.poll_for_decision_task = MagicMock()
    d.complete = MagicMock()
    d.create_decisions_from_flow = MagicMock()

    # assert running decider without identity
    d.run()
    d.client.poll_for_decision_task.assert_called_with(
        domain=d.domain,
        taskList=dict(name=d.task_list),
        identity='')

    # assert running decider with identity
    d.run('foo')
    d.client.poll_for_decision_task.assert_called_with(
        domain=d.domain,
        taskList=dict(name=d.task_list),
        identity='foo')


def test_running_workflow_exception(monkeypatch):
    """Run a decider with an exception raised during poll.
    """

    from tests.fixtures.flows import example

    d = decider.DeciderWorker(example, register=False)
    d.client.poll_for_decision_task = MagicMock(
        return_value=decider_events.history)
    d.complete = MagicMock()
    d.create_decisions_from_flow = MagicMock()
    exception = Exception('test')
    d.client.poll_for_decision_task.side_effect = exception
    d.on_exception = MagicMock()
    d.logger.error = MagicMock()
    d.run()
    assert d.on_exception.called
    d.logger.error.assert_called_with(exception, exc_info=True)
    assert not d.complete.called


def test_create_decisions_from_flow_exception(monkeypatch):
    """Test exception is raised and workflow fails when exception raised.
    """

    from tests.fixtures.flows import example

    decider_worker = decider.DeciderWorker(example, register=False)
    decider_worker.logger.error = MagicMock()
    decider_worker.on_exception = MagicMock()

    exception = Exception('test')
    monkeypatch.setattr(decider.activity,
        'find_available_activities', MagicMock(side_effect = exception))

    decisions = []
    mock_activity_states = MagicMock()
    mock_context = MagicMock()
    decider_worker.create_decisions_from_flow(
        decisions, mock_activity_states, mock_context)

    failed_execution = dict(
        decisionType='FailWorkflowExecution',
        failWorkflowExecutionDecisionAttributes=dict(reason=str(exception)))

    assert failed_execution in decisions
    assert decider_worker.on_exception.called
    decider_worker.logger.error.assert_called_with(exception, exc_info=True)


def test_running_workflow_without_events(monkeypatch):
    """Test running a workflow without having any events.
    """

    from tests.fixtures.flows import example

    d = decider.DeciderWorker(example, register=False)
    d.client.poll_for_decision_task = MagicMock(return_value={})
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

    from tests.fixtures.flows import example

    instance = list(example.activity_1.instances({}))[0]
    decisions = []
    decider.schedule_activity_task(decisions, instance)
    expects = dict(
        decisionType='ScheduleActivityTask',
        scheduleActivityTaskDecisionAttributes=dict(
            activityId=instance.id,
            activityType=dict(
                name=instance.activity_name,
                version='1.0'),
            taskList=dict(name=instance.activity_worker.task_list),
            input=json.dumps(instance.create_execution_input()),
            heartbeatTimeout=str(instance.heartbeat_timeout),
            startToCloseTimeout=str(instance.timeout),
            scheduleToStartTimeout=str(instance.schedule_to_start),
            scheduleToCloseTimeout=str(instance.schedule_to_close)))
    assert expects in decisions


def test_schedule_activity_task_with_version(monkeypatch):
    """Test scheduling an activity task with a version.
    """

    from tests.fixtures.flows import example

    instance = list(example.activity_1.instances({}))[0]
    decisions = []
    version = '2.0'
    decider.schedule_activity_task(decisions, instance, version=version)
    expects = dict(
        decisionType='ScheduleActivityTask',
        scheduleActivityTaskDecisionAttributes=dict(
            activityId=instance.id,
            activityType=dict(
                name=instance.activity_name,
                version=version),
            taskList=dict(name=instance.activity_worker.task_list),
            input=json.dumps(instance.create_execution_input()),
            heartbeatTimeout=str(instance.heartbeat_timeout),
            startToCloseTimeout=str(instance.timeout),
            scheduleToStartTimeout=str(instance.schedule_to_start),
            scheduleToCloseTimeout=str(instance.schedule_to_close)))
    assert expects in decisions


def test_schedule_activity_task_with_custom_id(monkeypatch):
    """Test scheduling an activity task with a custom id.
    """

    from tests.fixtures.flows import example

    instance = list(example.activity_1.instances({}))[0]
    decisions = []
    custom_id = 'special_id'
    decider.schedule_activity_task(decisions, instance, id=custom_id)
    expects = dict(
        decisionType='ScheduleActivityTask',
        scheduleActivityTaskDecisionAttributes=dict(
            activityId=custom_id,
            activityType=dict(
                name=instance.activity_name,
                version='1.0'),
            taskList=dict(name=instance.activity_worker.task_list),
            input=json.dumps(instance.create_execution_input()),
            heartbeatTimeout=str(instance.heartbeat_timeout),
            startToCloseTimeout=str(instance.timeout),
            scheduleToStartTimeout=str(instance.schedule_to_start),
            scheduleToCloseTimeout=str(instance.schedule_to_close)))
    assert expects in decisions
