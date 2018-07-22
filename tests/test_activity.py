from __future__ import absolute_import
from __future__ import print_function
try:
    from unittest.mock import MagicMock
    from unittest.mock import ANY
except:
    from mock import MagicMock
    from mock import ANY
from botocore import exceptions
import json
import pytest
import sys

from garcon import activity
from garcon import event
from garcon import runner
from garcon import task
from garcon import utils
from tests.fixtures import decider


def activity_run(
        monkeypatch, boto_client, poll=None, complete=None, fail=None,
        execute=None):
    """Create an activity.
    """

    current_activity = activity.Activity(boto_client)
    poll = poll or dict()

    monkeypatch.setattr(
        current_activity, 'execute_activity',
        execute or MagicMock(return_value=dict()))
    monkeypatch.setattr(
        boto_client, 'poll_for_activity_task', MagicMock(return_value=poll))
    monkeypatch.setattr(
        boto_client, 'respond_activity_task_completed', complete or MagicMock())
    monkeypatch.setattr(
        boto_client, 'respond_activity_task_failed', fail or MagicMock())

    return current_activity


@pytest.fixture(params=[0, 1, 2])
def generators(request):
    generators = []

    if request.param >= 1:
        def igenerator(context):
            for i in range(10):
                yield {'i': i}

        generators.append(igenerator)

    if request.param == 2:
        def dgenerator(context):
            for i in range(10):
                yield {'d': i * 2}
        generators.append(dgenerator)

    return generators


@pytest.fixture
def poll():
    return dict(
        activityId='something',
        taskToken='taskToken')


def test_poll_for_activity(monkeypatch, poll, boto_client):
    """Test that poll_for_activity successfully polls.
    """

    activity_task = poll
    current_activity = activity_run(monkeypatch, boto_client, poll)
    boto_client.poll_for_activity_task.return_value = activity_task

    activity_execution = current_activity.poll_for_activity()
    assert boto_client.poll_for_activity_task.called
    assert activity_execution.task_token is poll.get('taskToken')


def test_poll_for_activity_throttle_retry(monkeypatch, poll, boto_client):
    """Test that SWF throttles are retried during polling.
    """

    current_activity = activity_run(monkeypatch, boto_client, poll)
    boto_client.poll_for_activity_task.side_effect = exceptions.ClientError(
        {'Error': {'Code': 'ThrottlingException'}},
        'operation name')

    with pytest.raises(exceptions.ClientError):
        current_activity.poll_for_activity()
    assert boto_client.poll_for_activity_task.call_count == 5


def test_poll_for_activity_error(monkeypatch, poll, boto_client):
    """Test that non-throttle errors during poll are thrown.
    """

    current_activity = activity_run(monkeypatch, boto_client, poll)

    exception = Exception()
    boto_client.poll_for_activity_task.side_effect = exception

    with pytest.raises(Exception):
        current_activity.poll_for_activity()


def test_poll_for_activity_identity(monkeypatch, poll, boto_client):
    """Test that identity is passed to poll_for_activity.
    """

    current_activity = activity_run(monkeypatch, boto_client, poll)

    current_activity.poll_for_activity(identity='foo')
    boto_client.poll_for_activity_task.assert_called_with(
        domain=ANY, taskList=ANY, identity='foo')


def test_poll_for_activity_no_identity(monkeypatch, poll, boto_client):
    """Test poll_for_activity works without identity passed as param.
    """

    current_activity = activity_run(monkeypatch, boto_client, poll)

    current_activity.poll_for_activity()
    boto_client.poll_for_activity_task.assert_called_with(
        domain=ANY, taskList=ANY)


def test_run_activity(monkeypatch, poll, boto_client):
    """Run an activity.
    """

    current_activity = activity_run(monkeypatch, boto_client, poll=poll)
    current_activity.run()

    boto_client.poll_for_activity_task.assert_called_with(
        domain=ANY, taskList=ANY)
    assert current_activity.execute_activity.called
    assert boto_client.respond_activity_task_completed.called


def test_run_activity_identity(monkeypatch, poll, boto_client):
    """Run an activity with identity as param.
    """

    current_activity = activity_run(monkeypatch, boto_client, poll=poll)
    current_activity.run(identity='foo')

    boto_client.poll_for_activity_task.assert_called_with(
        domain=ANY, taskList=ANY, identity='foo')
    assert current_activity.execute_activity.called
    assert boto_client.respond_activity_task_completed.called


def test_run_capture_exception(monkeypatch, poll, boto_client):
    """Run an activity with an exception raised during activity execution.
    """

    current_activity = activity_run(monkeypatch, boto_client, poll)
    current_activity.on_exception = MagicMock()
    current_activity.execute_activity = MagicMock()
    error_msg_long = "Error" * 100
    actual_error_msg = error_msg_long[:255]
    current_activity.execute_activity.side_effect = Exception(error_msg_long)
    current_activity.run()

    assert boto_client.poll_for_activity_task.called
    assert current_activity.execute_activity.called
    assert current_activity.on_exception.called
    boto_client.respond_activity_task_failed.assert_called_with(
        taskToken=poll.get('taskToken'),
        reason=actual_error_msg)
    assert not boto_client.respond_activity_task_completed.called


def test_run_capture_poll_exception(monkeypatch, boto_client, poll):
    """Run an activity with an exception raised during poll.
    """

    current_activity = activity_run(monkeypatch, boto_client, poll=poll)

    current_activity.on_exception = MagicMock()
    current_activity.execute_activity = MagicMock()

    exception = Exception('poll exception')
    boto_client.poll_for_activity_task.side_effect = exception
    current_activity.run()

    assert boto_client.poll_for_activity_task.called
    assert current_activity.on_exception.called
    assert not current_activity.execute_activity.called
    assert not boto_client.respond_activity_task_completed.called

    current_activity.on_exception = None
    current_activity.logger.error = MagicMock()
    current_activity.run()
    current_activity.logger.error.assert_called_with(exception, exc_info=True)


def test_run_activity_without_id(monkeypatch, boto_client):
    """Run an activity without an activity id.
    """

    current_activity = activity_run(monkeypatch, boto_client, poll=dict())
    current_activity.run()

    assert boto_client.poll_for_activity_task.called
    assert not current_activity.execute_activity.called
    assert not boto_client.respond_activity_task_completed.called


def test_run_activity_with_context(monkeypatch, boto_client, poll):
    """Run an activity with a context.
    """

    context = dict(foo='bar')
    poll.update(input=json.dumps(context))

    current_activity = activity_run(monkeypatch, boto_client, poll=poll)
    current_activity.run()

    activity_execution = current_activity.execute_activity.call_args[0][0]
    assert activity_execution.context == context


def test_run_activity_with_result(monkeypatch, boto_client, poll):
    """Run an activity with a result.
    """

    result = dict(foo='bar')
    mock = MagicMock(return_value=result)
    current_activity = activity_run(monkeypatch, boto_client, poll=poll,
        execute=mock)
    current_activity.run()
    boto_client.respond_activity_task_completed.assert_called_with(
        result=json.dumps(result), taskToken=poll.get('taskToken'))


def test_task_failure(monkeypatch, boto_client, poll):
    """Run an activity that has a bad task.
    """

    resp = dict(foo='bar')
    mock = MagicMock(return_value=resp)
    reason = 'fail'
    current_activity = activity_run(monkeypatch, boto_client, poll=poll,
        execute=mock)
    current_activity.execute_activity.side_effect = Exception(reason)
    current_activity.run()

    boto_client.respond_activity_task_failed.assert_called_with(
        taskToken=poll.get('taskToken'),
        reason=reason)


def test_task_failure_on_close_activity(monkeypatch, boto_client, poll):
    """Run an activity failure when the task is already closed.
    """

    resp = dict(foo='bar')
    mock = MagicMock(return_value=resp)
    current_activity = activity_run(monkeypatch, boto_client, poll=poll,
        execute=mock)
    current_activity.execute_activity.side_effect = Exception('fail')
    boto_client.respond_activity_task_failed.side_effect = Exception('fail')
    current_activity.unset_log_context = MagicMock()
    current_activity.run()

    assert current_activity.unset_log_context.called


def test_execute_activity(monkeypatch, boto_client):
    """Test the execution of an activity.
    """

    monkeypatch.setattr(activity.ActivityExecution, 'heartbeat',
        lambda self: None)

    resp = dict(task_resp='something')
    custom_task = MagicMock(return_value=resp)

    current_activity = activity.Activity(boto_client)
    current_activity.runner = runner.Sync(custom_task)

    val = current_activity.execute_activity(activity.ActivityExecution(
        boto_client, 'activityId', 'taskToken', '{"context": "value"}'))

    assert custom_task.called
    assert val == resp


def test_hydrate_activity(monkeypatch, boto_client):
    """Test the hydratation of an activity.
    """

    current_activity = activity.Activity(boto_client)
    current_activity.hydrate(dict(
        name='activity',
        domain='domain',
        requires=[],
        on_exception=lambda actor, exception: print(exception),
        tasks=[lambda: dict('val')]))


def test_create_activity(monkeypatch, boto_client):
    """Test the creation of an activity via `create`.
    """

    create = activity.create(boto_client, 'domain_name', 'flow_name')

    current_activity = create(name='activity_name')
    assert isinstance(current_activity, activity.Activity)
    assert current_activity.name == 'flow_name_activity_name'
    assert current_activity.task_list == 'flow_name_activity_name'
    assert current_activity.domain is 'domain_name'
    assert current_activity.client is boto_client


def test_create_external_activity(monkeypatch, boto_client):
    """Test the creation of an external activity via `create`.
    """

    create = activity.create(boto_client, 'domain_name', 'flow_name')

    current_activity = create(
        name='activity_name',
        timeout=60,
        heartbeat=40,
        external=True)

    assert isinstance(current_activity, activity.ExternalActivity)
    assert current_activity.name == 'flow_name_activity_name'
    assert current_activity.task_list == 'flow_name_activity_name'
    assert current_activity.domain == 'domain_name'

    assert isinstance(current_activity.runner, runner.External)
    assert current_activity.runner.heartbeat() == 40
    assert current_activity.runner.timeout() == 60


def test_create_activity_worker(monkeypatch):
    """Test the creation of an activity worker.
    """

    from tests.fixtures.flows import example

    worker = activity.ActivityWorker(example)
    assert len(worker.activities) == 4

    assert worker.flow is example
    assert not worker.worker_activities


def test_instances_creation(monkeypatch, boto_client, generators):
    """Test the creation of an activity instance id with the use of a local
    context.
    """

    local_activity = activity.Activity(boto_client)
    external_activity = activity.ExternalActivity(timeout=60)

    for current_activity in [local_activity, external_activity]:
        current_activity.generators = generators

        if len(current_activity.generators):
            instances = list(current_activity.instances(dict()))
            assert len(instances) == pow(10, len(generators))
            for instance in instances:
                assert isinstance(instance.local_context.get('i'), int)

                if len(generators) == 2:
                    assert isinstance(instance.local_context.get('d'), int)
        else:
            instances = list(current_activity.instances(dict()))
            assert len(instances) == 1
            assert isinstance(instances[0].local_context, dict)
            # Context is empty since no generator was used.
            assert not instances[0].local_context


def test_activity_timeouts(monkeypatch, boto_client, generators):
    """Test the creation of an activity timeouts.

    More details: the timeout of a task is 120s, the schedule to start is 1000,
    100 activities are going to be scheduled when the generator is set. The
    schedule_to_start for all activities instance is: 10000 * 100 = 100k. The
    schedule to close is 100k + duration of an activity (which is 120s * 2).
    """

    timeout = 120
    start_timeout = 1000

    @task.decorate(timeout=timeout)
    def local_task():
        return

    current_activity = activity.Activity(boto_client)
    current_activity.hydrate(dict(schedule_to_start=start_timeout))
    current_activity.generators = generators
    current_activity.runner = runner.Sync(
        local_task.fill(),
        local_task.fill())

    total_generators = pow(10, len(current_activity.generators))
    schedule_to_start = start_timeout * total_generators
    for instance in current_activity.instances({}):
        assert current_activity.pool_size == total_generators
        assert instance.schedule_to_start == schedule_to_start
        assert instance.timeout == timeout * 2
        assert instance.schedule_to_close == (
            schedule_to_start + instance.timeout)


def test_external_activity_timeouts(monkeypatch, boto_client, generators):
    """Test the creation of an external activity timeouts.
    """

    timeout = 120
    start_timeout = 1000

    current_activity = activity.ExternalActivity(timeout=timeout)
    current_activity.hydrate(dict(schedule_to_start=start_timeout))
    current_activity.generators = generators

    total_generators = pow(10, len(current_activity.generators))
    schedule_to_start = start_timeout * total_generators
    for instance in current_activity.instances({}):
        assert current_activity.pool_size == total_generators
        assert instance.schedule_to_start == schedule_to_start
        assert instance.timeout == timeout
        assert instance.schedule_to_close == (
            schedule_to_start + instance.timeout)


@pytest.mark.skipif(sys.version_info < (3, 0), reason="requires Python3")
def test_worker_run(monkeypatch, boto_client):
    """Test running the worker.
    """

    from tests.fixtures.flows import example

    worker = activity.ActivityWorker(example)
    assert len(worker.activities) == 4
    for current_activity in worker.activities:
        monkeypatch.setattr(
            current_activity, 'run', MagicMock(return_value=False))

    worker.run()

    assert len(worker.activities) == 4
    for current_activity in worker.activities:
        assert current_activity.run.called


def test_worker_run_with_skipped_activities(monkeypatch):
    """Test running the worker with defined activities.
    """

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

        def run(self, identity=None):
            spy()
            self.count = self.count + 1
            if self.count < 5:
                return True
            return False

    activity_worker = Activity()
    activity_worker.name = 'activity_name'
    activity_worker.logger = MagicMock()
    activity.worker_runner(activity_worker)
    assert spy.called
    assert spy.call_count == 5


def test_worker_infinite_loop_on_external(monkeypatch):
    """There is no worker for external activities.
    """

    external_activity = activity.ExternalActivity(timeout=10)
    current_run = external_activity.run
    spy = MagicMock()

    def run():
        spy()
        return current_run()

    monkeypatch.setattr(external_activity, 'run', run)
    activity.worker_runner(external_activity)

    # This test might not fail, but it will hang the test suite since it is
    # going to trigger an infinite loop.
    assert spy.call_count == 1


def test_activity_launch_sequence():
    """Test available activities.
    """

    from tests.fixtures.flows import example

    # First available activity is the activity_1.
    context = dict()
    history = event.activity_states_from_events(decider.history['events'][:1])
    activities = list(
        activity.find_available_activities(example, history, context))
    uncomplete = list(
        activity.find_uncomplete_activities(example, history, context))
    assert len(activities) == 1
    assert len(uncomplete) == 4
    assert activities[0].activity_worker == example.activity_1

    # In between activities should not launch activities.
    history = event.activity_states_from_events(decider.history['events'][:5])
    activities = list(
        activity.find_available_activities(example, history, context))
    uncomplete = list(
        activity.find_uncomplete_activities(example, history, context))
    assert len(activities) == 0
    assert len(uncomplete) == 4

    # Two activities are launched in parallel: 2 and 3.
    history = event.activity_states_from_events(decider.history['events'][:7])
    activities = list(
        activity.find_available_activities(example, history, context))
    uncomplete = list(
        activity.find_uncomplete_activities(example, history, context))
    assert len(activities) == 2
    assert example.activity_1 not in uncomplete

    # Activity 3 completes before activity 2. Activity 4 depends on 2 and 3 to
    # complete.
    history = event.activity_states_from_events(decider.history['events'][:14])
    activities = list(
        activity.find_available_activities(example, history, context))
    uncomplete = list(
        activity.find_uncomplete_activities(example, history, context))
    assert len(activities) == 0
    assert example.activity_3 not in uncomplete

    # Activity 2 - 3 completed.
    history = event.activity_states_from_events(decider.history['events'][:22])
    activities = list(
        activity.find_available_activities(example, history, context))
    uncomplete = list(
        activity.find_uncomplete_activities(example, history, context))
    assert len(activities) == 1
    assert activities[0].activity_worker == example.activity_4
    assert example.activity_1 not in uncomplete
    assert example.activity_2 not in uncomplete
    assert example.activity_3 not in uncomplete

    # Close
    history = event.activity_states_from_events(decider.history['events'][:25])
    activities = list(
        activity.find_available_activities(example, history, context))
    uncomplete = list(
        activity.find_uncomplete_activities(example, history, context))
    assert not activities
    assert not uncomplete


def test_create_activity_instance():
    """Test the creation of an activity instance.
    """

    activity_mock = MagicMock()
    activity_mock.name = 'foobar'
    activity_mock.retry = 20

    instance = activity.ActivityInstance(activity_mock)

    assert activity_mock.name == instance.activity_name
    assert activity_mock.retry == instance.retry


def test_create_activity_instance_id(monkeypatch):
    """Test the creation of an activity instance id.
    """

    monkeypatch.setattr(utils, 'create_dictionary_key', MagicMock())

    activity_mock = MagicMock()
    activity_mock.name = 'activity'
    instance = activity.ActivityInstance(activity_mock)

    # No context was passed, so create_dictionary key didn't need to be
    # called.
    assert instance.id == activity_mock.name + '-1'
    assert not utils.create_dictionary_key.called


def test_create_activity_instance_id_with_local_context(monkeypatch):
    """Test the creation of an activity instance id with the use of a local
    context.
    """

    monkeypatch.setattr(utils, 'create_dictionary_key', MagicMock())

    activity_mock = MagicMock()
    activity_mock.name = 'activity'
    instance = activity.ActivityInstance(activity_mock, dict(foobar='yes'))

    assert instance.id.startswith(activity_mock.name)
    assert utils.create_dictionary_key.called


def test_create_activity_instance_input_without_runner(monkeypatch):
    """Test the creation of a context for an activity instance input without
    specifying a runner.
    """

    activity_mock = MagicMock()
    activity_mock.name = 'activity'
    activity_mock.runner = None
    context = dict(context='yes')
    instance = activity.ActivityInstance(activity_mock, context)

    with pytest.raises(runner.RunnerMissing):
        instance.create_execution_input()


def test_create_activity_instance_input(monkeypatch):
    """Test the creation of a context for an activity instance input.
    """

    @task.decorate()
    def task_a(value):
        pass

    activity_mock = MagicMock()
    activity_mock.name = 'activity'
    activity_mock.runner = runner.BaseRunner(task_a.fill(value='context'))
    instance = activity.ActivityInstance(
        activity_mock, local_context=dict(context='yes', unused='no'),
        execution_context=dict(somemore='values'))
    resp = instance.create_execution_input()

    assert len(resp) == 4
    assert resp.get('context') == 'yes'
    assert 'somemore' not in resp
    assert 'unused' not in resp
    assert 'execution.domain' in resp
    assert 'execution.run_id' in resp
    assert 'execution.workflow_id' in resp


def test_create_activity_instance_input_without_decorate(monkeypatch):
    """Test the creation of a context input without the use of a decorator.
    """

    def task_a(value):
        pass

    activity_mock = MagicMock()
    activity_mock.name = 'activity'
    context = dict(foo='bar')
    local_context = dict(context='yes')

    activity_mock.runner = runner.BaseRunner(task_a)
    instance = activity.ActivityInstance(
        activity_mock, local_context=local_context,
        execution_context=context)

    resp = instance.create_execution_input()
    assert resp.get('foo') == 'bar'
    assert resp.get('context') == 'yes'


def test_create_activity_instance_input_with_zero_or_empty_values(
        monkeypatch):
    """Test the creation of a context for an activity instance input.
    """

    @task.decorate()
    def task_a(value1, value2, value3, value4):
        pass

    activity_mock = MagicMock()
    activity_mock.name = 'activity'
    activity_mock.runner = runner.BaseRunner(
        task_a.fill(
            value1='zero',
            value2='empty_list',
            value3='empty_dict',
            value4='none'))
    instance = activity.ActivityInstance(
        activity_mock,
        local_context=dict(
            zero=0, empty_list=[], empty_dict={}, none=None))

    resp = instance.create_execution_input()

    assert len(resp) == 6
    assert resp.get('zero') == 0
    assert resp.get('empty_list') == []
    assert resp.get('empty_dict') == {}
    assert 'none' not in resp


def test_activity_state():
    """Test the creation of the activity state.
    """

    activity_id = 'id'
    state = activity.ActivityState(activity_id)
    assert state.activity_id is activity_id
    assert not state.get_last_state()

    state.add_state(activity.ACTIVITY_FAILED)
    state.add_state(activity.ACTIVITY_COMPLETED)
    assert len(state.states) == 2
    assert state.get_last_state() is activity.ACTIVITY_COMPLETED

    result = 'foobar'
    state.set_result(result)
    assert state.result == result

    with pytest.raises(Exception):
        state.set_result('shouldnt reset')

    assert state.result == result
