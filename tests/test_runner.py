from __future__ import absolute_import
try:
    from unittest.mock import MagicMock
except:
    from mock import MagicMock

import pytest

from garcon import activity
from garcon import runner
from garcon import task


EMPTY_CONTEXT = dict()


def test_execute_default_task_runner():
    """Should throw an exception.
    """

    current_runner = runner.BaseRunner()
    with pytest.raises(NotImplementedError):
        current_runner.execute(None, None)


def test_synchronous_tasks(monkeypatch):
    """Test synchronous tasks.
    """

    monkeypatch.setattr(activity.Activity, '__init__', lambda self: None)
    monkeypatch.setattr(activity.Activity, 'heartbeat', lambda self: None)

    resp = dict(foo='bar')
    current_runner = runner.Sync(
        MagicMock(), MagicMock(return_value=resp))
    current_activity = activity.Activity()
    current_activity.hydrate(dict(runner=current_runner))

    result = current_runner.execute(current_activity, EMPTY_CONTEXT)

    assert len(current_runner.tasks) == 2

    for current_task in task.flatten(current_runner.tasks, EMPTY_CONTEXT):
        assert current_task.called

    assert resp == result


def test_aynchronous_tasks(monkeypatch):
    """Test asynchronous tasks.
    """

    monkeypatch.setattr(activity.Activity, '__init__', lambda self: None)
    monkeypatch.setattr(activity.Activity, 'heartbeat', lambda self: None)

    tasks = [MagicMock() for i in range(5)]
    tasks[2].return_value = dict(oi='mondo')
    tasks[4].return_value = dict(bonjour='monde')
    expected_response = dict(
        list(tasks[2].return_value.items()) +
        list(tasks[4].return_value.items()))

    workers = 2
    current_runner = runner.Async(*tasks, max_workers=workers)

    assert current_runner.max_workers == workers
    assert len(current_runner.tasks) == len(tasks)

    current_activity = activity.Activity()
    current_activity.hydrate(dict(runner=current_runner))

    context = dict(hello='world')
    resp = current_runner.execute(current_activity, context)

    for current_task in tasks:
        assert current_task.called

    assert resp == expected_response


def test_calculate_timeout_with_no_tasks():
    """Task list without task has no timeout.
    """

    task_list = runner.BaseRunner()
    assert task_list.timeout(EMPTY_CONTEXT) == 0


def test_calculate_heartbeat_with_no_tasks():
    """Task list without tasks has no heartbeat.
    """

    task_list = runner.BaseRunner()
    assert task_list.heartbeat(EMPTY_CONTEXT) == 0


def test_calculate_default_timeout():
    """Tasks that do not have a set timeout get the default timeout.
    """

    task_list = runner.BaseRunner(lambda x: x)
    assert task_list.timeout(EMPTY_CONTEXT) == runner.DEFAULT_TASK_TIMEOUT


def test_calculate_default_heartbeat():
    """Tasks that do not have a set timeout get the default timeout.
    """

    task_list = runner.BaseRunner(lambda x: x)
    assert task_list.heartbeat(EMPTY_CONTEXT) == runner.DEFAULT_TASK_HEARTBEAT


def test_calculate_timeout():
    """Check methods that have set timeout.
    """

    timeout = 10

    @task.timeout(timeout)
    def task_a():
        pass

    current_runner = runner.BaseRunner(task_a)
    assert current_runner.timeout(EMPTY_CONTEXT) == timeout

    @task.decorate(timeout=timeout)
    def task_b():
        pass

    current_runner = runner.BaseRunner(task_b)
    assert current_runner.timeout(EMPTY_CONTEXT) == timeout

    def task_c():
        pass

    current_runner = runner.BaseRunner(task_a, task_c)
    current_timeout = current_runner.timeout(EMPTY_CONTEXT)
    expected_timeout = timeout + runner.DEFAULT_TASK_TIMEOUT
    assert current_timeout == expected_timeout


def test_calculate_heartbeat():
    """Check methods that have set timeout.
    """

    @task.decorate(heartbeat=5)
    def task_a():
        pass

    current_runner = runner.BaseRunner(task_a)
    assert current_runner.heartbeat(EMPTY_CONTEXT) == 5

    @task.decorate(heartbeat=3)
    def task_b():
        pass

    current_runner = runner.BaseRunner(task_b)
    assert current_runner.heartbeat(EMPTY_CONTEXT) == 3

    @task.decorate(heartbeat=4498)
    def task_c():
        pass

    def task_d():
        pass

    current_runner = runner.BaseRunner(
        task_a, task_b, task_c, task_d)
    assert current_runner.heartbeat(EMPTY_CONTEXT) == 4498


def test_runner_requirements():
    """Test the requirements for the runner
    """

    @task.decorate()
    def task_a():
        pass

    @task.decorate(timeout=20)
    def task_b():
        pass

    value_1 = 'context.value'
    value_2 = 'context.value_1'
    current_runner = runner.BaseRunner(
        task_a.fill(foo=value_1),
        task_b.fill(foobar=value_2))

    requirements = current_runner.requirements(EMPTY_CONTEXT)
    assert len(requirements) == 2
    assert value_1 in requirements
    assert value_2 in requirements


def test_runner_requirements_without_decoration():
    """Should just throw an exception.
    """

    def task_a():
        pass

    current_runner = runner.BaseRunner(task_a)

    with pytest.raises(runner.NoRunnerRequirementsFound):
        current_runner.requirements(EMPTY_CONTEXT)
