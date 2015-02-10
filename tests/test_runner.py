from __future__ import absolute_import
try:
    from unittest.mock import MagicMock
except:
    from mock import MagicMock

import pytest

from garcon import runner
from garcon import task


def test_synchronous_tasks():
    """Test synchronous tasks.
    """

    resp = dict(foo='bar')
    current_runner = runner.Sync(
        MagicMock(), MagicMock(return_value=resp))
    result = current_runner.execute(None, dict())

    assert len(current_runner.tasks) == 2

    for current_task in current_runner.tasks:
        assert current_task.called

    assert resp == result


def test_aynchronous_tasks():
    """Test asynchronous tasks.
    """

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

    context = dict(hello='world')
    resp = current_runner.execute(None, context)

    for current_task in tasks:
        assert current_task.called

    assert resp == expected_response


def test_calculate_timeout_with_no_tasks():
    """Task list without task has no timeout.
    """

    task_list = runner.BaseRunner()
    assert task_list.timeout == '0'


def test_calculate_default_timeout():
    """Tasks that do not have a set timeout get the default timeout.
    """

    task_list = runner.BaseRunner(lambda x: x)
    assert task_list.timeout == str(runner.DEFAULT_TASK_TIMEOUT)


def test_calculate_timeout():
    """Check methods that have set timeout.
    """

    timeout = 10

    @task.timeout(timeout)
    def task_a():
        pass

    current_runner = runner.BaseRunner(task_a)
    assert current_runner.timeout == str(timeout)

    def task_b():
        pass

    current_runner = runner.BaseRunner(task_a, task_b)
    assert current_runner.timeout == str(timeout + runner.DEFAULT_TASK_TIMEOUT)
