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
    tasks = [MagicMock(return_value=resp),]
    current_runner = runner.Sync()
    result = current_runner.execute(None, tasks, dict())

    for current_task in tasks:
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
    current_runner = runner.Async(max_workers=workers)

    assert current_runner.max_workers == workers

    context = dict(hello='world')
    resp = current_runner.execute(None, tasks, context)

    for current_task in tasks:
        assert current_task.called

    assert resp == expected_response


def test_calculate_timeout_with_no_tasks():
    """Task list without task has no timeout.
    """

    tasks = []
    current_runner = runner.BaseRunner()
    assert current_runner.estimate_timeout(tasks) == '0'


def test_calculate_default_timeout():
    """Tasks that do not have a set timeout get the default timeout.
    """

    current_runner = runner.BaseRunner()
    tasks = [lambda x: x]
    estimate_timeout = current_runner.estimate_timeout(tasks)
    assert estimate_timeout == str(runner.DEFAULT_TASK_TIMEOUT)


def test_calculate_timeout():
    """Check methods that have set timeout.
    """

    timeout = 10

    @task.timeout(timeout)
    def task_a():
        pass

    current_runner = runner.BaseRunner()
    assert current_runner.estimate_timeout([task_a]) == str(timeout)

    def task_b():
        pass

    current_runner = runner.BaseRunner()
    tasks = [task_a, task_b]
    estimate_timeout = current_runner.estimate_timeout(tasks)
    assert estimate_timeout == str(timeout + runner.DEFAULT_TASK_TIMEOUT)
