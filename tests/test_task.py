from unittest.mock import MagicMock

from garcon import task


def test_synchronous_tasks():
    """Test synchronous tasks.
    """

    resp = dict(foo='bar')
    runner = task.SyncTasks(MagicMock(), MagicMock(return_value=resp))
    result = runner.execute(dict())

    assert len(runner.tasks) == 2

    for current_task in runner.tasks:
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
    runner = task.AsyncTasks(*tasks, max_workers=workers)

    assert runner.max_workers == workers
    assert len(runner.tasks) == len(tasks)

    context = dict(hello='world')
    resp = runner.execute(context)

    for current_task in tasks:
        assert current_task.called

    assert resp == expected_response


def test_timeout_decorator():
    """Test the timeout decorator.
    """

    timeout = 10
    @task.timeout(timeout)
    def test():
        pass

    assert test.__garcon__.get('timeout') == timeout


def test_decorator():
    """Test the Decorator.

    It should add __garcon__ to the method and if a key / value is
    passed, it should add it.
    """

    def test():
        pass

    task.decorate(test)
    assert hasattr(test, '__garcon__')

    task.decorate(test, 'foo')
    assert test.__garcon__.get('foo') is None

    task.decorate(test, 'foo', 'bar')
    assert test.__garcon__.get('foo') is 'bar'


def test_calculate_timeout_with_no_tasks():
    """Task list without task has no timeout.
    """

    task_list = task.Tasks()
    assert not task_list.timeout


def test_calculate_default_timeout():
    """Tasks that do not have a set timeout get the default timeout.
    """

    task_list = task.Tasks(lambda x: x)
    assert task_list.timeout == task.DEFAULT_TASK_TIMEOUT


def test_calculate_timeout():
    """Check methods that have set timeout.
    """

    timeout = 10

    @task.timeout(timeout)
    def task_a():
        pass

    task_list = task.Tasks(task_a)
    assert task_list.timeout == timeout

    def task_b():
        pass

    task_list = task.Tasks(task_a, task_b)
    assert task_list.timeout == timeout + task.DEFAULT_TASK_TIMEOUT
