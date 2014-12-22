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
