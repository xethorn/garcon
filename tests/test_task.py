from __future__ import absolute_import
try:
    from unittest.mock import MagicMock
except:
    from mock import MagicMock

import pytest

from garcon import task
from garcon import param


def test_timeout_decorator():
    """Test the timeout decorator.
    """

    timeout = 10

    @task.timeout(timeout)
    def test():
        pass

    assert test.__garcon__.get('timeout') == timeout


def test_timeout_decorator_with_heartbeat():
    """Test the timeout decorator with heartbeat.
    """

    timeout = 20
    heartbeat = 30

    @task.timeout(timeout, heartbeat=heartbeat)
    def test():
        pass

    assert test.__garcon__.get('heartbeat') == heartbeat
    assert test.__garcon__.get('timeout') == timeout

    @task.timeout(timeout)
    @task.decorate(timeout=heartbeat)
    def test2():
        pass

    assert test2.__garcon__.get('heartbeat') == heartbeat
    assert test2.__garcon__.get('timeout') == timeout


def test_decorator():
    """Test the Decorator.

    It should add __garcon__ to the method and if a key / value is
    passed, it should add it.
    """

    def test():
        pass

    task._decorate(test)
    assert hasattr(test, '__garcon__')

    task._decorate(test, 'foo')
    assert test.__garcon__.get('foo') is None

    task._decorate(test, 'foo', 'bar')
    assert test.__garcon__.get('foo') is 'bar'


def test_generator_decorator():
    """Test the geneartor decorator.
    """

    @task.list
    def test():
        pass

    assert test.__garcon__.get('list')
    assert task.is_task_list(test)


def test_link_decorator():
    """Test linking the decorator between two methods.
    """

    def testA():
        pass

    def testB():
        pass

    task._decorate(testA, 'foo', 'value')
    task._link_decorator(testA, testB)
    assert testA.__garcon__ == testB.__garcon__
    assert testA.__garcon__.get('foo') == 'value'
    assert testB.__garcon__.get('foo') == 'value'


def test_link_decorator_with_empty_source():
    """Test linking decorators when garcon is not set on the source.
    """

    def testA():
        pass

    def testB():
        pass

    task._link_decorator(testA, testB)
    assert not getattr(testA, '__garcon__', None)
    assert len(testB.__garcon__) is 0

    task._decorate(testB, 'foo', 'value')
    assert testB.__garcon__.get('foo') == 'value'


def test_task_decorator():
    """Test the task decorator.
    """

    timeout = 40
    userinfo = 'something'

    @task.decorate(timeout=timeout)
    def test(user):
        assert user is userinfo

    assert test.__garcon__.get('timeout') == timeout
    assert test.__garcon__.get('heartbeat') == timeout
    assert callable(test.fill)

    call = test.fill(user='user')
    call(dict(user='something'))


def test_task_decorator_with_heartbeat():
    """Test the task decorator with heartbeat.
    """

    heartbeat = 50

    @task.decorate(heartbeat=heartbeat)
    def test(user):
        assert user is userinfo

    assert test.__garcon__.get('heartbeat') == heartbeat


def test_task_decorator_with_activity():
    """Test the task decorator with an activity.
    """

    current_activity = MagicMock()

    @task.decorate()
    def test(activity):
        activity()
        assert activity is current_activity

    call = test.fill()
    call(dict(), activity=current_activity)

    assert current_activity.called


def test_task_decorator_with_context():
    """Test the task decorator with an activity.
    """

    current_context = {}
    spy = MagicMock()

    @task.decorate()
    def test(context):
        spy()

    call = test.fill()

    with pytest.raises(Exception):
        call(current_context)

    assert not spy.called


def test_contextify_added_fill():
    """Verify that calling contextify on a method added .fill on the method.
    """

    @task.contextify
    def test(activity, context, something):
        pass

    assert test
    assert test.fill


def test_contextify_default_method_call():
    """Test that the contextify decorator is not altering the method itself.
    The contextify decorator should preserve the format of the method, it
    allows us to use it in more than one context.
    """

    response = dict(somekey='somevalue')
    spy = MagicMock()

    @task.contextify
    def method(activity, key_to_replace, key_to_not_replace=None):
        assert key_to_replace == 'value'
        assert key_to_not_replace is None
        spy()
        return response

    assert method(None, 'value') is response
    assert spy.called


def test_contextify():
    """Try using contextify on a method.
    """

    value = 'random'
    kwargs_value = 'more-random'
    return_value = 'a value'
    spy = MagicMock()

    @task.contextify
    def method(
        activity, key_to_replace, key_not_to_replace=None,
            kwargs_to_replace=None):

        assert not key_not_to_replace
        assert key_to_replace == value
        assert kwargs_to_replace == kwargs_value
        spy()
        return dict(return_value=return_value)

    fn = method.fill(
        key_to_replace='context.key',
        kwargs_to_replace='context.kwarg_key')

    resp = fn({'context.key': value, 'context.kwarg_key': kwargs_value})

    assert isinstance(resp, dict)
    assert resp.get('return_value') is return_value


def test_contextify_with_mapped_response():
    """Test the contextify method with mapped response.
    """

    return_value = 'a value'

    @task.contextify
    def method(
        activity, key_to_replace, key_not_to_replace=None,
            kwargs_to_replace=None):

        assert activity == 'activity'
        return dict(return_value=return_value)

    fn = method.fill(
        key_to_replace='context.key',
        kwargs_to_replace='context.kwarg_key',
        namespace='somethingrandom')

    resp = fn(
        {'context.key': 'test', 'context.kwarg_key': 'a'},
        activity='activity')

    assert isinstance(resp, dict)
    assert len(resp) == 1
    assert resp.get('somethingrandom.return_value') is return_value


def test_flatten():
    """Test the flatten function.
    """

    spy = MagicMock

    @task.decorate(timeout=10)
    def task_a(name):
        pass

    @task.decorate(timeout=10)
    def task_b(name):
        pass

    @task.list
    def task_generator(context):
        yield task_b
        if context.get('value'):
            yield task_a

    value = list(task.flatten(
        (task_a, task_b, task_generator, task_a),
        dict(value='something')))
    assert value == [task_a, task_b, task_b, task_a, task_a]


def test_fill_function_call():
    """Test filling the function call.
    """

    def test_function(activity, arg_one, key, kwarg_one=None, kwarg_two=None):
        pass

    requirements = dict(
        arg_one=param.Param('context.arg'),
        kwarg_one=param.Param('context.kwarg'))
    activity = None
    context = {
        'context.arg': 'arg.value',
        'context.kwarg': 'kwarg.value'}

    data = task.fill_function_call(
        test_function, requirements, activity, context)

    assert not data.get('activity')
    assert not data.get('context')
    assert not data.get('key')
    assert not data.get('kwarg_two')
    assert data.get('arg_one') == 'arg.value'
    assert data.get('kwarg_one') == 'kwarg.value'
    test_function(**data)


def test_namespace_result():
    """Test namespacing the results.
    """

    value = 'foo'

    resp = task.namespace_result(dict(test=value), '')
    assert resp.get('test') == value

    resp = task.namespace_result(dict(test=value), 'namespace')
    assert resp.get('namespace.test') == value
