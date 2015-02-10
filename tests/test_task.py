from __future__ import absolute_import
try:
    from unittest.mock import MagicMock
except:
    from mock import MagicMock

import pytest

from garcon import task


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

    task._decorate(test)
    assert hasattr(test, '__garcon__')

    task._decorate(test, 'foo')
    assert test.__garcon__.get('foo') is None

    task._decorate(test, 'foo', 'bar')
    assert test.__garcon__.get('foo') is 'bar'


def test_task_decorator():
    """Test the task decorator.
    """

    timeout = 40
    userinfo = 'something'

    @task.decorate(timeout=timeout)
    def test(user):
        assert user is userinfo

    assert test.__garcon__.get('timeout') == timeout
    assert callable(test.fill)

    call = test.fill(user='user')
    call(None, dict(user='something'))


def test_task_decorator_with_activity():
    """Test the task decorator with an activity.
    """

    current_activity = MagicMock()

    @task.decorate()
    def test(activity):
        activity()
        assert activity is current_activity

    call = test.fill()
    call(current_activity, dict())

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
        call(None, current_context)

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

    resp = fn(None, {'context.key': value, 'context.kwarg_key': kwargs_value})

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

        return dict(return_value=return_value)

    fn = method.fill(
        key_to_replace='context.key',
        kwargs_to_replace='context.kwarg_key',
        namespace='somethingrandom')

    resp = fn(None, {'context.key': 'test', 'context.kwarg_key': 'a'})

    assert isinstance(resp, dict)
    assert len(resp) == 1
    assert resp.get('somethingrandom.return_value') is return_value


def test_fill_function_call():
    """Test filling the function call.
    """

    def test_function(activity, arg_one, key, kwarg_one=None, kwarg_two=None):
        pass

    requirements = dict(arg_one='context.arg', kwarg_one='context.kwarg')
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
