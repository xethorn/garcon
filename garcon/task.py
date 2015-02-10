# -*- coding: utf-8 -*-
"""
Task
====

Tasks are small discrete applications that are meant to perform a defined
action within an activity. An activity can have more than one task, they can
run in series or in parallel.

Tasks can add values to the context by returning a dictionary that contains
the informations to add (useful if you need to pass information from one
task – in an activity, to another activity's task.)

Note:
    If you need a task runner that is not covered by the two scenarios below,
    you may need to just have a main task, and have this task split the work
    the way you want.
"""

import copy


def decorate(timeout=None, enable_contextify=True):
    """Generic task decorator for tasks.

    Args:
        timeout (int): The timeout of the task (see timeout).
        contextify (boolean): If the task can be contextified (see contextify).

    Return:
        callable: The wrapper.
    """

    def wrapper(fn):
        if timeout:
            _decorate(fn, 'timeout', timeout)

        if enable_contextify:
            contextify(fn)

        return fn

    return wrapper


def timeout(time):
    """Wrapper for a task to define its timeout.

    Args:
        time (int): the timeout in seconds
    """

    def wrapper(fn):
        _decorate(fn, 'timeout', time)
        return fn

    return wrapper


def _decorate(fn, key=None, value=None):
    """Add the garcon property to the function.

    Args:
        fn (callable): The function to alter.
        key (string): The key to set (optional.)
        value (any): The value to set (optional.)
    """

    if not hasattr(fn, '__garcon__'):
        setattr(fn, '__garcon__', dict())

    if key:
        fn.__garcon__.update({
            key: value
        })


def contextify(fn):
    """Decorator to take values from the context and apply them to fn.

    The goal of this decorator is to allow methods to be called with different
    values from the same context. For instance: if you need to increase the
    throughtput of two different dynamodb tables, you will need to pass a
    table name, table index, and the new throughtput.

    If you have more than one table, it gets difficult to manage. With this
    decorator, it's a little easier::

        @contextify
        def increase_dynamodb_throughtput(
                activity, context, table_name=None, table_index=None,
                table_throughtput=None):
            print(table_name)
        activity_task = increase_dynamodb_throughtput.fill(
            table_name='dynamodb.table_name1',
            table_index='dynamodb.table_index1',
            table_throughtput='dynamodb.table_throughtput1')
        context = dict(
            'dynamodb.table_name1': 'table_name',
            'dynamodb.table_index1': 'index',
            'dynamodb.table_throughtput1': 'throughtput1')
        activity_task(..., context) # shows table_name
    """

    def fill(namespace=None, **requirements):

        def wrapper(activity, context, **kwargs):
            kwargs.update(
                fill_function_call(fn, requirements, activity, context))

            response = fn(**kwargs)
            if not response or not namespace:
                return response

            return namespace_result(response, namespace)
        return wrapper

    fn.fill = fill
    return fn


def fill_function_call(fn, requirements, activity, context):
    """Fill a function calls from values from the context to the variable.

    Args:
        fn (callable): the function to call.
        requirements (dict): the requirements. The key represent the variable
            name and the value represents where the value is in the context.
        activity (ActivityWorker): the current activity worker.
        context (dict): the current context.

    Return:
        dict: The arguments to call the method with.
    """

    function_arguments = fn.__code__.co_varnames[:fn.__code__.co_argcount]
    kwargs = dict()

    for argument in function_arguments:
        value = copy.deepcopy(context.get(requirements.get(argument)))

        if argument == 'context':
            raise Exception(
                'Data used from the context should be explicit. A task should'
                ' not randomly access information from the context.')

        elif argument == 'activity':
            value = activity

        kwargs.update({
            argument: value
        })

    return kwargs


def namespace_result(dictionary, namespace):
    """Namespace the response

    This method takes the keys in the map and add a prefix to all the keys
    (the namespace)::

        resp = dict(key='value', index='storage')
        namespace_response(resp, 'namespace')
        # Returns: {'namespace.index': 'storage', 'namespace.key': 'value'}

    Args:
        dictionary (dict): The dictionary to update.
        map (dict): The keys to update.

    Return:
        Dict: the updated dictionary
    """

    if not namespace:
        return dictionary

    return {
        (namespace + '.' + key): value for key, value in dictionary.items()}
