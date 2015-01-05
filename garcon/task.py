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

from concurrent import futures
from concurrent.futures import ThreadPoolExecutor


DEFAULT_TASK_TIMEOUT = 600  # 10 minutes.


class Tasks():

    def __init__(self, *args):
        self.tasks = args

    @property
    def timeout(self):
        """Calculate and return the timeout for an activity.

        The calculation of the timeout is pessimistic: it takes the worse case
        scenario (even for asynchronous task lists, it supposes there is only
        one thread completed at a time.)

        Return:
            int: The timeout.
        """

        timeout = 0

        for task in self.tasks:
            task_timeout = DEFAULT_TASK_TIMEOUT
            task_details = getattr(task, '__garcon__', None)

            if task_details:
                task_timeout = task_details.get(
                    'timeout', DEFAULT_TASK_TIMEOUT)

            timeout = timeout + task_timeout

        return timeout


class SyncTasks(Tasks):

    def execute(self, context):
        result = dict()
        for task in self.tasks:
            resp = task(context)
            result.update(resp or dict())
        return result


class AsyncTasks(Tasks):

    def __init__(self, *args, max_workers=3):
        self.tasks = args
        self.max_workers = max_workers

    def execute(self, context):
        result = dict()
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            tasks = []
            for task in self.tasks:
                tasks.append(executor.submit(task, context))

            for future in futures.as_completed(tasks):
                data = future.result()
                result.update(data or {})
        return result


def timeout(time):
    """Wrapper for a task to define its timeout.

    Args:
        time (int): the timeout in seconds
    """

    def wrapper(fn):
        decorate(fn, 'timeout', time)
        return fn

    return wrapper


def decorate(fn, key=None, value=None):
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
