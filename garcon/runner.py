"""
Task runners
============

The task runners are responsible for running all the tasks (either in series
or in parallel). There's only one task runner per activity. The base is
"""

from concurrent import futures
from concurrent.futures import ThreadPoolExecutor


DEFAULT_TASK_TIMEOUT = 600  # 10 minutes.


class BaseRunner():

    def estimate_timeout(self, tasks):
        """Calculate and return the timeout for an activity.

        The calculation of the timeout is pessimistic: it takes the worse case
        scenario (even for asynchronous task lists, it supposes there is only
        one thread completed at a time.)

        Args:
            tasks (list): the list of tasks the runner needs to execute.

        Return:
            str: The timeout (boto requires the timeout to be a string and not
                a regular number.)
        """

        timeout = 0

        for task in tasks:
            task_timeout = DEFAULT_TASK_TIMEOUT
            task_details = getattr(task, '__garcon__', None)

            if task_details:
                task_timeout = task_details.get(
                    'timeout', DEFAULT_TASK_TIMEOUT)

            timeout = timeout + task_timeout

        return str(timeout)

    def execute(self, activity, tasks, context):
        """Execution of the tasks.
        """

        return NotImplementedError


class Sync(BaseRunner):

    def execute(self, activity, tasks, context):
        result = dict()
        for task in tasks:
            task_context = dict(list(result.items()) + list(context.items()))
            resp = task(task_context, activity=activity)
            result.update(resp or dict())
        return result


class Async(BaseRunner):

    def __init__(self, max_workers=3):
        self.max_workers = max_workers

    def execute(self, activity, tasks, context):
        result = dict()
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            task_threads = []
            for task in tasks:
                task_threads.append(
                    executor.submit(task, context, activity=activity))

            for future in futures.as_completed(task_threads):
                data = future.result()
                result.update(data or {})
        return result
