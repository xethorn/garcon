"""
Task runners
============

The task runners are responsible for running all the tasks (either in series
or in parallel). There's only one task runner per activity.
"""

from concurrent import futures
from concurrent.futures import ThreadPoolExecutor

from garcon.task import flatten


DEFAULT_TASK_TIMEOUT = 600   # 10 minutes.
DEFAULT_TASK_HEARTBEAT = 600  # 10 minutes


class NoRunnerRequirementsFound(Exception):
    pass


class RunnerMissing(Exception):
    pass


class BaseRunner():

    def __init__(self, *args):
        self.tasks = args

    def timeout(self, context):
        """Calculate and return the timeout for an activity.

        The calculation of the timeout is pessimistic: it takes the worse case
        scenario (even for asynchronous task lists, it supposes there is only
        one thread completed at a time.)

        Return:
            str: The timeout (boto requires the timeout to be a string and not
                a regular number.)
        """

        timeout = 0

        for task in flatten(self.tasks, context):
            task_timeout = DEFAULT_TASK_TIMEOUT
            task_details = getattr(task, '__garcon__', None)

            if task_details:
                task_timeout = task_details.get(
                    'timeout', DEFAULT_TASK_TIMEOUT)

            timeout = timeout + task_timeout

        return timeout

    def heartbeat(self, context):
        """Calculate and return the heartbeat for an activity.

        The heartbeat represents when an actvitity should be sending a signal
        to SWF that it has not completed yet. The heartbeat is sent everytime
        a new task is going to be launched.

        Similar to the `BaseRunner.timeout`, the heartbeat is pessimistic, it
        looks at the largest heartbeat and set it up.

        Return:
            str: The heartbeat timeout (boto requires the timeout to be a
                string not a regular number.)
        """

        heartbeat = 0

        for task in flatten(self.tasks, context):
            task_details = getattr(task, '__garcon__', None)
            task_heartbeat = DEFAULT_TASK_HEARTBEAT

            if task_details:
                task_heartbeat = task_details.get(
                    'heartbeat', DEFAULT_TASK_HEARTBEAT)

            if task_heartbeat > heartbeat:
                heartbeat = task_heartbeat

        return heartbeat

    def requirements(self, context):
        """Find all the requirements from the list of tasks and return it.

        If a task does not use the `task.decorate`, no assumptions can be made
        on which values from the context will be used, and it will raise an
        exception.

        Raise:
            NoRequirementFound: The exception when no requirements have been
                mentioned in at least one or more tasks.

        Return:
            set: the list of the required values from the context.
        """

        requirements = []

        # Get all the tasks and the lists (so the .fill on lists are also
        # considered.)
        all_tasks = list(self.tasks) + list(flatten(self.tasks, context))
        for task in all_tasks:
            task_details = getattr(task, '__garcon__', None)
            if task_details:
                requirements += task_details.get('requirements', [])
            else:
                raise NoRunnerRequirementsFound()
        return set(requirements)

    def execute(self, activity, context):
        """Execution of the tasks.
        """

        raise NotImplementedError


class Sync(BaseRunner):

    def execute(self, activity, context):
        result = dict()
        for task in flatten(self.tasks, context):
            activity.heartbeat()
            task_context = dict(list(result.items()) + list(context.items()))
            resp = task(task_context, activity=activity)
            result.update(resp or dict())
        return result


class Async(BaseRunner):

    def __init__(self, *args, **kwargs):
        self.tasks = args
        self.max_workers = kwargs.get('max_workers', 3)

    def execute(self, activity, context):
        result = dict()
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            tasks = []
            for task in flatten(self.tasks, context):
                tasks.append(executor.submit(task, context, activity=activity))

            for future in futures.as_completed(tasks):
                activity.heartbeat()
                data = future.result()
                result.update(data or {})
        return result


class External(BaseRunner):

    def __init__(self, timeout=None, heartbeat=None):
        """Create the External Runner.

        Args:
            timeout (int): activity timeout in seconds (mandatory)
            heartbeat (int): heartbeat timeout in seconds, if not defined, it
                will be equal to the timeout.
        """

        assert timeout, 'External runner requires a timeout.'

        self.timeout = lambda ctx=None: timeout
        self.heartbeat = lambda ctx=None: (heartbeat or timeout)
