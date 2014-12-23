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


class Tasks():

    def __init__(self, *args):
        self.tasks = args


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
        with ThreadPoolExecutor(max_workers=3) as executor:
            tasks = []
            for task in self.tasks:
                tasks.append(executor.submit(task, context))

            for future in futures.as_completed(tasks):
                data = future.result()
                result.update(data or {})
        return result
