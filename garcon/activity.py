# -*- coding: utf-8 -*-
"""
Activity
========

Activities are self generated classes to which you can pass an identifier,
and a list of tasks to perform. The activities are in between the decider and
the task.

For ease, two types of task runners are available: Sync and Async. If
you need something more specific, you should either create your own runner, or
you should create a main task that will then split the work.
"""

from threading import Thread
import boto.swf.layer2 as swf
import json

from garcon import log

ACTIVITY_STANDBY = 0
ACTIVITY_SCHEDULED = 1
ACTIVITY_COMPLETED = 2
ACTIVITY_FAILED = 3


class Activity(swf.ActivityWorker, log.GarconLogger):
    version = '1.0'
    task_list = None

    def run(self):
        """Activity Runner.

        Information is being pulled down from SWF and it checks if the Activity
        can be ran. As part of the information provided, the input of the
        previous activity is consumed (context).
        """

        activity_task = self.poll()
        packed_context = activity_task.get('input')
        context = dict()

        if packed_context:
            context = json.loads(packed_context)
            self.set_log_context(context)

        if 'activityId' in activity_task:
            try:
                context = self.execute_activity(context)
                self.complete(result=json.dumps(context))
            except Exception as error:
                # If the workflow has been stopped, it is not possible for the
                # activity to be updated – it throws an exception which stops
                # the worker immediately.
                try:
                    self.fail(reason=str(error))
                except:
                    pass

        self.unset_log_context()
        return True

    def execute_activity(self, context):
        """Execute the tasks within the activity.

        Args:
            context (dict): The flow context.
        """

        return self.tasks.execute(self, context)

    def hydrate(self, data):
        """Hydrate the task with information provided.

        Args:
            data (dict): the data to use (if defined.)
        """

        self.name = self.name or data.get('name')
        self.domain = getattr(self, 'domain', '') or data.get('domain')
        self.requires = getattr(self, 'requires', []) or data.get('requires')
        self.retry = getattr(self, 'retry', None) or data.get('retry', 0)
        self.task_list = self.task_list or data.get('task_list')
        self.tasks = getattr(self, 'tasks', []) or data.get('tasks')

    @property
    def timeout(self):
        """Return the timeout in seconds.

        This timeout corresponds on when the activity has started and when we
        assume the activity has ended (which corresponds in boto to
        start_to_close_timeout.)

        Return:
            int: Task list timeout.
        """

        return self.tasks.timeout


class ActivityWorker():

    def __init__(self, flow, activities=None):
        """Initiate an activity worker.

        The activity worker take in consideration all the activities from a
        flow, or specific activities. Some activities (tasks) might require
        more power than others, and be then launched on different machines.

        If a list of activities is passed, the worker will be focused on
        completing those and will ignore all the others.

        Args:
            flow (module): the flow module.
            activities (list): the list of activities that this worker should
                handle.
        """

        self.flow = flow
        self.activities = find_activities(self.flow)
        self.worker_activities = activities

    def run(self):
        """Run the activities.
        """

        for activity in self.activities:
            if (self.worker_activities and
                    not activity.name in self.worker_activities):
                continue
            Thread(target=worker_runner, args=(activity,)).start()


def worker_runner(worker):
    """Run indefinitely the worker.

    Args:
        worker (object): the Activity worker.
    """

    while(worker.run()):
        continue


def create(domain):
    """Helper method to create Activities.

    The helper method simplifies the creation of an activity by setting the
    domain, the task list, and the activity dependencies (what other
    activities) need to be completed before this one can run.

    Note:
        The task list is generated based on the domain and the name of the
        activity. Always make sure your activity name is unique.
    """

    def wrapper(**options):
        activity = Activity()
        activity.hydrate(dict(
            domain=domain,
            name=options.get('name'),
            requires=options.get('requires', []),
            retry=options.get('retry'),
            task_list=domain + '_' + options.get('name'),
            tasks=options.get('tasks', [])
        ))
        return activity
    return wrapper


def find_available_activities(flow, history):
    """Find all available activities of a flow.

    The history contains all the information of our activities (their state).
    This method focuses on finding all the activities that need to run.

    Args:
        flow (module): the flow module.
        history (dict): the history information.
    """

    for activity in find_activities(flow):
        # If an event is already available for the activity, it means it is
        # not in standby anymore, it's either processing or has been completed.
        # The activity is thus not available anymore.
        event = history.get(activity.name)

        if event:
            if event[-1] != ACTIVITY_FAILED:
                continue
            elif (not activity.retry or
                    activity.retry < count_activity_failures(event)):
                raise Exception(
                    'The activity failures has exceeded its retry limit.')

        add = True
        for requirement in activity.requires:
            requirement_evt = history.get(requirement.name) or []
            if not ACTIVITY_COMPLETED in requirement_evt:
                add = False
                break

        if add:
            yield activity


def find_uncomplete_activities(flow, history):
    """Find uncomplete activities.

    Uncomplete activities are all the activities that are not marked as
    completed.

    Args:
        flow (module): the flow module.
        history (dict): the history information.
    Yield:
        activity: The available activity.
    """

    for activity in find_activities(flow):
        evts = history.get(activity.name)
        if not evts or ACTIVITY_COMPLETED not in evts:
            yield activity


def find_activities(flow):
    """Retrieves all the activities from a flow.

    Args:
        flow (module): the flow module.
    Return:
        List of all the activities for the flow.
    """

    activities = []
    for module_attribute in dir(flow):
        instance = getattr(flow, module_attribute)
        if isinstance(instance, Activity):
            activities.append(instance)
    return activities


def count_activity_failures(events):
    """Count the number of times an activity has failed.

    Args:
        events (dict): list of activity events.
    Return:
        int: The number of times an activity has failed.
    """

    return len([evt for evt in events if evt == ACTIVITY_FAILED])
