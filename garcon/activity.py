"""
Activity
========

Activities are self generated classes to which you can pass an identifier,
and a list of tasks to perform. The activities are in between the decider and
the task.

For ease, two types of task runners are available: SyncTasks and AsyncTasks. If
you need something more specific, you should either create your own runner, or
you should create a main task that will then split the work.
"""

import boto.swf.layer2 as swf

ACTIVITY_STANDBY = 0
ACTIVITY_SCHEDULED = 1
ACTIVITY_COMPLETED = 2


class Activity(swf.ActivityWorker):
    version = '1.0'
    task_list = None

    def run(self):
        """Activity Runner.

        Information is being pulled down from SWF and it checks if the Activity
        can be ran. As part of the information provided, the input of the
        previous activity is consumed (context).
        """

        activity_task = self.poll()
        context = activity_task.get('input', {})

        if 'activityId' in activity_task:
            try:
                self.execute_activity(context)
                self.complete()
            except Exception as error:
                self.fail(reason=str(error))
                raise error
        return True

    def execute_activity(self, context):
        """Execute the tasks within the activity.

        Args:
            context (dict): The flow context.
        """

        return self.tasks.execute(context)

    def hydrate(self, data):
        """Hydrate the task with information provided.
        """

        self.domain = self.name or data.get('domain')
        self.name = self.name or data.get('name')
        self.requires = getattr(self, 'requires', []) or data.get('requires')
        self.task_list = self.task_list or data.get('task_list')
        self.tasks = getattr(self, 'tasks', []) or data.get('tasks')


def create(domain):
    """Helper method to create Activities.

    The helper method simplifies the creation of an activity by setting the
    domain, the task list, and the activity dependencies (what other
    activities) need to be completed before this one can run.
    """

    def wrapper(**options):
        activity = Activity()
        activity.hydrate(dict(
            domain=domain,
            name=options.get('name'),
            requires=options.get('requires', []),
            task_list=domain + '_' + options.get('name'),
            tasks=options.get('tasks', []),
        ))
        return activity
    return wrapper


def find_available_activities(flow, history):
    """Hydrate the flow activities from an event.
    """

    for activity in find_activities(flow):
        # If an event is already available for the activity, it means it is
        # not in standby anymore, it's either processing or has been completed.
        # The activity is thus not available anymore.
        event = history.get(activity.name)
        pprint.pprint(event)

        if event:
            continue

        add = True
        for requirement in activity.requires:
            requirement_evt = history.get(requirement.name)
            if not requirement_evt == ACTIVITY_COMPLETED:
                add = False
                break

        if add:
            yield activity


def find_uncomplete_activities(flow, history):
    """Find uncomplete activities.
    """

    for activity in find_activities(flow):
        event = history.get(activity.name)
        if not event or event != ACTIVITY_COMPLETED:
            yield activity


def find_activities(flow):
    """Find activities in a flow.
    """

    activities = []
    for module_attribute in dir(flow):
        instance = getattr(flow, module_attribute)
        if isinstance(instance, Activity):
            activities.append(instance)
    return activities
