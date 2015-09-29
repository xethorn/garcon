# -*- coding: utf-8 -*-
"""
Activity
========

Activities are self generated classes to which you can pass an identifier,
and a list of tasks to perform. The activities are in between the decider and
the tasks.

For ease, two types of task runners are available: Sync and Async. If
you need something more specific, you should either create your own runner, or
you should create a main task that will then split the work.

Create an activity::

    from garcon import activity

    # First step is to create the workflow on a specific domain.
    create = activity.create('domain')

    initial_activity = create(
        # Name of your activity
        name='activity_name',

        # List of tasks to run (here we use the Sync runner)
        run=runner.Sync(task1),

        # No requires since it's the first one. Later in your flow, if you have
        # a dependency, just use the variable that contains the activity.
        requires=[],

        # If the activity fails, number of times you want to retry.
        retry=0,

        # If you want to run the activity `n` times, you can use a generator.
        generator=[generator_name])

"""

from threading import Thread
import boto.swf.layer2 as swf
import itertools
import json

from garcon import log
from garcon import utils
from garcon import runner


ACTIVITY_STANDBY = 0
ACTIVITY_SCHEDULED = 1
ACTIVITY_COMPLETED = 2
ACTIVITY_FAILED = 3

DEFAULT_ACTIVITY_SCHEDULE_TO_START = 600  # 10 minutes


class ActivityInstanceNotReadyException(Exception):
    """Exception when an activity instance is not ready.

    Activity instances that are considered not ready are instances that have
    not completed.
    """

    pass


class ActivityInstance:

    def __init__(
            self, activity_worker, local_context=None, execution_context=None):
        """Activity Instance.

        In SWF, Activity is a worker: it will get information from the context,
        and will launch activity instances (only one, unless you have a
        generator.) The activity instance generates its key (visible in the SWF
        console) from the local context. Activity instances are owned by an
        execution.

        Args:
            activity_worker (ActivityWorker): The activity worker that owns
                this specific Activity Instance.
            local_context (dict): the local context of the activity (it does
                not include the execution context.) Most times the context will
                be empty since it is only filled with data that comes from the
                generators.
            execution_context (dict): the execution context of when an activity
                will be scheduled with.
        """

        self.activity_worker = activity_worker
        self.execution_context = execution_context or dict()
        self.local_context = local_context or dict()
        self.global_context = dict(
            list(self.execution_context.items()) +
            list(self.local_context.items()))

    @property
    def activity_name(self):
        """Return the activity name of the worker.
        """

        return self.activity_worker.name

    @property
    def retry(self):
        """Return the number of retries allowed (matches the worker.)
        """

        return self.activity_worker.retry

    @property
    def id(self):
        """Generate the id of the activity.

        The id is crutial (not just important): it allows to indentify the
        state the activity instance in the event history (if it has failed,
        been executed, or marked as completed.)

        Return:
            str: composed of the activity name (task list), and the activity
                id.
        """

        if not self.local_context:
            activity_id = 1
        else:
            activity_id = utils.create_dictionary_key(self.local_context)

        return '{name}-{id}'.format(
            name=self.activity_name,
            id=activity_id)

    @property
    def schedule_to_start(self):
        """Return the schedule to start timeout.

        The schedule to start timeout assumes that only one activity worker is
        available (since swf does not provide a count of available workers). So
        if the default value is 5 minutes, and you have 10 instances: the
        schedule to start will be 50 minutes for all instances.

        Return:
            int: Schedule to start timeout.
        """

        return (
            self.activity_worker.pool_size *
            self.activity_worker.schedule_to_start_timeout)

    @property
    def schedule_to_close(self):
        """Return the schedule to close timeout.

        The schedule to close timeout is a simple calculation that defines when
        an activity (from the moment it has been scheduled) should end. It is
        a calculation between the schedule to start timeout and the activity
        timeout.

        Return:
            int: Schedule to close timeout.
        """

        return self.schedule_to_start + self.timeout

    @property
    def timeout(self):
        """Return the timeout in seconds.

        This timeout corresponds on when the activity has started and when we
        assume the activity has ended (which corresponds in boto to
        start_to_close_timeout.)

        Return:
            int: Task list timeout.
        """

        return self.runner.timeout(self.global_context)

    @property
    def heartbeat_timeout(self):
        """Return the heartbeat in seconds.

        This heartbeat corresponds on when an activity needs to send a signal
        to swf that it is still running. This will set the value when the
        activity is scheduled.

        Return:
            int: Task list timeout.
        """

        return self.runner.heartbeat(self.global_context)

    @property
    def runner(self):
        """Shortcut to get access to the runner.

        Raises:
            runner.RunnerMissing: an activity should always have a runner,
                if the runner is missing an exception is raised (we will not
                be able to calculate values such as timeouts without a runner.)

        Return:
            Runner: the activity runner.
        """

        activity_runner = getattr(self.activity_worker, 'runner', None)
        if not activity_runner:
            raise runner.RunnerMissing()
        return activity_runner

    def create_execution_input(self):
        """Create the input of the activity from the context.

        AWS has a limit on the number of characters that can be used (32k). If
        you use the `task.decorate`, the data sent to the activity is optimized
        to match the values of the context as well as the execution context.

        Return:
            dict: the input to send to the activity.
        """

        activity_input = dict()

        try:
            for requirement in self.runner.requirements(self.global_context):
                value = self.global_context.get(requirement)
                if value:
                    activity_input.update({requirement: value})

            activity_input.update(self.execution_context.items())

        except runner.NoRunnerRequirementsFound:
            return self.global_context
        return activity_input


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
                    self.fail(reason=str(error)[:255])
                    if self.on_exception:
                        self.on_exception(self, error)
                except:
                    pass

        self.unset_log_context()
        return True

    def execute_activity(self, context):
        """Execute the runner.

        Args:
            context (dict): The flow context.
        """

        return self.runner.execute(self, context)

    def hydrate(self, data):
        """Hydrate the task with information provided.

        Args:
            data (dict): the data to use (if defined.)
        """

        self.pool_size = 0
        self.version = data.get('version') or self.version
        self.name = self.name or data.get('name')
        self.domain = getattr(self, 'domain', '') or data.get('domain')
        self.requires = getattr(self, 'requires', []) or data.get('requires')
        self.retry = getattr(self, 'retry', None) or data.get('retry', 0)
        self.task_list = self.task_list or data.get('task_list')
        self.on_exception = (
            getattr(self, 'on_exception', None) or data.get('on_exception'))

        # The start timeout is how long it will take between the scheduling
        # of the activity and the start of the activity.
        self.schedule_to_start_timeout = (
            getattr(self, 'schedule_to_start_timeout', None) or
            data.get('schedule_to_start') or
            DEFAULT_ACTIVITY_SCHEDULE_TO_START)

        # The previous way to create an activity was to fill a `tasks` param,
        # which is not `run`.
        self.runner = (
            getattr(self, 'runner', None) or
            data.get('run') or data.get('tasks'))

        self.generators = getattr(
            self, 'generators', None) or data.get('generators')

    def instances(self, context):
        """Get all instances for one activity based on the current context.

        There are two scenarios: when the activity worker has a generator and
        when it does not. When it doesn't (the most simple case), there will
        always be one instance returned.

        Generators will however consume the context to calculate how many
        instances of the activity are needed – and it will generate them
        (regardless of their state.)

        Args:
            context (dict): the current context.
        Return:
            list: all the instances of the activity (for a current workflow
                execution.)
        """

        if not self.generators:
            self.pool_size = 1
            yield ActivityInstance(self, execution_context=context)
            return

        generator_values = []
        for generator in self.generators:
            generator_values.append(generator(context))

        contexts = list(itertools.product(*generator_values))
        self.pool_size = len(contexts)
        for generator_contexts in contexts:
            # Each generator returns a context, merge all the contexts
            # to only be one - which can be used to 1/ create the id of the
            # activity and 2/ be passed as a local context.
            instance_context = dict()
            for current_generator_context in generator_contexts:
                instance_context.update(current_generator_context.items())

            yield ActivityInstance(
                self, execution_context=context,
                local_context=instance_context)


class ExternalActivity(Activity):
    """External activity

    One of the main advantages of SWF is the ability to write a workflow that
    has activities written in any languages. The external activity class allows
    to write the workflow in Garcon and benefit from some features (timeout
    calculation among other things, sending context data.)
    """

    def __init__(self, timeout=None, heartbeat=None):
        """Create the External Activity.

        Args:
            timeout (int): activity timeout in seconds (mandatory)
            heartbeat (int): heartbeat timeout in seconds, if not defined, it
                will be equal to the timeout.
        """

        self.runner = runner.External(timeout=timeout, heartbeat=heartbeat)

    def run(self):
        """Run the external activity.

        This activity is handled outside, so the run method should remain
        unimplemented and return False (so the run loop stops.)
        """

        return False


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
        self.activities = find_workflow_activities(self.flow)
        self.worker_activities = activities

    def run(self):
        """Run the activities.
        """

        for activity in self.activities:
            if (self.worker_activities and
                    activity.name not in self.worker_activities):
                continue
            Thread(target=worker_runner, args=(activity,)).start()


class ActivityState:
    """
    Activity State
    ==============

    Provides information about a specific activity instance state (if the
    instance is already scheduled, has failed, or has been completed.) Along
    with the default values, this class also provides additional metadata such
    as the result of an activity instance.
    """

    def __init__(self, activity_id):
        """Create a State.

        Args:
            activity_id (str): the activity id.
        """

        self.activity_id = activity_id
        self._result = None
        self.states = []

    @property
    def result(self):
        """Get the result.
        """

        if not self.ready:
            raise ActivityInstanceNotReadyException()
        return self._result

    @property
    def ready(self):
        """Check if an activity is ready.
        """

        return self.get_last_state() == ACTIVITY_COMPLETED

    def get_last_state(self):
        """Get the last state of the activity execution.

        Return:
            int: the state of the activity (see: activity.py)
        """

        if len(self.states):
            return self.states[-1]
        return None

    def add_state(self, state):
        """Add a state in the activity execution.

        Args:
            state (int): the state of the activity to add (see activity.py)
        """

        self.states.append(state)

    def set_result(self, result):
        """Set the result of the activity.

        This method sometimes throws an exception: an activity id can only have
        one result.

        Args:
            result (dict): Result of the activity.
        """

        if self._result:
            raise Exception('Result is ummutable – it should not be changed.')
        self._result = result

    def wait():
        """Wait until ready.
        """

        if not self.ready():
            raise ActivityInstanceNotReadyException()


def worker_runner(worker):
    """Run indefinitely the worker.

    Args:
        worker (object): the Activity worker.
    """

    while(worker.run()):
        continue


def create(domain, name, version='1.0', on_exception=None):
    """Helper method to create Activities.

    The helper method simplifies the creation of an activity by setting the
    domain, the task list, and the activity dependencies (what other
    activities) need to be completed before this one can run.

    Note:
        The task list is generated based on the domain and the name of the
        activity. Always make sure your activity name is unique.

    Args:
        domain (str): the domain name.
        name (str): name of the activity.
        version (str): activity version.
        on_exception (callable): the error handler.

    Return:
        callable: activity generator.
    """

    def wrapper(**options):
        activity = Activity()

        if options.get('external'):
            activity = ExternalActivity(
                timeout=options.get('timeout'),
                heartbeat=options.get('heartbeat'))

        activity_name = '{name}_{activity}'.format(
            name=name,
            activity=options.get('name'))

        activity.hydrate(dict(
            domain=domain,
            version=version,
            name=activity_name,
            generators=options.get('generators', []),
            requires=options.get('requires', []),
            retry=options.get('retry'),
            task_list=activity_name,
            tasks=options.get('tasks'),
            run=options.get('run'),
            schedule_to_start=options.get('schedule_to_start'),
            on_exception=options.get('on_exception') or on_exception))
        return activity
    return wrapper


def find_available_activities(flow, history, context):
    """Find all available activity instances of a flow.

    The history contains all the information of our activities (their state).
    This method focuses on finding all the activities that need to run.

    Args:
        flow (module): the flow module.
        history (dict): the history information.
        context (dict): from the context find the available activities.
    """

    for instance in find_activities(flow, context):
        # If an event is already available for the activity, it means it is
        # not in standby anymore, it's either processing or has been completed.
        # The activity is thus not available anymore.
        states = history.get(instance.activity_name, {}).get(instance.id)

        if states:
            if states.get_last_state() != ACTIVITY_FAILED:
                continue
            elif (not instance.retry or
                    instance.retry < count_activity_failures(states)):
                raise Exception(
                    'The activity failures has exceeded its retry limit.')

        can_yield = True
        for requirement in instance.activity_worker.requires:
            require_history = history.get(requirement.name)

            if not require_history:
                can_yield = False
                break

            for requirement_states in require_history.values():
                if ACTIVITY_COMPLETED not in requirement_states.states:
                    can_yield = False
                    break

        if can_yield:
            yield instance


def find_uncomplete_activities(flow, history, context):
    """Find uncomplete activity instances.

    Uncomplete activities are all the activities that are not marked as
    completed.

    Args:
        flow (module): the flow module.
        history (dict): the history information.
        context (dict): from the context find the available activities.
    Yield:
        activity: The available activity.
    """

    for instance in find_activities(flow, context):
        states = history.get(instance.activity_name, {}).get(instance.id)
        if not states or ACTIVITY_COMPLETED not in states.states:
            yield instance


def find_workflow_activities(flow):
    """Retrieves all the activities from a flow

    Args:
        flow (module): the flow module.
    Return:
        list: all the activities.
    """

    activities = []
    for module_attribute in dir(flow):
        current_activity = getattr(flow, module_attribute)
        if isinstance(current_activity, Activity):
            activities.append(current_activity)
    return activities


def find_activities(flow, context):
    """Retrieves all the activities from a flow.

    Args:
        flow (module): the flow module.
    Return:
        list: All the activity instances for the flow.
    """

    activities = []
    for module_attribute in dir(flow):
        current_activity = getattr(flow, module_attribute)

        if isinstance(current_activity, Activity):
            for activity_instance in current_activity.instances(context):
                activities.append(activity_instance)

    return activities


def count_activity_failures(states):
    """Count the number of times an activity has failed.

    Args:
        states (dict): list of activity states.
    Return:
        int: The number of times an activity has failed.
    """

    return len([evt for evt in states.states if evt == ACTIVITY_FAILED])
