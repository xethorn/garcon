# -*- coding: utf-8 -*-
"""
Decider Worker
===============

The decider worker is focused on orchestrating which activity needs to be
executed and when based on the flow procided.
"""

from boto.swf.exceptions import SWFDomainAlreadyExistsError
from boto.swf.exceptions import SWFTypeAlreadyExistsError
import boto.swf.layer2 as swf
import functools
import json

from garcon import activity
from garcon import event


class DeciderWorker(swf.Decider):

    def __init__(self, flow, register=True):
        """Initialize the Decider Worker.

        Args:
            flow (module): Flow module.
            register (boolean): If this flow needs to be register on AWS.
        """

        self.flow = flow
        self.domain = flow.domain
        self.version = getattr(flow, 'version', '1.0')
        self.activities = activity.find_workflow_activities(flow)
        self.task_list = flow.name
        self.on_exception = getattr(flow, 'on_exception', None)
        super(DeciderWorker, self).__init__()

        if register:
            self.register()

    def get_history(self, poll):
        """Get all the history.

        The full history needs to be recovered from SWF to make sure that all
        the activities have been properly scheduled. With boto, only the last
        100 events are provided, this methods retrieves all events.

        Args:
            poll (object): The poll object (see AWS SWF for details.)
        Return:
            list: All the events.
        """

        events = poll['events']
        while 'nextPageToken' in poll:
            poll = self.poll(next_page_token=poll['nextPageToken'])

            if 'events' in poll:
                events += poll['events']

        # Remove all the events that are related to decisions and only.
        return [e for e in events if not e['eventType'].startswith('Decision')]

    def get_workflow_execution_info(self, poll):
        """Get the workflow execution info from a given poll if it exists.

        Args:
            poll (object): The poll object (see AWS SWF for details.)
        Return:
            `dict`: Workflow execution info including domain, workflowId and
                runId.
        """

        execution_info = None
        if ('workflowExecution' in poll and 'workflowId' in
                poll['workflowExecution'] and 'runId' in
                poll['workflowExecution']):

            workflow_execution = poll['workflowExecution']
            execution_info = {
                'execution.domain': self.domain,
                'execution.workflow_id': workflow_execution['workflowId'],
                'execution.run_id': workflow_execution['runId']
            }

        return execution_info

    def get_activity_states(self, history):
        """Get the activity states from the history.

        From the full history extract the different activity states. Those
        states contain

        Args:
            history (list): the full history.
        Return:
            dict: list of all the activities and their state. It only contains
                activities that have been scheduled with AWS.
        """

        return event.activity_states_from_events(history)

    def register(self):
        """Register the Workflow on SWF.

        To work, SWF needs to have pre-registered the domain, the workflow,
        and the different activities, this method takes care of this part.
        """

        registerables = []
        registerables.append(swf.Domain(name=self.domain))
        registerables.append(swf.WorkflowType(
            domain=self.domain,
            name=self.task_list,
            version=self.version,
            task_list=self.task_list))

        for current_activity in self.activities:
            registerables.append(
                swf.ActivityType(
                    domain=self.domain,
                    name=current_activity.name,
                    version=self.version,
                    task_list=current_activity.task_list))

        for swf_entity in registerables:
            try:
                swf_entity.register()
            except (SWFDomainAlreadyExistsError, SWFTypeAlreadyExistsError):
                print(
                    swf_entity.__class__.__name__, swf_entity.name,
                    'already exists')

    def create_decisions_from_flow(self, decisions, activity_states, context):
        """Create the decisions from the flow.

        Simple flows don't need a custom decider, since all the requirements
        can be provided at the activity level. Discovery of the next activity
        to schedule is thus very straightforward.

        Args:
            decisions (Layer1Decisions): the layer decision for swf.
            activity_states (dict): all the state activities.
            context (dict): the context of the activities.
        """

        try:
            for current in activity.find_available_activities(
                    self.flow, activity_states, context):

                schedule_activity_task(
                    decisions, current, version=self.version)
            else:
                activities = list(
                    activity.find_uncomplete_activities(
                        self.flow, activity_states, context))
                if not activities:
                    decisions.complete_workflow_execution()
        except Exception as e:
            decisions.fail_workflow_execution(reason=str(e))
            if self.on_exception:
                self.on_exception(self, e)

    def delegate_decisions(self, decisions, decider, history, context):
        """Delegate the decisions.

        For more complex flows (the ones that have, for instance, optional
        activities), you can write your own decider. The decider receives a
        method `schedule` which schedule the activity if not scheduled yet,
        and if scheduled, returns its result.

        Args:
            decisions (Layer1Decisions): the layer decision for swf.
            decider (callable): the decider (it needs to have schedule)
            history (dict): all the state activities and its history.
            context (dict): the context of the activities.
        """

        schedule_context = ScheduleContext()
        decider_schedule = functools.partial(
            schedule, decisions, schedule_context, history, context,
            version=self.version)

        try:
            decider(schedule=decider_schedule)

            # When no exceptions are raised and the method decider has returned
            # it means that there i nothing left to do in the current decider.
            if schedule_context.completed:
                decisions.complete_workflow_execution()
        except activity.ActivityInstanceNotReadyException:
            pass
        except Exception as e:
            decisions.fail_workflow_execution(reason=str(e))
            if self.on_exception:
                self.on_exception(self, e)

    def run(self):
        """Run the decider.

        The decider defines which task needs to be launched and when based on
        the list of events provided. It looks at the list of all the available
        activities, and launch the ones that:

          * are not been scheduled yet.
          * have all the dependencies resolved.

        If the decider is not able to find an uncompleted activity, the
        workflow can safely mark its execution as complete.

        Return:
            boolean: Always return true, so any loop on run can act as a long
                running process.
        """

        poll = self.poll()
        custom_decider = getattr(self.flow, 'decider', None)

        if 'events' not in poll:
            return True

        history = self.get_history(poll)
        activity_states = self.get_activity_states(history)
        workflow_execution_info = self.get_workflow_execution_info(poll)
        context = event.get_current_context(history)

        if workflow_execution_info is not None:
            context.update(workflow_execution_info)

        decisions = swf.Layer1Decisions()
        if not custom_decider:
            self.create_decisions_from_flow(
                decisions, activity_states, context)
        else:
            self.delegate_decisions(
                decisions, custom_decider, activity_states, context)
        self.complete(decisions=decisions)
        return True


class ScheduleContext:
    """
    Schedule Context
    ================

    The schedule context keeps track of all the current scheduling progress – 
    which allows to easy determinate if there are more decisions to be taken
    or if the execution can be closed.
    """

    def __init__(self):
        """Create a schedule context.
        """

        self.completed = True

    def mark_uncompleted(self):
        """Mark the scheduling as completed.

        When a scheduling is completed, it means all the activities have been
        properly scheduled and they have all completed.
        """

        self.completed = False


def schedule_activity_task(
        decisions, instance, version='1.0', id=None):
    """Schedule an activity task.

    Args:
        decisions (Layer1Decisions): the layer decision for swf.
        instance (ActivityInstance): the activity instance to schedule.
        version (str): the version of the activity instance.
        id (str): optional id of the activity instance.
    """

    decisions.schedule_activity_task(
        id or instance.id,
        instance.activity_name,
        version,
        task_list=instance.activity_worker.task_list,
        input=json.dumps(instance.create_execution_input()),
        heartbeat_timeout=str(instance.heartbeat_timeout),
        start_to_close_timeout=str(instance.timeout),
        schedule_to_start_timeout=str(instance.schedule_to_start),
        schedule_to_close_timeout=str(instance.schedule_to_close))


def schedule(
        decisions, schedule_context, history, context, schedule_id,
        current_activity, requires=None, input=None, version='1.0'):
    """Schedule an activity.

    Scheduling an activity requires all the requirements to be completed (all
    activities should be marked as completed). The scheduler also mixes the
    input with the full execution context to send the data to the activity.

    Args:
        decisions (Layer1Decisions): the layer decision for swf.
        schedule_context (dict): information about the schedule.
        history (dict): history of the execution.
        context (dict): context of the execution.
        schedule_id (str): the id of the activity to schedule.
        current_activity (Activity): the activity to run.
        requires (list): list of all requirements.
        input (dict): additional input for the context.

    Throws:
        ActivityInstanceNotReadyException: if one of the activity in the
            requirements is not ready.

    Return:
        State: the state of the schedule (contains the response).
    """

    ensure_requirements(requires)
    activity_completed = set()
    result = dict()

    instance_context = dict()
    instance_context.update(context or {})
    instance_context.update(input or {})

    for current in current_activity.instances(instance_context):
        current_id = '{}-{}'.format(current.id, schedule_id)
        states = history.get(current.activity_name, {}).get(current_id)

        if states:
            if states.get_last_state() == activity.ACTIVITY_COMPLETED:
                result.update(states.result or dict())
                activity_completed.add(True)
                continue

            activity_completed.add(False)
            schedule_context.mark_uncompleted()

            if states.get_last_state() != activity.ACTIVITY_FAILED:
                continue
            elif (not current.retry or
                    current.retry < count_activity_failures(states)):
                raise Exception(
                    'The activity failures has exceeded its retry limit.')

        activity_completed.add(False)
        schedule_context.mark_uncompleted()
        schedule_activity_task(
            decisions, current, id=current_id, version=version)

    state = activity.ActivityState(current_activity.name)
    state.add_state(activity.ACTIVITY_SCHEDULED)

    if len(activity_completed) == 1 and True in activity_completed:
        state.add_state(activity.ACTIVITY_COMPLETED)
        state.set_result(result)
    return state


def ensure_requirements(requires):
    """Ensure scheduling meets requirements.

    Verify the state of the requirements to make sure the activity can be
    scheduled.

    Args:
        requires (list): list of all requirements.

    Throws:
        ActivityInstanceNotReadyException: if one of the activity in the
            requirements is not ready.
    """

    requires = requires or []
    for require in requires:
        if (not require or
                require.get_last_state() != activity.ACTIVITY_COMPLETED):
            raise activity.ActivityInstanceNotReadyException()
