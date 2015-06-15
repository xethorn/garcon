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
        if 'workflowExecution' in poll and 'workflowId' in \
                poll['workflowExecution'] and  'runId' in \
                poll['workflowExecution']:

            workflow_execution =  poll['workflowExecution']
            execution_info = {
                'execution.domain' : self.domain,
                'execution.workflow_id' : workflow_execution['workflowId'],
                'execution.run_id' : workflow_execution['runId']
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

        if not 'events' in poll:
            return True

        history = self.get_history(poll)
        activity_states = self.get_activity_states(history)
        workflow_execution_info = self.get_workflow_execution_info(poll)
        context = event.get_current_context(history)

        if workflow_execution_info is not None:
            context.update(workflow_execution_info)

        decisions = swf.Layer1Decisions()

        try:
            for current in activity.find_available_activities(
                    self.flow, activity_states, context):

                decisions.schedule_activity_task(
                    current.id,  # activity id.
                    current.activity_name,
                    self.version,
                    task_list=current.activity_worker.task_list,
                    input=json.dumps(current.create_execution_input()),
                    heartbeat_timeout=str(current.heartbeat_timeout),
                    start_to_close_timeout=str(current.timeout),
                    schedule_to_start_timeout=str(current.schedule_to_start),
                    schedule_to_close_timeout=str(current.schedule_to_close))
            else:
                activities = list(
                    activity.find_uncomplete_activities(
                        self.flow, activity_states, context))
                if not activities:
                    decisions.complete_workflow_execution()
        except Exception as e:
            decisions.fail_workflow_execution(reason=str(e))

        self.complete(decisions=decisions)
        return True
