# -*- coding: utf-8 -*-
"""
Context
=======

Context carries information that have been retrieved from the different SWF
events of an execution.
"""

import json


class ExecutionContext:

    def __init__(self, events=None):
        """Create the execution context.

        An execution context gathers the execution input and the result of all
        the activities that have successfully ran. It also adds the execution
        input into the mix (for logger purposes).

        Args:
            events (list): optional list of all the events.
        """

        self.current = {}
        self.workflow_input = {}

        if events:
            for event in events:
                self.add(event)

    def add(self, event):
        """Add an event into the execution context.

        The events are the ones coming from SWF directly (so the fields are the
        ones we expect).

        Args:
            event (dict): the event to add to the context.
        """

        event_type = event.get('eventType')
        if event_type == 'ActivityTaskCompleted':
            self.add_activity_result(event)
        elif event_type == 'WorkflowExecutionStarted':
            self.set_execution_input(event)

    def set_workflow_execution_info(self, execution_info, domain):
        """Add the workflow execution info.

        Workflow execution info contains the domain, workflow id and run id.
        This allows the logger to properly namespace the messages and
        facilitate debugging.

        Args:
            execution_info (dict): the execution information.
            domain (str): the current domain
        """

        if ('workflowExecution' in execution_info and
                'workflowId' in execution_info['workflowExecution'] and
                'runId' in execution_info['workflowExecution']):

            workflow_execution = execution_info['workflowExecution']
            self.current.update({
                'execution.domain': domain,
                'execution.workflow_id': workflow_execution['workflowId'],
                'execution.run_id': workflow_execution['runId']
            })

    def set_execution_input(self, execution_event):
        """Add the workflow execution input.

        Please note the input within the execution event should always be a
        json string.

        Args:
            execution_event (str): the execution event information.
        """

        attributes = execution_event['workflowExecutionStartedEventAttributes']
        result = attributes.get('input')
        if result:
            result = json.loads(result)
            self.workflow_input = result
            self.current.update(result)

    def add_activity_result(self, activity_event):
        """Add an activity result.

        Please note: the result of an activity event should always be a json
        string.

        Args:
            activity_event (str): json object that represents the activity
                information.
        """

        attributes = activity_event['activityTaskCompletedEventAttributes']
        result = attributes.get('result')

        if result:
            self.current.update(json.loads(result))
