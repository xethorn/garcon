"""
Decider Worker
===============

The decider worker is focused on orchestrating which activity needs to be
executed and when based on the flow procided.
"""

from boto.swf.exceptions import SWFTypeAlreadyExistsError
from boto.swf.exceptions import SWFDomainAlreadyExistsError
from threading import Thread
import boto.swf.layer2 as swf
import time

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
        self.task_list = flow.domain + '_decider'
        self.version = '1.0'
        self.activities = activity.find_activities(flow)

        super().__init__()

        if register:
            self.register()

    def get_history(self, pool):
        """Get all the history.

        The full history needs to be recovered from SWF to make sure that all
        the activities have been properly scheduled. With boto, only the last
        100 events are provided, this methods retrieves all events.

        Args:
            pool (object): The pool object (see AWS SWF for details.)
        Return:
            dict: list of all the activities and their state. It only contains
                activities that have been scheduled with AWS.
        """

        events = pool['events']
        while 'nextPageToken' in pool:
            pool = self.poll(next_page_token=pool['nextPageToken'])

            if 'events' in pool:
                events += pool['events']

        # Remove all the events that are related to decisions and only.
        events = [
            e for e in events if not e['eventType'].startswith('Decision')]

        return event.activity_states_from_events(events)


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

        for activity in self.activities:
            registerables.append(
                swf.ActivityType(
                    domain=self.domain,
                    name=activity.name,
                    version=self.version,
                    task_list=activity.task_list))

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
        """

        pool = self.poll()

        if not 'events' in pool:
            return

        history = self.get_history(pool)
        decisions = swf.Layer1Decisions()

        for current in activity.find_available_activities(self.flow, history):
            decisions.schedule_activity_task(
                '%s-%i' % (current.name, time.time()),
                current.name,
                self.version,
                task_list=current.task_list,
                input='json object')
        else:
            activities = list(
                activity.find_uncomplete_activities(self.flow, history))
            if not activities:
                decisions.complete_workflow_execution()

        self.complete(decisions=decisions)
        return True
