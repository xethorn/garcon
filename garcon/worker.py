from boto.swf.exceptions import SWFTypeAlreadyExistsError, SWFDomainAlreadyExistsError
from threading import Thread
from time import sleep
import boto.swf.layer2 as swf
import time

from garcon import activity
from garcon import event


class DeciderWorker(swf.Decider):

    def __init__(self, flow, activities):
        self.flow = flow
        self.domain = flow.domain
        self.task_list = flow.domain + '_decider'
        self.version = '1.0'
        self.activities = activity.find_activities(flow)

        super().__init__()
        self.populate_swf()

    def get_history(self, pool):
        """Get all the events.

        If there are more than 100 events, we need to capture all the remaining
        events (supposing we are in the case where we have more than 1 worker
        for a decision.)
        """

        events = pool['events']
        while 'nextPageToken' in pool:
            pool = self.poll(next_page_token=pool['nextPageToken'])

            if 'events' in pool:
                events += pool['events']

        # Remove all the events that are related to decisions and only.
        events = [
            e for e in events if not e['eventType'].startswith('Decision')]

        events = event.prepare_events(events)
        return events


    def populate_swf(self):
        """Populate SWF with the necessary data.
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



class ActivityWorker():

    def __init__(self, flow, activities=None):
        self.flow = flow
        self.activities = activity.find_activities(self.flow)
        self.worker_activities = activities

    def run(self):
        for activity in self.activities:
            if (self.worker_activities and
                    not activity.name in self.worker_activities):
                continue

            Thread(target=worker_runner, args=(activity,)).start()


def worker_runner(worker):
    while(True):
        worker.run()
