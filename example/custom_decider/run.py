from garcon import activity
from garcon import decider
from threading import Thread
import boto.swf.layer2 as swf
import time

import workflow

# Initiate the workflow on the dev domain and custom_decider name.
flow = workflow.Workflow('dev', 'custom_decider')
deciderworker = decider.DeciderWorker(flow)

# swf.WorkflowType(
#     name=flow.name, domain=flow.domain,
#     version='1.0', task_list=flow.name).start()

Thread(target=activity.ActivityWorker(flow).run).start()
while(True):
    deciderworker.run()
    time.sleep(1)
