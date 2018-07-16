from garcon import activity
from garcon import decider
from threading import Thread
import boto3
import time

import workflow

client = boto3.client('swf', region_name='us-east-1')
deciderworker = decider.DeciderWorker(client, workflow)

client.start_workflow_execution(
    domain=workflow.domain,
    workflowId='unique-workflow-identifier',
    workflowType=dict(
        name=workflow.name,
        version='1.0'),
    taskList=dict(name=workflow.name))

Thread(target=activity.ActivityWorker(client, workflow).run).start()
while(True):
    deciderworker.run()
    time.sleep(1)
