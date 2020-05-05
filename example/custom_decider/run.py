from garcon import activity
from garcon import decider
from threading import Thread
import time

import boto3
import workflow

# Initiate the workflow on the dev domain and custom_decider name.
client = boto3.client('swf', region_name='us-east-1')
workflow = workflow.Workflow(client, 'dev', 'custom_decider')
deciderworker = decider.DeciderWorker(workflow)

client.start_workflow_execution(
    domain=workflow.domain,
    workflowId='unique-workflow-identifier',
    workflowType=dict(
        name=workflow.name,
        version='1.0'),
    executionStartToCloseTimeout='3600',
    taskStartToCloseTimeout='3600',
    childPolicy='TERMINATE',
    taskList=dict(name=workflow.name))

Thread(target=activity.ActivityWorker(workflow).run).start()
while(True):
    deciderworker.run()
    time.sleep(1)
