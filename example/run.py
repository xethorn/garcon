from garcon import activity
from garcon import decider
from threading import Thread
import boto.swf.layer2 as swf
import time

import test_flow

deciderworker = decider.DeciderWorker(test_flow)

swf.WorkflowType(
    name=test_flow.domain + '_decider', domain=test_flow.domain,
    version='1.0', task_list=test_flow.domain + '_decider').start()

Thread(target=activity.ActivityWorker(test_flow).run).start()
while(True):
    deciderworker.run()
    time.sleep(1)
