import boto.swf.layer2 as swf

from garcon import activity
from garcon import runner
import logging
import random


logger = logging.getLogger(__name__)

domain = 'dev'
name = 'workflow_sample'
create = activity.create(domain, name)


def activity_failure(context, activity):
    num = int(random.random() * 4)
    if num != 3:
        logger.warn('activity_3: fails')
        raise Exception('fails')

    print('activity_3: end')


test_activity_1 = create(
    name='o',
    run=runner.Sync(
        lambda context, activity: logger.debug('activity_1')))

test_activity_2 = create(
    name='activity_2',
    requires=[test_activity_1],
    run=runner.Async(
        lambda context, activity: logger.debug('activity_2_task_1'),
        lambda context, activity: logger.debug('activity_2_task_2')))

test_activity_3 = create(
    name='activity_3',
    retry=10,
    requires=[test_activity_1],
    run=runner.Sync(activity_failure))

test_activity_4 = create(
    name='activity_4',
    requires=[test_activity_3, test_activity_2],
    run=runner.Sync(
        lambda context, activity: logger.debug('activity_4')))
