from __future__ import print_function
import boto.swf.layer2 as swf

from garcon import activity
from garcon import task


domain = 'dev'
create = activity.create(domain)

activity_1 = create(
    name='activity_1',
    tasks=task.SyncTasks(
        lambda: print('activity_1')))

activity_2 = create(
    name='activity_2',
    requires=[activity_1],
    tasks=task.AsyncTasks(
        lambda: print('activity_2_task_1'),
        lambda: print('activity_2_task_2')))

activity_3 = create(
    name='activity_3',
    requires=[activity_1],
    tasks=task.SyncTasks(
        lambda: print('activity_3')))

activity_4 = create(
    name='activity_4',
    requires=[activity_3, activity_2],
    tasks=task.SyncTasks(
        lambda: print('activity_4')))
