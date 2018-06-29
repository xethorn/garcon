history = [
    {
        'eventId': 1,
        'eventTimestamp': 1530183236.437,
        'eventType': 'WorkflowExecutionStarted',
        'workflowExecutionStartedEventAttributes': {
            'childPolicy': 'TERMINATE',
            'executionStartToCloseTimeout': '3600',
            'parentInitiatedEventId': 0,
            'taskList': {'name': 'basic'},
            'taskStartToCloseTimeout': '300',
            'workflowType': {'name': 'basic', 'version': '1.0'}
        }
    },
    {
        'activityTaskScheduledEventAttributes':
            {
                'activityId': 'basic_activity_1-1',
                'activityType':
                    {
                        'name': 'basic_activity_1',
                        'version': '1.0'
                    },
                'decisionTaskCompletedEventId': 4,
                'heartbeatTimeout': '600',
                'input': '{"execution.domain": "MyTutorialDomain", "execution.workflow_id": "basic-1.0-1530183235", "execution.run_id": "22S4V/sVSQmLzQN1yk/4RVv6t3uLP5HOVd3qK1VYY9ZVo="}',
                'scheduleToCloseTimeout': '1200',
                'scheduleToStartTimeout': '600',
                'startToCloseTimeout': '600',
                'taskList': {'name': 'basic_activity_1'}
            },
        'eventId': 5,
        'eventTimestamp': 1530183244.205,
        'eventType': 'ActivityTaskScheduled'
    },
    {
        'activityTaskStartedEventAttributes':
            {
                'scheduledEventId': 5
            },
        'eventId': 6,
        'eventTimestamp': 1530183244.271,
        'eventType': 'ActivityTaskStarted'
    },
    {
        'activityTaskCompletedEventAttributes':
            {
                'result': '{"first_act_result": "file is downloaded"}',
                'scheduledEventId': 5, 'startedEventId': 6
            },
        'eventId': 7,
        'eventTimestamp': 1530183245.568,
        'eventType': 'ActivityTaskCompleted'
    },
    {
        'activityTaskScheduledEventAttributes':
            {
                'activityId': 'basic_activity_2-1',
                'activityType': {'name': 'basic_activity_2', 'version': '1.0'},
                'decisionTaskCompletedEventId': 10, 'heartbeatTimeout': '30',
                'input': '{"execution.domain": "MyTutorialDomain", "execution.run_id": "22S4V/sVSQmLzQN1yk/4RVv6t3uLP5HOVd3qK1VYY9ZVo=", "execution.workflow_id": "basic-1.0-1530183235"}',
                'scheduleToCloseTimeout': '630',
                'scheduleToStartTimeout': '600',
                'startToCloseTimeout': '30',
                'taskList': {'name': 'basic_activity_2'}
            },
        'eventId': 11,
        'eventTimestamp': 1530183247.589,
        'eventType': 'ActivityTaskScheduled'
    },
    {
        'activityTaskStartedEventAttributes': {'scheduledEventId': 11},
        'eventId': 12,
        'eventTimestamp': 1530183297.418,
        'eventType': 'ActivityTaskStarted'
    },
    {
        'activityTaskTimedOutEventAttributes':
            {
                'scheduledEventId': 11,
                'startedEventId': 12,
                'timeoutType': 'START_TO_CLOSE'
            },
        'eventId': 13,
        'eventTimestamp': 1530183327.421,
        'eventType': 'ActivityTaskTimedOut'
    }
]

time_out_event = {
    'activityTaskTimedOutEventAttributes':
        {
            'scheduledEventId': 11,
            'startedEventId': 12,
            'timeoutType': 'START_TO_CLOSE'
        },
    'eventId': 13,
    'eventTimestamp': 1530183327.421,
    'eventType': 'ActivityTaskTimedOut'
}

critical_activity_namelist = ['basic_activity_2']
not_timed_out_activity = ['basic_activity_1']
