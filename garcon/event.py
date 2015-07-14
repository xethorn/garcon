# -*- coding: utf-8 -*-
from garcon import activity
import json


def activity_states_from_events(events):
    """Get activity states from a list of events.

    The workflow events contains the different states of our activities. This
    method consumes the logs, and regenerates a dictionnary with the list of
    all the activities and their states.

    Note:
        Please note: from the list of events, only activities that have been
        registered are accessible. For all the others that have not yet
        started, they won't be part of this list.

    Args:
        events (dict): list of all the events.
    Return:
        `dict`: the activities and their state.
    """

    events = sorted(events, key=lambda item: item.get('eventId'))
    event_id_info = dict()
    activity_events = dict()

    for event in events:
        event_id = event.get('eventId')
        event_type = event.get('eventType')

        if event_type == 'ActivityTaskScheduled':
            activity_info = event.get('activityTaskScheduledEventAttributes')
            activity_id = activity_info.get('activityId')
            activity_name = activity_info.get('activityType').get('name')
            event_id_info.update({
                event_id: {
                    'activity_name': activity_name,
                    'activity_id': activity_id}
            })

            activity_events.setdefault(
                activity_name, {}).setdefault(
                    activity_id, activity.ActivityState(activity_id)).add_state(
                        activity.ACTIVITY_SCHEDULED)

        elif event_type == 'ActivityTaskFailed':
            activity_info = event.get('activityTaskFailedEventAttributes')
            activity_event = event_id_info.get(
                activity_info.get('scheduledEventId'))
            activity_id = activity_event.get('activity_id')

            activity_events.setdefault(
                activity_event.get('activity_name'), {}).setdefault(
                    activity_id,
                    activity.ActivityState(activity_id)).add_state(
                        activity.ACTIVITY_FAILED)

        elif event_type == 'ActivityTaskCompleted':
            activity_info = event.get('activityTaskCompletedEventAttributes')
            activity_event = event_id_info.get(
                activity_info.get('scheduledEventId'))
            activity_id = activity_event.get('activity_id')

            activity_events.setdefault(
                activity_event.get('activity_name'), {}).setdefault(
                    activity_id,
                    activity.ActivityState(activity_id)).add_state(
                        activity.ACTIVITY_COMPLETED)

            result = json.loads(activity_info.get('result') or '{}')
            activity_events.get(
                activity_event.get('activity_name')).get(
                    activity_id).set_result(result)

    return activity_events


def get_current_context(events):
    """Get the current context from the list of events.

    Each activity returns bits of information that needs to be provided to the
    next activities.

    Args:
        events (list): List of events.
    Return:
        dict: The current context.
    """

    events = sorted(events, key=lambda item: item.get('eventId'))
    context = {}

    for event in events:
        event_id = event.get('eventId')
        event_type = event.get('eventType')
        result = None

        if event_type == 'ActivityTaskCompleted':
            attributes = event['activityTaskCompletedEventAttributes']
            result = attributes.get('result')

        if event_type == 'WorkflowExecutionStarted':
            attributes = event['workflowExecutionStartedEventAttributes']
            result = attributes.get('input')

        if result:
            context.update(json.loads(result))

    return context
