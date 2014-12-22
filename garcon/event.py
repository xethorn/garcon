import collections

from garcon import activity


def prepare_events(events):
    """Prepare the events to be more easy to understand.
    """
    import pprint

    events = sorted(events, key=lambda item: item.get('eventId'))
    event_id_name = dict()
    activity_events = dict()

    for event in events:
        event_id = event.get('eventId')
        event_type = event.get('eventType')

        if event_type == 'ActivityTaskScheduled':
            activity_info = event.get('activityTaskScheduledEventAttributes')
            activity_name = activity_info.get('activityType').get('name')
            event_id_name.update({
                event_id: activity_name
            })

            activity_events.update({
                activity_name: activity.ACTIVITY_SCHEDULED
            })
        elif event_type == 'ActivityTaskCompleted':
            activity_info = event.get('activityTaskCompletedEventAttributes')
            activity_name = event_id_name.get(
                activity_info.get('scheduledEventId'))

            activity_events.update({
                activity_name: activity.ACTIVITY_COMPLETED
            })

    pprint.pprint(activity_events)
    return activity_events
