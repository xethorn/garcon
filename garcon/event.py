from garcon import activity


def activity_states_from_events(events):
    """Get activity states from a list of events.

    The workflow events contains the different states of our activities. This
    method consumes the logs, and regenerates a dictionnary with the list of
    all the activities and their states.

    Note:
        Please note: from the list of events, only activities that have been
        registered are accessible. For all the others that have not yet started,
        they won't be part of this list.

    Args:
        events (dict): list of all the events.
    Return:
        `dict`: the activities and their state.
    """

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

    return activity_events
