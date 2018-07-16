"""
Utils
=====

"""

import hashlib


def create_dictionary_key(dictionary):
    """Create a key that represents the content of the dictionary.

    Args:
        dictionary (dict): the dictionary to use.
    Return:
        str: the key that represents the content of the dictionary.
    """

    if not isinstance(dictionary, dict):
        raise TypeError('The value passed should be a dictionary.')

    if not dictionary:
        raise ValueError('The dictionary cannot be empty.')

    sorted_dict = sorted(dictionary.items())

    key_parts = ''.join([
        "'{key}':'{val}';".format(key=key, val=val)
        for (key, val) in sorted_dict])

    return hashlib.sha1(key_parts.encode('utf-8')).hexdigest()

def non_throttle_error(exception):
    """Activity Runner.

    Determine whether SWF Exception was a throttle or a different error.

    Args:
        exception: botocore.exceptions.Client instance.
    Return:
        bool: True if exception was a throttle, False otherwise.
    """

    return exception.response.get('Error').get('Code') != 'ThrottlingException'

def throttle_backoff_handler(details):
    """Callback to be used when a throttle backoff is invoked.

    For more details see: https://github.com/litl/backoff/#event-handlers

    Args:
        dictionary (dict): Details of the backoff invocation. Valid keys
            include:
                target: reference to the function or method being invoked.
                args: positional arguments to func.
                kwargs: keyword arguments to func.
                tries: number of invocation tries so far.
                wait: seconds to wait (on_backoff handler only).
                value: value triggering backoff (on_predicate decorator only).
    """

    activity = details['args'][0]
    activity.logger.info(
        'Throttle Exception occurred on try {}. '
        'Sleeping for {} seconds'.format(
            details['tries'], details['wait']))
