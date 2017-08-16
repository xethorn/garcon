"""
Utils
=====

"""

import hashlib
import random


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


def exponential_backoff_delay(growth_factor, attempts, base=None):
    """Calculate time to sleep based on exponential function.

    base * growth_factor ^ (attempts - 1)

    Args:
        growth_factor (float): Initial sleep time of backoff.
        attempts (int): Number of attempts that have been made already. More
            attempts increases the time to sleep exponentially.
        base (float): If passed sets base level sleep time. Otherwise defaults
            to float between 1-2.
    Return:
        float: time in seconds to sleep between retries.
    """

    base = base or random.uniform(1, 2)
    time_to_sleep = base * (growth_factor ** (attempts - 1))
    return time_to_sleep
