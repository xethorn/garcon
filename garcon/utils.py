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
