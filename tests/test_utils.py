import pytest
import datetime

from garcon import utils


def test_create_dictionary_key_with_string():
    """Try creating a key using a string instead of a dict.
    """

    with pytest.raises(TypeError):
        utils.create_dictionary_key('something')


def test_create_dictionary_key_with_empty_dict():
    """Try creating a key using an empty dictionary.
    """

    with pytest.raises(ValueError):
        utils.create_dictionary_key(dict())


def test_create_dictionary_key():
    """Try craeting a unique key from a dict.
    """

    values = [
        dict(foo=10),
        dict(foo2=datetime.datetime.now())]

    for value in values:
        assert len(utils.create_dictionary_key(value)) == 40
