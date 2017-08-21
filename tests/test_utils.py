try:
    from unittest.mock import MagicMock
except:
    from mock import MagicMock
import boto.exception as boto_exception
import datetime
import pytest
import json

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
    """Try creating a unique key from a dict.
    """

    values = [
        dict(foo=10),
        dict(foo2=datetime.datetime.now())]

    for value in values:
        assert len(utils.create_dictionary_key(value)) == 40

def test_create_dictionary_key():
    """Try creating a unique key from a dict.
    """

    values = [
        dict(foo=10),
        dict(foo2=datetime.datetime.now())]

    for value in values:
        assert len(utils.create_dictionary_key(value)) == 40


def test_non_throttle_error():
    """Assert SWF error is evaluated as non-throttle error properly.
    """

    response_status = 400
    response_reason = 'Bad Request'
    reponse_body = (
        '{"__type": "com.amazon.coral.availability#ThrottlingException",'
        '"message": "Rate exceeded"}')
    json_body = json.loads(reponse_body)
    exception = boto_exception.SWFResponseError(
        response_status, response_reason, body=json_body)
    result = utils.non_throttle_error(exception)
    assert not utils.non_throttle_error(exception)

    reponse_body = (
        '{"__type": "com.amazon.coral.availability#OtheException",'
        '"message": "Rate exceeded"}')
    json_body = json.loads(reponse_body)
    exception = boto_exception.SWFResponseError(
        response_status, response_reason, body=json_body)
    assert utils.non_throttle_error(exception)

def test_throttle_backoff_handler():
    """Assert backoff is logged correctly.
    """

    mock_activity = MagicMock()
    details = dict(
        args=(mock_activity,),
        tries=5,
        wait=10)
    utils.throttle_backoff_handler(details)
    mock_activity.logger.info.assert_called_with(
        'Throttle Exception occurred on try {}. '
        'Sleeping for {} seconds'.format(
            details['tries'], details['wait']))
