import logging

from garcon import log
from tests.fixtures import log as fixture


def test_get_logger_namespace():
    """Test that the logger nsmaespace is generated properly for a given
    execution context.
    """

    assert log.get_logger_namespace(fixture.execution_context) == '.'.join([
        log.LOGGER_PREFIX,
        fixture.execution_context.get('execution.domain'),
        fixture.execution_context.get('execution.workflow_id'),
        fixture.execution_context.get('execution.run_id')])


def test_set_log_context():
    """Test that the logger_name property has set properly or has not been set
    if an invalid execution context is passed in.
    """

    valid_mock = fixture.log_enabled_object()
    invalid_mock = fixture.log_disabled_object()

    assert valid_mock.logger_name == '.'.join([
        log.LOGGER_PREFIX,
        fixture.execution_context.get('execution.domain'),
        fixture.execution_context.get('execution.workflow_id'),
        fixture.execution_context.get('execution.run_id')])
    assert valid_mock.logger is logging.getLogger(valid_mock.logger_name)

    assert getattr(invalid_mock, 'logger_name', None) is None
    assert invalid_mock.logger is logging.getLogger(log.LOGGER_PREFIX)


def test_unset_log_context():
    """Test that the logger_name property has been unset.
    """

    valid_mock = fixture.log_enabled_object()
    valid_mock.unset_log_context()

    assert getattr(valid_mock, 'logger_name', None) is None
