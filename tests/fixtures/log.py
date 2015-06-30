from garcon import log


class MockLogClient(log.GarconLogger):
    """Mock of an object for which we want to add a Garcon logger
    """
    domain = 'test_domain'


# Valid execution context
execution_context = {
    'execution.domain': 'dev',
    'execution.run_id': '123abc=',
    'execution.workflow_id': 'test-workflow-id'}

# Invalid execution context. Keys are incorrect
invalid_execution_context = {
    'abcd.domain': 'dev',
    '123.run_id': '123abc=',
    'XYZ.workflow_id': 'test-workflow-id'}


def log_enabled_object():
    """Creates a mock object with log enabled
    """

    mock = MockLogClient()
    mock.set_log_context(execution_context)

    return mock


def log_disabled_object():
    """Creates a mock object with no log
    """

    mock = MockLogClient()
    mock.set_log_context(invalid_execution_context)

    return mock
