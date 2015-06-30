"""Garcon logger module
"""

import logging


LOGGER_PREFIX = 'garcon'


class GarconLogger:
    """This class is meant to be extended to get the Garcon logger feature
    The logger injects the execution context into the logger name.

    This is used by the Activity class in Garcon and allows you to log from
    the activity object. Typically, you can log from a Garcon task and it will
    prefix your log messages with execution context information (domain,
    workflow_id, run_id).

    Requirements:
    Your loggers need to be set up so there is at least one of them with a name
    prefixed with LOGGER_PREFIX. The Garcon logger will inherit the handlers
    from that logger.

    The formatter for your handler(s) must use the logger name.
    Formatter Example::

        %(asctime)s - %(name)s - %(levelname)s - %(message)s

    This formatter will generate a log message as follow:
    '2015-01-15 - garcon.[domain].[workflow_id].[run_id] - [level] - [message]'
    """

    @property
    def logger(self):
        """Return the appropriate logger. Default to LOGGER_PREFIX if
        no logger name was set.

        Return:
            logging.Logger: a logger object
        """

        return logging.getLogger(
            getattr(self, 'logger_name', None) or LOGGER_PREFIX)

    def set_log_context(self, execution_context):
        """Set a logger name with execution context passed in.

        Args:
            execution_context (dict): execution context information
        """

        if ('execution.domain' in execution_context and
                'execution.workflow_id' in execution_context and
                'execution.run_id' in execution_context):

            self.logger_name = get_logger_namespace(execution_context)

    def unset_log_context(self):
        """Unset the logger name.
        """

        self.logger_name = None


def get_logger_namespace(execution_context):
    """Return the logger namespace for a given execution context.

    Args:
        execution_context (dict): execution context information
    """

    return '.'.join([
            LOGGER_PREFIX,
            execution_context.get('execution.domain'),
            execution_context.get('execution.workflow_id'),
            execution_context.get('execution.run_id')])
