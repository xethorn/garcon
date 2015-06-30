"""
Param
=====

Params are values that are passed to the activities. They are either provided
by the execution context (see Param) or are statically provided at the runtime
of the activity (see StaticParam). Custom params should extend the Param class.

Note:
    Make sure the custom param class lists out all the dependencies to the
    execution context in `requirements`.
"""


class BaseParam:
    """Base Param Class.

    Provides the structure and required methods of any param class.
    """

    @property
    def requirements(self):
        """Return the requirements for this param.
        """

        return
        yield

    def get_data(self, context):
        """Get the data.

        Args:
            context (dict): the context (optional) in which the data might be
                found. For Static Param this won't be necessary.
        """

        raise NotImplementedError()


class Param(BaseParam):

    def __init__(self, context_key):
        """Create a default param.

        Args:
            context_key (str): the context key.
        """

        self.context_key = context_key

    @property
    def requirements(self):
        """Return the requirements for this param.
        """

        yield self.context_key

    def get_data(self, context):
        """Get value from the context.

        Args:
            context (dict): the context in which the data might be found based
                on the key provided.

        Return:
            obj: an object from the context that corresponds to the context
                key.
        """

        return context.get(self.context_key, None)


class StaticParam(BaseParam):

    def __init__(self, value):
        """Create a static param.

        Args:
            value (str): the value of the param.
        """

        self.value = value

    def get_data(self, context):
        """Get value from the context.

        Args:
            context (dict): execution context (not used.)
        """

        return self.value


def get_all_requirements(params):
    """Get all the requirements from a list of params.

    Args:
        params (list): The list of params.
    """

    requirements = []
    for param in params:
        requirements += list(param.requirements)
    return requirements


def parametrize(requirement):
    """Parametrize a requirement.

    Args:
        requirement (*): the requirement to parametrize.
    """

    if isinstance(requirement, str):
        return Param(requirement)
    elif isinstance(requirement, BaseParam):
        return requirement
    raise UnknownParamException()


class UnknownParamException(Exception):
    pass
