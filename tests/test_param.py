import pytest

from garcon import param


def test_base_param_class():
    """Test the base class: cannot get data and return no requirements.
    """

    current_param = param.BaseParam()
    with pytest.raises(NotImplementedError):
        current_param.get_data({})

    assert not list(current_param.requirements)


def test_static_param():
    """Test the behavior of the static param class
    """

    message = 'Hello World'
    current_param = param.StaticParam(message)
    assert current_param.get_data({}) is message
    assert not list(current_param.requirements)


def test_default_param():
    """Test the behavior of the default param class.
    """

    key = 'context.key'
    message = 'Hello World'
    current_param = param.Param(key)
    requirements = list(current_param.requirements)
    assert current_param.get_data({key: message}) is message
    assert requirements[0] is key


def test_all_requirements():
    """Test getting all the requirements.
    """

    keys = ['context.key1', 'context.key2', 'context.key3']
    manual_keys = ['context.manual_key1', 'context.manual_key2']
    params = [param.Param(key) for key in keys]
    params += manual_keys
    params += [param.StaticParam('Value')]
    params = [param.parametrize(current_param) for current_param in params]

    resp = param.get_all_requirements(params)
    for key in keys:
        assert key in resp

    for manual_key in manual_keys:
        assert manual_key in resp

    assert 'Value' not in resp


def test_parametrize():
    """Test parametrize.

    Parametrize only allows objects that inherits BaseParam or string.
    """

    keys = ['context.key1', 'context.key2', 'context.key3']
    manual_keys = ['context.manual_key1', 'context.manual_key2']
    params = [param.Param(key) for key in keys]
    params += manual_keys
    params += [param.StaticParam('Value')]
    params = [param.parametrize(current_param) for current_param in params]

    for current_param in params:
        assert isinstance(current_param, param.BaseParam)

    with pytest.raises(param.UnknownParamException):
        param.parametrize(list('Unknown'))
