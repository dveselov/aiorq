import pytest

from aiorq.utils import function_name
from fixtures import some_calculation, Number, CallableObject


def test_function_name_regular_function():
    """Use __module__ together with __name__ attribute for regular
    function.
    """

    func_name, instance = function_name(some_calculation)
    assert func_name == 'fixtures.some_calculation'
    assert instance is None


def test_function_name_instance_method():
    """Use method __name__ and method __self__ attributes."""

    n = Number(2)
    func_name, instance = function_name(n.div)
    assert func_name == 'div'
    assert instance is n


def test_function_name_string_function():
    """Use function name as is."""

    func_name, instance = function_name('fixtures.say_hello')
    assert func_name == 'fixtures.say_hello'
    assert instance is None


def test_function_name_bytes_function():
    """Use decoded bytes as function name."""

    func_name, instance = function_name(b'fixtures.say_hello')
    assert func_name == 'fixtures.say_hello'
    assert instance is None


def test_function_name_builtin():
    """Use builtins module for global objects."""

    func_name, instance = function_name(len)
    assert func_name == 'builtins.len'
    assert instance is None


def test_function_name_callable_object():
    """Use __call__ method and instance as is for callable objects."""

    kallable = CallableObject()
    func_name, instance = function_name(kallable)
    assert func_name == '__call__'
    assert instance is kallable


def test_function_name_not_a_function():
    """Raise exception in the case argument can't be a function."""

    with pytest.raises(TypeError):
        function_name(1)
