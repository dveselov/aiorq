"""
    aiorq.utils
    ~~~~~~~~~~~

    Utility functions.

    :copyright: (c) 2015-2016 by Artem Malyshev.
    :license: LGPL-3, see LICENSE for more details.
"""

from calendar import timegm
from datetime import datetime
from importlib import import_module
from inspect import ismethod, isfunction, isbuiltin


unset = object()


def current_timestamp():
    """Current UTC timestamp."""

    return timegm(datetime.utcnow().utctimetuple())


def utcparse(timestring):
    return datetime.strptime(timestring, '%Y-%m-%dT%H:%M:%SZ')


def utcformat(dt):
    return dt.strftime('%Y-%m-%dT%H:%M:%SZ')


def utcnow():
    return datetime.utcnow()


def import_attribute(name):
    """Return an attribute from a dotted path name (e.g. "path.to.func")."""

    module_name, attribute = name.rsplit('.', 1)
    module = import_module(module_name)
    return getattr(module, attribute)


def function_name(function):
    """Calculate function name."""

    instance = None
    if ismethod(function):
        func_name = function.__name__
        instance = function.__self__
    elif isinstance(function, str):
        func_name = function
    elif isinstance(function, bytes):
        func_name = function.decode()
    elif isfunction(function) or isbuiltin(function):
        func_name = '{}.{}'.format(function.__module__, function.__name__)
    elif hasattr(function, '__call__'):
        func_name = '__call__'
        instance = function
    else:
        msg = 'Expected a callable or a string, but got: {}'.format(function)
        raise TypeError(msg)
    return func_name, instance
