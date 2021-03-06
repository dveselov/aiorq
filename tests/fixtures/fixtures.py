import asyncio
from unittest.mock import Mock

from aiorq import get_current_job
from aiorq.job import Job


@asyncio.coroutine
def say_hello(name=None):

    if name is None:
        name = 'Stranger'
    return 'Hi there, {}!'.format(name)


def some_calculation(x, y, z=1):

    return x * y / z


class Number:

    def __init__(self, value):

        self.value = value

    @classmethod
    def divide(cls, x, y):

        return x * y

    def div(self, y):

        return self.value / y


class CallableObject(object):

    def __call__(self):

        return "I'm callable"


@asyncio.coroutine
def access_self():

    assert get_current_job() is not None


@asyncio.coroutine
def long_running_job(timeout=10):

    yield from asyncio.sleep(timeout)
    return 'Done sleeping...'


@asyncio.coroutine
def echo(*args, **kwargs):

    return (args, kwargs)


class UnicodeStringObject:

    def __repr__(self):

        return 'é'


@asyncio.coroutine
def div_by_zero(x):

    return x / 0


l = lambda: 1


mock = Mock()


@asyncio.coroutine
def touch_a_mock():

    mock(1)


@asyncio.coroutine
def touch_a_mock_after_timeout(timeout):

    yield from asyncio.sleep(timeout)
    mock(1)


@asyncio.coroutine
def do_nothing():
    """The best job in the world."""

    pass


class CustomJob(Job):

    pass
