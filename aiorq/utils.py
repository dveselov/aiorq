"""
    aiorq.utils
    ~~~~~~~~~~~

    This module implements different utility functions.

    :copyright: (c) 2015-2016 by Artem Malyshev.
    :license: LGPL-3, see LICENSE for more details.
"""

import asyncio
import contextlib
import functools

from .exceptions import PipelineError


def pipelined_method(f):
    """Allows to pass transaction easily or create the new one if necessary."""

    @asyncio.coroutine
    @functools.wraps(f)
    def wrapper(self, *args, **kwargs):

        if 'pipeline' in kwargs:
            f(self, *args, **kwargs)
        else:
            pipe = self.connection.pipeline()
            kw = {'pipeline': pipe}
            kw.update(kwargs)
            f(self, *args, **kw)
            yield from pipe.execute()

    return wrapper


def pipeline_method(method):
    """Decorates method with access to current `asyncio.Task` pipeline."""

    @asyncio.coroutine
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):

        if pipeline_bound(self):
            method(self, *args, **kwargs)
        else:
            pipeline = self.connection.pipeline()
            with Pipeline(pipeline, loop=self.loop):
                method(self, *args, **kwargs)
            yield from pipeline.execute()

    return wrapper


@contextlib.contextmanager
def Pipeline(pipeline, *, loop=None):
    """Bind pipeline for current `asyncio.Task`."""

    current_task = asyncio.Task.current_task(loop=loop)
    current_task.pipeline = pipeline
    try:
        yield
    finally:
        del current_task.pipeline


def pipeline_bound(obj):
    """Check if there is pipeline binding for this object."""

    try:
        obj.pipeline
    except PipelineError:
        return False
    else:
        return True


@property
def pipeline_property(self):
    """Current `asyncio.Task` pipeline property."""

    current_task = asyncio.Task.current_task(loop=self.loop)
    try:
        current_pipeline = current_task.pipeline
    except AttributeError:
        raise PipelineError
    return current_pipeline
