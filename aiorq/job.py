"""
    aiorq.job
    ~~~~~~~~~

    This module implement serializable deferred jobs.

    :copyright: (c) 2015-2016 by Artem Malyshev.
    :license: LGPL-3, see LICENSE for more details.
"""
# This code was adapted from rq.job module written by Vincent Driessen
# and released under 2-clause BSD license.

import asyncio
import pickle

from . import protocol
from .specs import JobStatus
from .utils import utcformat, utcparse, import_attribute, function_name


@asyncio.coroutine
def cancel_job(job_id, connection=None):
    """Cancels the job with the given job ID, preventing execution.

    Discards any job info (i.e. it can't be requeued later).
    """

    return None


@asyncio.coroutine
def requeue_job(job_id, connection=None):
    """Requeues the job with the given job ID.

    The job ID should be refer to a failed job (i.e. it should be on
    the failed queue).  If no such (failed) job exists, a
    NoSuchJobError is raised.
    """

    return None


@asyncio.coroutine
def get_current_job(connection=None):
    """Returns the Job instance that is currently being executed.

    If this function is invoked from outside a job context, None is
    returned.
    """

    return None


def create_job(redis, id, spec):
    """Create job instance from job id and protocol job spec."""

    job_id = id if isinstance(id, str) else id.decode()
    created_at = utcparse(spec[b'created_at'].decode())
    enqueued_at = utcparse(spec[b'enqueued_at'].decode())
    func_name, instance, args, kwargs = pickle.loads(spec[b'data'])
    if instance:
        func = getattr(instance, func_name)
    else:
        func = import_attribute(func_name)
    description = spec[b'description'].decode()
    status = spec[b'status'].decode()
    origin = spec[b'origin'].decode()
    timeout = spec[b'timeout']
    result_ttl = spec[b'result_ttl']
    job = Job(connection=redis, id=job_id, created_at=created_at,
              enqueued_at=enqueued_at, func=func, args=args,
              kwargs=kwargs, description=description, timeout=timeout,
              result_ttl=result_ttl, status=status, origin=origin)
    return job


class Job:
    """A Job is just convenient data structure to pass around (meta) data."""

    protocol = protocol

    def __init__(self, connection, id, func, args, kwargs, description,
                 timeout, result_ttl, origin, created_at,
                 enqueued_at=None, status=None, dependency_id=None):

        self.connection = connection
        self.id = id
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.description = description
        self.timeout = timeout
        self.result_ttl = result_ttl
        self.origin = origin
        self.created_at = created_at
        self.enqueued_at = enqueued_at  # TODO: don't store in spec if None
        self.status = status  # TODO: don't store in spec if None
        self.dependency_id = dependency_id  # TODO: don't store in spec if None

    @asyncio.coroutine
    def get_status(self):
        """Get job status asynchronously."""

        status = yield from self.protocol.job_status(self.connection, self.id)
        self.status = status.decode()
        return self.status

    @property
    @asyncio.coroutine
    def is_finished(self):

        status = yield from self.protocol.job_status(self.connection, self.id)
        self.status = status.decode()
        return self.status == JobStatus.FINISHED

    @property
    @asyncio.coroutine
    def is_queued(self):

        status = yield from self.protocol.job_status(self.connection, self.id)
        self.status = status.decode()
        return self.status == JobStatus.QUEUED

    @property
    @asyncio.coroutine
    def is_failed(self):

        status = yield from self.protocol.job_status(self.connection, self.id)
        self.status = status.decode()
        return self.status == JobStatus.FAILED

    @property
    @asyncio.coroutine
    def is_started(self):

        status = yield from self.protocol.job_status(self.connection, self.id)
        self.status = status.decode()
        return self.status == JobStatus.STARTED

    @property
    @asyncio.coroutine
    def is_deferred(self):

        status = yield from self.protocol.job_status(self.connection, self.id)
        self.status = status.decode()
        return self.status == JobStatus.DEFERRED

    @property
    @asyncio.coroutine
    def result(self):
        """Returns the return value of the job.

        Initially, right after enqueueing a job, the return value will be
        None.  But when the job has been executed, and had a return value or
        exception, this will return that value or exception.

        Note that, when the job has no return value (i.e. returns None), the
        ReadOnlyJob object is useless, as the result won't be written back to
        Redis.

        Also note that you cannot draw the conclusion that a job has _not_
        been executed when its return value is None, since return values
        written back to Redis will expire after a given amount of time (500
        seconds by default).
        """

        if self._result is None:
            rv = yield from self.connection.hget(self.key, 'result')
            if rv is not None:
                self._result = loads(rv)
        return self._result
