"""
    aiorq.queue
    ~~~~~~~~~~~

    This module define asyncio compatible job queue.

    :copyright: (c) 2015-2016 by Artem Malyshev.
    :license: LGPL-3, see LICENSE for more details.
"""
# This code was adapted from rq.queue module written by Vincent
# Driessen and released under 2-clause BSD license.

import asyncio
import functools
import pickle
import uuid

from . import protocol
from .exceptions import (NoSuchJobError, UnpickleError,
                         DequeueTimeout, InvalidJobOperationError)
from .job import Job, create_job
from .utils import (function_name, utcnow, utcformat,
                    make_description, import_attribute)


def get_failed_queue(connection=None):
    """Returns a handle to the special failed queue."""

    return FailedQueue(connection=connection)


@functools.total_ordering
class Queue:
    """asyncio job queue."""

    job_class = Job
    protocol = protocol
    default_timeout = 180

    @classmethod
    @asyncio.coroutine
    def all(cls, connection):
        """Returns an iterable of all Queues."""

        keys = yield from cls.protocol.queues(connection)
        return [cls(name=key.decode(), connection=connection)
                for key in keys]

    def __init__(self, connection, name='default', default_timeout=None,
                 job_class=None):

        self.connection = connection
        self.name = name

        if default_timeout:
            self.default_timeout = default_timeout

        if job_class is not None:
            if isinstance(job_class, str):
                job_class = import_attribute(job_class)
            self.job_class = job_class

    @asyncio.coroutine
    def empty(self):
        """Removes all messages on the queue."""

        return (yield from self.protocol.empty_queue(self.connection, self.name))

    @asyncio.coroutine
    def is_empty(self):
        """Returns whether the current queue is empty."""

        return (yield from self.protocol.queue_length(self.connection, self.name)) == 0

    @asyncio.coroutine
    def fetch_job(self, job_id):
        spec = yield from self.protocol.job(self.connection, job_id)
        if spec:
            # TODO: respect self.job_class here.
            job = create_job(self.connection, job_id, spec)
            return job
        else:
            yield from self.protocol.cancel_job(self.connection, self.name, job_id)

    @asyncio.coroutine
    def get_job_ids(self, offset=0, length=-1):
        """Returns a slice of job IDs in the queue."""

        start = offset
        if length >= 0:
            end = offset + (length - 1)
        else:
            end = length
        jobs = yield from self.protocol.jobs(self.connection, self.name, start, end)
        return [job_id.decode() for job_id in jobs]

    @asyncio.coroutine
    def get_jobs(self, offset=0, length=-1):
        """Returns a slice of jobs in the queue."""

        job_ids = yield from self.get_job_ids(offset, length)
        jobs = []
        for job_id in job_ids:
            job = yield from self.protocol.job(self.connection, job_id)
            jobs.append(create_job(self.connection, job_id, job))
        return jobs

    @property
    @asyncio.coroutine
    def job_ids(self):
        """Returns a list of all job IDS in the queue."""

        return (yield from self.get_job_ids())

    @property
    @asyncio.coroutine
    def jobs(self):
        """Returns a list of all (valid) jobs in the queue."""

        return (yield from self.get_jobs())

    @property
    @asyncio.coroutine
    def count(self):
        """Returns a count of all messages in the queue."""

        return (yield from self.protocol.queue_length(self.connection, self.name))

    @asyncio.coroutine
    def remove(self, job_or_id):
        """Removes Job from queue, accepts either a Job instance or ID."""

        job_id = (job_or_id.id
                  if isinstance(job_or_id, self.job_class)
                  else job_or_id)
        yield from self.protocol.cancel_job(self.connection, self.name, job_id)

    @asyncio.coroutine
    def compact(self):
        """Removes all "dead" jobs from the queue by cycling through it, while
        guaranteeing FIFO semantics.
        """

        COMPACT_QUEUE = 'rq:queue:_compact:{0}'.format(uuid.uuid4())

        yield from self.connection.rename(self.key, COMPACT_QUEUE)
        while True:
            job_id = as_text((yield from self.connection.lpop(COMPACT_QUEUE)))
            if job_id is None:
                break
            if (yield from self.job_class.exists(job_id, self.connection)):
                (yield from self.connection.rpush(self.key, job_id))

    @asyncio.coroutine
    def enqueue(self, f, *args, **kwargs):
        """Creates a job to represent the delayed function call and enqueues
        it.

        Expects the function to call, along with the arguments and keyword
        arguments.

        The function argument `f` may be any of the following:

        * A reference to a function
        * A reference to an object's instance method
        * A string, representing the location of a function (must be
          meaningful to the import context of the workers)
        """

        return (yield from self.enqueue_call(f, args, kwargs))

    @asyncio.coroutine
    def enqueue_call(self, func, args=None, kwargs=None, timeout=None,
                     result_ttl=None, ttl=None, description=None,
                     depends_on=None, job_id=None, at_front=False, meta=None):
        """Creates a job to represent the delayed function call and enqueues
        it.

        It is much like `.enqueue()`, except that it takes the function's args
        and kwargs as explicit arguments.  Any kwargs passed to this function
        contain options for RQ itself.
        """

        id = job_id or str(uuid.uuid4())
        args = args or ()
        kwargs = kwargs or {}
        func_name, instance = function_name(func)
        job_tuple = func_name, instance, args, kwargs
        data = pickle.dumps(job_tuple, protocol=pickle.HIGHEST_PROTOCOL)
        description = description or make_description(func_name, args, kwargs)
        timeout = timeout or self.default_timeout
        created_at = utcnow()
        spec = {
            'redis': self.connection,
            'queue': self.name,
            'id': id,
            'data': data,
            'description': description,
            'timeout': timeout,
            'created_at': utcformat(created_at),
            'at_front': at_front,
        }
        if result_ttl:
            spec['result_ttl'] = result_ttl
        if ttl:
            pass     # TODO: process ttl
        if depends_on:
            # TODO: can we use None instead of unset in the protocol?
            if isinstance(depends_on, self.job_class):
                spec['dependency_id'] = depends_on.id
            else:
                spec['dependency_id'] = depends_on
        # TODO: pass meta argument after rq 0.5.7 release
        status, enqueued_at = yield from self.protocol.enqueue_job(**spec)
        job_spec = {
            'connection': self.connection,
            'id': id,
            'func': func,
            'args': args,
            'kwargs': kwargs,
            'description': description,
            'timeout': timeout,
            'result_ttl': result_ttl, # TODO: what store here?
            'origin': self.name,
            'created_at': created_at,
            'status': status,
            'enqueued_at': enqueued_at,
        }
        job = self.job_class(**job_spec)
        return job

    def __eq__(self, other):

        return self.name == other.name

    def __lt__(self, other):

        return self.name < other.name

    def __hash__(self):
        return hash(self.name)

    def __repr__(self):
        return 'Queue({!r})'.format(self.name)

    def __str__(self):
        return '<Queue {!r}>'.format(self.name)


class FailedQueue(Queue):
    """Special queue for failed asynchronous jobs."""

    def __init__(self, connection=None):

        super().__init__(JobStatus.FAILED, connection=connection)

    @asyncio.coroutine
    def quarantine(self, job, exc_info):
        """Puts the given Job in quarantine (i.e. put it on the failed queue).
        """

        # Add Queue key set
        yield from self.connection.sadd(self.redis_queues_keys, self.key)

        job.ended_at = utcnow()
        # NOTE: we can't pass exception instance directly to aioredis
        # command execution as we can with StrictRedis.  StrictRedis
        # client make internal cast all non string values to the
        # string.  So we need to do it explicitly.
        job.exc_info = str(exc_info)
        pipe = self.connection.multi_exec()
        yield from job.save(pipeline=pipe)

        yield from self.push_job_id(job.id, pipeline=pipe)
        yield from pipe.execute()

        return job

    @asyncio.coroutine
    def requeue(self, job_id):
        """Requeues the job with the given job ID."""

        try:
            job = yield from self.job_class.fetch(
                job_id, connection=self.connection)
        except NoSuchJobError:
            # Silently ignore/remove this job and return (i.e. do nothing)
            yield from self.remove(job_id)
            return

        # Delete it from the failed queue (raise an error if that failed)
        if not (yield from self.remove(job)):
            raise InvalidJobOperationError('Cannot requeue non-failed jobs')

        yield from job.set_status(JobStatus.QUEUED)
        job.exc_info = None
        q = Queue(job.origin, connection=self.connection)
        yield from q.enqueue_job(job)
