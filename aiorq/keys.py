"""
    aiorq.keys
    ~~~~~~~~~~

    Redis keys naming convention.

    :copyright: (c) 2015-2016 by Artem Malyshev.
    :license: LGPL-3, see LICENSE for more details.
"""

from .specs import JobStatus


def queues_key():
    """Redis key for all named queues names."""

    return 'rq:queues'


def queue_key(name):
    """Redis key for named queue."""

    return 'rq:queue:' + name


def failed_queue_key():
    """Redis key for failed queue."""

    return queue_key(JobStatus.FAILED)


def job_key(id):
    """Redis key for job hash."""

    return 'rq:job:' + id


def dependents(id):
    """Redis key for job dependents."""

    return 'rq:job:' + id + ':dependents'


def started_registry(queue):
    """Redis key for started job registry."""

    return 'rq:wip:' + queue


def finished_registry(queue):
    """Redis key for finished job registry."""

    return 'rq:finished:' + queue


def deferred_registry(queue):
    """Redis key for deferred job registry."""

    return 'rq:deferred:' + queue


def workers_key():
    """Redis key for workers set."""

    return 'rq:workers'


def worker_key(name):
    """Redis key for worker."""

    return 'rq:worker:' + name
