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

    return b'rq:queues'


def queue_key(name):
    """Redis key for named queue."""

    return b'rq:queue:' + name


def failed_queue_key():
    """Redis key for failed queue."""

    return queue_key(JobStatus.FAILED)


def job_key(id):
    """Redis key for job hash."""

    return b'rq:job:' + id
