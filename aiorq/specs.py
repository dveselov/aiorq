"""
    aiorq.specs
    ~~~~~~~~~~~

    State transition specs.

    :copyright: (c) 2015-2016 by Artem Malyshev.
    :license: LGPL-3, see LICENSE for more details.
"""


class JobStatus:

    QUEUED = 'queued'
    FINISHED = 'finished'
    FAILED = 'failed'
    STARTED = 'started'
    DEFERRED = 'deferred'


class WorkerStatus:

    STARTED = 'started'
    SUSPENDED = 'suspended'
    BUSY = 'busy'
    IDLE = 'idle'
