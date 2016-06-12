import time
from datetime import datetime
from pickle import UnpicklingError

import pytest

from aiorq import (cancel_job, get_current_job, requeue_job, Queue,
                   get_failed_queue, Worker)
from aiorq.exceptions import NoSuchJobError
from aiorq.job import Job, loads, description
from aiorq.protocol import (enqueue_job, dequeue_job, start_job,
                            finish_job, fail_job)
from aiorq.specs import JobStatus
from aiorq.utils import utcformat, utcnow
from fixtures import (Number, some_calculation, say_hello,
                      CallableObject, access_self, long_running_job,
                      echo, UnicodeStringObject, div_by_zero, l)
from helpers import strip_microseconds


# Loads.


def test_loads(redis):
    """Loads job form the job spec."""

    id = b'2a5079e7-387b-492f-a81c-68aa55c194c8'
    spec = {
        b'created_at': b'2016-04-05T22:40:35Z',
        b'data': b'\x80\x04\x950\x00\x00\x00\x00\x00\x00\x00(\x8c\x19fixtures.some_calculation\x94NK\x03K\x04\x86\x94}\x94\x8c\x01z\x94K\x02st\x94.',  # noqa
        b'description': b'fixtures.some_calculation(3, 4, z=2)',
        b'timeout': 180,
        b'result_ttl': 5000,
        b'status': JobStatus.QUEUED.encode(),
        b'origin': b'default',
        b'enqueued_at': b'2016-05-03T12:10:11Z',
    }
    job = loads(redis, id, spec)
    assert job.connection == redis
    assert job.id == '2a5079e7-387b-492f-a81c-68aa55c194c8'
    assert job.created_at == datetime(2016, 4, 5, 22, 40, 35)
    assert job.func == some_calculation
    assert job.args == (3, 4)
    assert job.kwargs == {'z': 2}
    assert job.description == 'fixtures.some_calculation(3, 4, z=2)'
    assert job.timeout == 180
    assert job.result_ttl == 5000
    assert job.status == 'queued'
    assert job.origin == 'default'
    assert job.enqueued_at == datetime(2016, 5, 3, 12, 10, 11)


def test_loads_instance_method(redis):
    """Loads instance method job form the job spec."""

    id = b'2a5079e7-387b-492f-a81c-68aa55c194c8'
    spec = {
        b'created_at': b'2016-04-05T22:40:35Z',
        b'data': b'\x80\x04\x959\x00\x00\x00\x00\x00\x00\x00(\x8c\x03div\x94\x8c\x08fixtures\x94\x8c\x06Number\x94\x93\x94)}\x94\x92\x94}\x94\x8c\x05value\x94K\x02sbK\x04\x85\x94}\x94t\x94.',  # noqa
        b'description': b'div(4)',
        b'timeout': 180,
        b'result_ttl': 5000,
        b'status': JobStatus.QUEUED.encode(),
        b'origin': b'default',
        b'enqueued_at': b'2016-05-03T12:10:11Z',
    }
    job = loads(redis, id, spec)
    assert job.connection == redis
    assert job.id == '2a5079e7-387b-492f-a81c-68aa55c194c8'
    assert job.created_at == datetime(2016, 4, 5, 22, 40, 35)
    assert job.func.__name__ == 'div'
    assert job.func.__self__.__class__ == Number
    assert job.args == (4,)
    assert job.kwargs == {}
    assert job.description == 'div(4)'
    assert job.timeout == 180
    assert job.result_ttl == 5000
    assert job.status == 'queued'
    assert job.origin == 'default'
    assert job.enqueued_at == datetime(2016, 5, 3, 12, 10, 11)


def test_loads_unreadable_data(redis):
    """Loads unreadable pickle string will raise UnpickleError."""

    id = b'2a5079e7-387b-492f-a81c-68aa55c194c8'
    spec = {
        b'created_at': b'2016-04-05T22:40:35Z',
        b'data': b'this is no pickle string',
        b'description': b'fixtures.some_calculation(3, 4, z=2)',
        b'timeout': 180,
        b'result_ttl': 5000,
        b'status': JobStatus.QUEUED.encode(),
        b'origin': b'default',
        b'enqueued_at': b'2016-05-03T12:10:11Z',
    }
    with pytest.raises(UnpicklingError):
        loads(redis, id, spec)


def test_loads_unimportable_data(redis):
    """Loads unimportable data will raise attribute error."""

    id = b'2a5079e7-387b-492f-a81c-68aa55c194c8'
    spec = {
        b'created_at': b'2016-04-05T22:40:35Z',
        # nay_hello instead of say_hello
        b'data': b"\x80\x04\x95'\x00\x00\x00\x00\x00\x00\x00(\x8c\x12fixtures.nay_hello\x94N\x8c\x06Lionel\x94\x85\x94}\x94t\x94.",  # noqa
        b'description': b"fixtures.say_hello('Lionel')",
        b'timeout': 180,
        b'result_ttl': 5000,
        b'status': JobStatus.QUEUED.encode(),
        b'origin': b'default',
        b'enqueued_at': b'2016-05-03T12:10:11Z',
    }
    with pytest.raises(AttributeError):
        loads(redis, id, spec)


# Description.


def test_description():
    """Make job description."""

    desc = description('fixtures.some_calculation', (3, 4), {'z': 2})
    assert desc == 'fixtures.some_calculation(3, 4, z=2)'


# Job.


def test_job_status(redis):
    """Access job status checkers like is_started."""

    job = Job(
        connection=redis,
        id='56e6ba45-1aa3-4724-8c9f-51b7b0031cee',
        func=some_calculation,
        args=(3, 4),
        kwargs={'z': 2},
        description='fixtures.some_calculation(3, 4, z=2)',
        timeout=180,
        result_ttl=5000,
        origin='default',
        created_at=datetime(2016, 4, 5, 22, 40, 35))
    # TODO: use queue methods here?
    yield from enqueue_job(redis, 'default',
                           '56e6ba45-1aa3-4724-8c9f-51b7b0031cee', b'xxx',
                           'fixtures.some_calculation(3, 4, z=2)', 180,
                           '2016-04-05T22:40:35Z')
    assert (yield from job.is_queued)
    yield from dequeue_job(redis, 'default')
    yield from start_job(redis, 'default',
                         '56e6ba45-1aa3-4724-8c9f-51b7b0031cee', 180)
    assert (yield from job.is_started)
    yield from finish_job(redis, 'default',
                          '56e6ba45-1aa3-4724-8c9f-51b7b0031cee')
    assert (yield from job.is_finished)
    yield from fail_job(redis, 'default',
                        '56e6ba45-1aa3-4724-8c9f-51b7b0031cee',
                        "Exception('We are here')")
    assert (yield from job.is_failed)
    job = Job(
        connection=redis,
        id='2a5079e7-387b-492f-a81c-68aa55c194c8',
        created_at=datetime(2016, 4, 5, 22, 40, 35),
        func=some_calculation,
        args=(3, 4),
        kwargs={'z': 2},
        description='fixtures.some_calculation(3, 4, z=2)',
        timeout=180,
        result_ttl=5000,
        origin='default',
        dependency_id=job.id)
    yield from enqueue_job(redis, 'default',
                           '2a5079e7-387b-492f-a81c-68aa55c194c8', b'xxx',
                           'fixtures.some_calculation(3, 4, z=2)', 180,
                           '2016-04-05T22:40:35Z',
                           dependency_id='56e6ba45-1aa3-4724-8c9f-51b7b0031cee')
    assert (yield from job.is_deferred)
    status = yield from job.get_status()
    assert status == JobStatus.DEFERRED


# TODO: persist job meta dict as pickle
# TODO: persist result ttl
# TODO: persist custom description


def test_job_access_outside_job_fails():
    """The current job is accessible only within a job context."""

    assert not (yield from get_current_job())


# TODO: get job inside running worker
# TODO: calculate result_ttl if not specified
# TODO: calculate job ttl field
# TODO: set ttl via queue.enqueue
# TODO: persist custom job_id with queue.enqueue
# TODO: description with unicode string in the argument repr
# TODO: create job with ttl, dequeue job, ttl argument should be equal
# TODO: expire job with ttl
