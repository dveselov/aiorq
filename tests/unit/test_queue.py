import asyncio
import pickle
from datetime import datetime

import pytest

import stubs
import helpers
from aiorq import Queue, get_failed_queue, Worker
from aiorq.exceptions import InvalidJobOperationError, DequeueTimeout
from aiorq.job import Job
from aiorq.specs import JobStatus
from aiorq.utils import unset, utcformat, utcnow
from fixtures import say_hello, Number, echo, div_by_zero, CustomJob


def test_store_connection():
    """Each queue store connection we give it."""

    connection = object()
    q = Queue(connection)
    assert q.connection is connection


def test_create_queue():
    """We can create queue instance."""

    connection = object()
    q = Queue(connection)
    assert q.name == 'default'


def test_create_named_queue():
    """We can create named queue instance."""

    connection = object()
    q = Queue(connection, 'my-queue')
    assert q.name == 'my-queue'


def test_queue_magic_methods():
    """Test simple magic method behavior of the Queue class."""

    connection = object()
    q = Queue(connection)
    assert hash(q) == hash('default')
    assert str(q) == "<Queue 'default'>"
    assert repr(q) == "Queue('default')"


def test_custom_default_timeout():
    """Override default timeout."""

    connection = object()
    q = Queue(connection)
    assert q.default_timeout == 180
    q = Queue(connection, default_timeout=500)
    assert q.default_timeout == 500


def test_custom_job_class():
    """Ensure custom job class assignment works as expected."""

    connection = object()
    q = Queue(connection, job_class=CustomJob)
    assert q.job_class == CustomJob


def test_custom_job_string():
    """Ensure custom job string assignment works as expected."""

    connection = object()
    q = Queue(connection, job_class='fixtures.CustomJob')
    assert q.job_class == CustomJob


def test_equality():
    """Mathematical equality of queues."""

    connection = object()
    q1 = Queue(connection, 'foo')
    q2 = Queue(connection, 'foo')
    q3 = Queue(connection, 'bar')
    assert q1 == q2
    assert q2 == q1
    assert q1 != q3
    assert q2 != q3


def test_queue_order():
    """Mathematical order of queues."""

    connection = object()
    q1 = Queue(connection, 'a')
    q2 = Queue(connection, 'b')
    q3 = Queue(connection, 'c')
    assert q1 < q2
    assert q3 > q2


def test_all_queues():
    """Get all created queues."""

    connection = object()

    queue_names = [b'foo', b'bar', b'baz']

    class Protocol:
        @staticmethod
        @asyncio.coroutine
        def queues(redis):
            assert redis is connection
            return queue_names

    class TestQueue(Queue):
        protocol = Protocol()

    queues = yield from TestQueue.all(connection)
    assert len(queues) == 3
    for queue in queues:
        assert isinstance(queue, TestQueue)
        assert queue.connection is connection
        assert queue.name == queue_names.pop(0).decode()


def test_empty_queue():
    """Emptying queues."""

    connection = object()

    class Protocol:
        @staticmethod
        @asyncio.coroutine
        def empty_queue(redis, name):
            assert redis is connection
            assert name == 'example'
            return 2

    class TestQueue(Queue):
        protocol = Protocol()

    q = TestQueue(connection, 'example')
    assert (yield from q.empty()) == 2


def test_queue_is_empty():
    """Detecting empty queues."""

    connection = object()
    lengths = [2, 0]

    class Protocol:
        @staticmethod
        @asyncio.coroutine
        def queue_length(redis, name):
            assert redis is connection
            assert name == 'example'
            return lengths.pop(0)

    class TestQueue(Queue):
        protocol = Protocol()

    q = TestQueue(connection, 'example')
    assert not (yield from q.is_empty())
    assert (yield from q.is_empty())


def test_queue_count():
    """Count all messages in the queue."""

    connection = object()

    class Protocol:
        @staticmethod
        @asyncio.coroutine
        def queue_length(redis, name):
            assert redis is connection
            assert name == 'example'
            return 3

    class TestQueue(Queue):
        protocol = Protocol()

    q = TestQueue(connection, 'example')
    assert (yield from q.count) == 3


def test_remove():
    """Ensure queue.remove properly removes Job from queue."""

    connection = object()
    sentinel = []

    class Protocol:
        @staticmethod
        @asyncio.coroutine
        def cancel_job(redis, name, id):
            assert redis is connection
            assert name == 'example'
            assert id == '56e6ba45-1aa3-4724-8c9f-51b7b0031cee'
            sentinel.append(1)

    class TestQueue(Queue):
        protocol = Protocol()

    q = TestQueue(connection, 'example')

    job = Job(
        connection=connection,
        id='56e6ba45-1aa3-4724-8c9f-51b7b0031cee',
        func=say_hello,
        args=(),
        kwargs={},
        description='fixtures.say_hello()',
        timeout=180,
        result_ttl=5000,
        origin='default',
        created_at=datetime(2016, 4, 5, 22, 40, 35))

    yield from q.remove(job)
    yield from q.remove(job.id)
    assert len(sentinel) == 2


def test_fetch_job():
    """Fetch job by id."""

    connection = object()

    class Protocol:
        @staticmethod
        @asyncio.coroutine
        def job(redis, id):
            assert redis is connection
            assert id == stubs.job_id
            return {
                b'created_at': b'2016-04-05T22:40:35Z',
                b'data': b'\x80\x04\x950\x00\x00\x00\x00\x00\x00\x00(\x8c\x19fixtures.some_calculation\x94NK\x03K\x04\x86\x94}\x94\x8c\x01z\x94K\x02st\x94.',  # noqa
                b'description': b'fixtures.some_calculation(3, 4, z=2)',
                b'timeout': 180,
                b'result_ttl': 5000,
                b'status': JobStatus.QUEUED.encode(),
                b'origin': stubs.queue.encode(),
                b'enqueued_at': utcformat(utcnow()).encode(),
            }

    class TestQueue(Queue):
        protocol = Protocol()

    q = TestQueue(connection)
    job = yield from q.fetch_job(stubs.job_id)
    assert job.connection is connection


def test_fetch_job_no_such_job():
    """Cancel job_id from the queue when job cache was missed."""

    connection = object()

    sentinel = []

    class Protocol:
        @staticmethod
        @asyncio.coroutine
        def job(redis, id):
            assert redis is connection
            assert id == stubs.job_id
            return {}

        @staticmethod
        @asyncio.coroutine
        def cancel_job(redis, queue, id):
            assert redis is connection
            assert queue == stubs.queue
            assert id == stubs.job_id
            sentinel.append(1)

    class TestQueue(Queue):
        protocol = Protocol()

    q = TestQueue(connection)
    job = yield from q.fetch_job(stubs.job_id)
    assert not job
    assert sentinel


def test_jobs():
    """Getting jobs out of a queue."""

    connection = object()

    class Protocol:
        @staticmethod
        @asyncio.coroutine
        def jobs(redis, queue, start, end):
            assert redis is connection
            assert queue == 'example'
            assert start == 0
            assert end == -1
            return [stubs.job_id.encode()]

        @staticmethod
        @asyncio.coroutine
        def job(redis, id):
            assert redis is connection
            assert id == stubs.job_id
            return {
                b'created_at': b'2016-04-05T22:40:35Z',
                b'data': b'\x80\x04\x950\x00\x00\x00\x00\x00\x00\x00(\x8c\x19fixtures.some_calculation\x94NK\x03K\x04\x86\x94}\x94\x8c\x01z\x94K\x02st\x94.',  # noqa
                b'description': b'fixtures.some_calculation(3, 4, z=2)',
                b'timeout': 180,
                b'result_ttl': 5000,
                b'status': JobStatus.QUEUED.encode(),
                b'origin': stubs.queue.encode(),
                b'enqueued_at': utcformat(utcnow()).encode(),
            }

    class TestQueue(Queue):
        protocol = Protocol()

    q = TestQueue(connection, 'example')
    [job] = yield from q.jobs
    assert job.connection is connection
    assert job.id == stubs.job_id
    assert job.description == stubs.job['description']


def test_jobs_expired_job():
    """Job hash can expire, so we should skip this id."""

    connection = object()

    class Protocol:
        @staticmethod
        @asyncio.coroutine
        def jobs(redis, queue, start, end):
            assert redis is connection
            assert queue == 'example'
            assert start == 0
            assert end == -1
            return [stubs.job_id.encode()]

        @staticmethod
        @asyncio.coroutine
        def job(redis, id):
            assert redis is connection
            assert id == stubs.job_id
            return {}

    class TestQueue(Queue):
        protocol = Protocol()

    q = TestQueue(connection, 'example')
    assert not (yield from q.jobs)


def test_job_ids():
    """Getting job ids out of a queue."""

    connection = object()

    class Protocol:
        @staticmethod
        @asyncio.coroutine
        def jobs(redis, queue, start, end):
            assert redis is connection
            assert queue == 'example'
            assert start == 0
            assert end == -1
            return [stubs.job_id.encode()]

    class TestQueue(Queue):
        protocol = Protocol()

    q = TestQueue(connection, 'example')
    ids = yield from q.job_ids
    assert ids == [stubs.job_id]


def test_get_job_ids_offset_and_length():
    """Offset and length arguments affects ids sequence."""

    connection = object()

    class Protocol:
        @staticmethod
        @asyncio.coroutine
        def jobs(redis, queue, start, end):
            assert redis is connection
            assert queue == 'example'
            assert start == 10
            assert end == 11
            return [b'foo', b'bar']

    class TestQueue(Queue):
        protocol = Protocol()

    q = TestQueue(connection, 'example')
    ids = yield from q.get_job_ids(10, 2)
    assert ids == ['foo', 'bar']


def test_compact():
    """Queue.compact() removes non-existing jobs."""

    connection = object()

    sentinel = []

    class Protocol:
        @staticmethod
        @asyncio.coroutine
        def compact_queue(redis, name):
            assert redis is connection
            assert name == 'example'
            sentinel.append(1)

    class TestQueue(Queue):
        protocol = Protocol()

    q = TestQueue(connection, 'example')
    yield from q.compact()
    assert sentinel


def test_enqueue():
    """Enqueueing job onto queues."""

    connection = object()
    uuids = []

    class Protocol:
        @staticmethod
        @asyncio.coroutine
        def enqueue_job(redis, queue, id, data, description, timeout,
                        created_at, *, result_ttl=unset, dependency_id=unset,
                        at_front=False):
            assert redis is connection
            assert queue == 'example'
            assert isinstance(id, str)
            assert data == b'\x80\x04\x952\x00\x00\x00\x00\x00\x00\x00(\x8c\x12fixtures.say_hello\x94N\x8c\x04Nick\x94\x85\x94}\x94\x8c\x03foo\x94\x8c\x03bar\x94st\x94.' # noqa
            assert description == "fixtures.say_hello('Nick', foo='bar')"
            assert timeout == 180
            assert created_at == utcformat(utcnow())
            assert result_ttl is unset
            assert dependency_id is unset
            assert at_front is False
            uuids.append(id)
            return JobStatus.QUEUED, utcnow()

    class TestQueue(Queue):
        protocol = Protocol()

    q = TestQueue(connection, 'example')

    job = yield from q.enqueue(say_hello, 'Nick', foo='bar')

    assert job.connection is connection
    assert job.id == uuids.pop(0)
    assert job.func == say_hello
    assert job.args == ('Nick',)
    assert job.kwargs == {'foo': 'bar'}
    assert job.description == "fixtures.say_hello('Nick', foo='bar')"
    assert job.timeout == 180
    assert job.result_ttl is None  # TODO: optional?
    assert job.origin == q.name
    assert helpers.strip_microseconds(job.created_at) == helpers.strip_microseconds(utcnow())
    assert helpers.strip_microseconds(job.enqueued_at) == helpers.strip_microseconds(utcnow())
    assert job.status == JobStatus.QUEUED
    assert job.dependency_id is None


def test_enqueue_call():
    """Enqueueing job onto queues."""

    connection = object()
    uuids = []

    class Protocol:
        @staticmethod
        @asyncio.coroutine
        def enqueue_job(redis, queue, id, data, description, timeout,
                        created_at, *, result_ttl=unset, dependency_id=unset,
                        at_front=False):
            assert redis is connection
            assert queue == 'example'
            assert isinstance(id, str)
            assert data == b'\x80\x04\x952\x00\x00\x00\x00\x00\x00\x00(\x8c\x12fixtures.say_hello\x94N\x8c\x04Nick\x94\x85\x94}\x94\x8c\x03foo\x94\x8c\x03bar\x94st\x94.' # noqa
            assert description == "fixtures.say_hello('Nick', foo='bar')"
            assert timeout == 180
            assert created_at == utcformat(utcnow())
            assert result_ttl is unset
            assert dependency_id is unset
            assert at_front is False
            uuids.append(id)
            return JobStatus.QUEUED, utcnow()

    class TestQueue(Queue):
        protocol = Protocol()

    q = TestQueue(connection, 'example')

    job = yield from q.enqueue_call(say_hello, args=('Nick',), kwargs={'foo': 'bar'})

    assert job.connection is connection
    assert job.id == uuids.pop(0)
    assert job.func == say_hello
    assert job.args == ('Nick',)
    assert job.kwargs == {'foo': 'bar'}
    assert job.description == "fixtures.say_hello('Nick', foo='bar')"
    assert job.timeout == 180
    assert job.result_ttl == None  # TODO: optional?
    assert job.origin == q.name
    assert helpers.strip_microseconds(job.created_at) == helpers.strip_microseconds(utcnow())
    assert helpers.strip_microseconds(job.enqueued_at) == helpers.strip_microseconds(utcnow())
    assert job.status == JobStatus.QUEUED
    assert job.dependency_id is None


def test_enqueue_call_custom_job_id():
    """Preserve passed job_id."""

    class Protocol:
        @staticmethod
        @asyncio.coroutine
        def enqueue_job(redis, queue, id, data, description, timeout,
                        created_at, *, result_ttl=unset, dependency_id=unset,
                        at_front=False):
            assert id == 'my_id'
            return JobStatus.QUEUED, utcnow()

    class TestQueue(Queue):
        protocol = Protocol()

    q = TestQueue(None)
    job = yield from q.enqueue_call(say_hello, job_id='my_id')
    assert job.id == 'my_id'


def test_enqueue_call_no_args():
    """Pass empty tuple in the case arguments were not provided."""

    class Protocol:
        @staticmethod
        @asyncio.coroutine
        def enqueue_job(redis, queue, id, data, description, timeout,
                        created_at, *, result_ttl=unset, dependency_id=unset,
                        at_front=False):
            _, _, args, _ = pickle.loads(data)
            assert args == ()
            return JobStatus.QUEUED, utcnow()

    class TestQueue(Queue):
        protocol = Protocol()

    q = TestQueue(None)
    job = yield from q.enqueue_call(say_hello)
    assert job.args == ()


def test_enqueue_call_no_kwargs():
    """Pass empty dict in the case keyword arguments were not
    provided.
    """

    class Protocol:
        @staticmethod
        @asyncio.coroutine
        def enqueue_job(redis, queue, id, data, description, timeout,
                        created_at, *, result_ttl=unset, dependency_id=unset,
                        at_front=False):
            _, _, _, kwargs = pickle.loads(data)
            assert kwargs == {}
            return JobStatus.QUEUED, utcnow()

    class TestQueue(Queue):
        protocol = Protocol()

    q = TestQueue(None)
    job = yield from q.enqueue_call(say_hello)
    assert job.kwargs == {}


def test_enqueue_call_preserve_desctiption():
    """Preserve job description passed."""

    class Protocol:
        @staticmethod
        @asyncio.coroutine
        def enqueue_job(redis, queue, id, data, description, timeout,
                        created_at, *, result_ttl=unset, dependency_id=unset,
                        at_front=False):
            assert description == 'My Job'
            return JobStatus.QUEUED, utcnow()

    class TestQueue(Queue):
        protocol = Protocol()

    q = TestQueue(None)
    job = yield from q.enqueue_call(say_hello, description='My Job')
    assert job.description == 'My Job'


def test_enqueue_call_preserve_timeout():
    """Preserve passed job timeout."""

    class Protocol:
        @staticmethod
        @asyncio.coroutine
        def enqueue_job(redis, queue, id, data, description, timeout,
                        created_at, *, result_ttl=unset, dependency_id=unset,
                        at_front=False):
            assert timeout == 7
            return JobStatus.QUEUED, utcnow()

    class TestQueue(Queue):
        protocol = Protocol()

    q = TestQueue(None)
    job = yield from q.enqueue_call(say_hello, timeout=7)
    assert job.timeout == 7


def test_enqueue_call_preserve_result_ttl():
    """Preserve passed result ttl."""

    class Protocol:
        @staticmethod
        @asyncio.coroutine
        def enqueue_job(redis, queue, id, data, description, timeout,
                        created_at, *, result_ttl=unset, dependency_id=unset,
                        at_front=False):
            assert result_ttl == 7
            return JobStatus.QUEUED, utcnow()

    class TestQueue(Queue):
        protocol = Protocol()

    q = TestQueue(None)
    job = yield from q.enqueue_call(say_hello, result_ttl=7)
    assert job.result_ttl == 7


def test_enqueue_call_dependency_id():
    """Pass dependency as id string.  Create deferred job."""

    dependencies = [unset, 'foo']
    replies = [(JobStatus.QUEUED, utcnow()),
               (JobStatus.DEFERRED, None)]

    class Protocol:
        @staticmethod
        @asyncio.coroutine
        def enqueue_job(redis, queue, id, data, description, timeout,
                        created_at, *, result_ttl=unset, dependency_id=unset,
                        at_front=False):
            assert dependency_id == dependencies.pop(0)
            return replies.pop(0)

    class TestQueue(Queue):
        protocol = Protocol()

    q = TestQueue(None)
    job_foo = yield from q.enqueue_call(say_hello, job_id='foo')
    job_bar = yield from q.enqueue_call(say_hello, job_id='bar', depends_on='foo')
    assert job_bar.status == JobStatus.DEFERRED
    assert not job_bar.enqueued_at


def test_enqueue_call_dependency_job():
    """Pass dependency as job instance.  Create deferred job."""

    dependencies = [unset, 'foo']
    replies = [(JobStatus.QUEUED, utcnow()),
               (JobStatus.DEFERRED, None)]

    class Protocol:
        @staticmethod
        @asyncio.coroutine
        def enqueue_job(redis, queue, id, data, description, timeout,
                        created_at, *, result_ttl=unset, dependency_id=unset,
                        at_front=False):
            assert dependency_id == dependencies.pop(0)
            return replies.pop(0)

    class TestQueue(Queue):
        protocol = Protocol()

    q = TestQueue(None)
    job_foo = yield from q.enqueue_call(say_hello, job_id='foo')
    job_bar = yield from q.enqueue_call(say_hello, job_id='bar', depends_on=job_foo)
    assert job_bar.status == JobStatus.DEFERRED
    assert not job_bar.enqueued_at


# TODO: meta field


# Failed queue tests.


# TODO: test_requeue_job
# TODO: test_requeue_nonfailed_job_fails
# TODO: test_quarantine_preserves_timeout
# TODO: test_requeueing_preserves_timeout
# TODO: test_requeue_sets_status_to_queued

# TODO: synchronize protocol stubs with actual protocol implementation
