from rq.job import JobStatus

from aiorq.job import Job
from aiorq.queue import Queue, FailedQueue
from fixtures import say_hello


def test_add_and_remove(redis, registry, timestamp):
    """Adding and removing job to StartedJobRegistry."""

    job = Job()

    # Test that job is added with the right score
    yield from registry.add(job, 1000)
    assert (yield from redis.zscore(registry.key, job.id)) < timestamp + 1002

    # Ensure that a timeout of -1 results in a score of -1
    yield from registry.add(job, -1)
    assert (yield from redis.zscore(registry.key, job.id)) == -1

    # Ensure that job is properly removed from sorted set
    yield from registry.remove(job)
    assert not (yield from redis.zscore(registry.key, job.id))


def test_get_job_ids(redis, registry, timestamp):
    """Getting job ids from StartedJobRegistry."""

    yield from redis.zadd(registry.key, timestamp + 10, 'foo')
    yield from redis.zadd(registry.key, timestamp + 20, 'bar')
    assert (yield from registry.get_job_ids()) == ['foo', 'bar']


def test_get_expired_job_ids(redis, registry, timestamp):
    """Getting expired job ids form StartedJobRegistry."""

    yield from redis.zadd(registry.key, 1, 'foo')
    yield from redis.zadd(registry.key, timestamp + 10, 'bar')
    yield from redis.zadd(registry.key, timestamp + 30, 'baz')

    expired = yield from registry.get_expired_job_ids()
    assert expired == ['foo']
    expired = yield from registry.get_expired_job_ids(timestamp + 20)
    assert expired == ['foo', 'bar']


def test_cleanup(redis, registry):
    """Moving expired jobs to FailedQueue."""

    failed_queue = FailedQueue(connection=redis)
    assert (yield from failed_queue.is_empty())

    queue = Queue(connection=redis)
    job = yield from queue.enqueue(say_hello)

    yield from redis.zadd(registry.key, 2, job.id)

    yield from registry.cleanup(1)
    assert job.id not in (yield from failed_queue.job_ids)
    assert (yield from redis.zscore(registry.key, job.id)) == 2

    yield from registry.cleanup()
    assert job.id in (yield from failed_queue.job_ids)
    assert not (yield from redis.zscore(registry.key, job.id))
    yield from job.refresh()
    assert (yield from job.get_status()) == JobStatus.FAILED
