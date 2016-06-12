"""
    aiorq.protocol
    ~~~~~~~~~~~~~~

    Redis related state manipulations.

    :copyright: (c) 2015-2016 by Artem Malyshev.
    :license: LGPL-3, see LICENSE for more details.
"""

import asyncio

from .exceptions import InvalidOperationError
from .keys import (queues_key, queue_key, failed_queue_key, job_key,
                   started_registry, finished_registry, deferred_registry,
                   workers_key, worker_key, dependents)
from .specs import JobStatus, WorkerStatus
from .utils import unset, current_timestamp, utcformat, utcnow


@asyncio.coroutine
def queues(redis):
    """All RQ queues.

    :type redis: `aioredis.Redis`

    """

    return (yield from redis.smembers(queues_key()))


@asyncio.coroutine
def jobs(redis, queue, start=0, end=-1):
    """All queue jobs.

    :type redis: `aioredis.Redis`
    :type queue: str
    :type start: int
    :type end: int

    """

    return (yield from redis.lrange(queue_key(queue), start, end))


@asyncio.coroutine
def job(redis, id):
    """Get job hash by job id.

    :type redis: `aioredis.Redis`
    :type id: str

    """

    job_hash = yield from redis.hgetall(job_key(id))
    if b'timeout' in job_hash:
        job_hash[b'timeout'] = int(job_hash[b'timeout'])
    if b'result_ttl' in job_hash:
        job_hash[b'result_ttl'] = int(job_hash[b'result_ttl'])
    return job_hash


@asyncio.coroutine
def job_status(redis, id):
    """Get job status.

    :type redis: `aioredis.Redis`
    :type id: str

    """

    return (yield from redis.hget(job_key(id), 'status'))


@asyncio.coroutine
def started_jobs(redis, queue, start=0, end=-1):
    """All started jobs from this queue.

    :type redis: `aioredis.Redis`
    :type queue: str
    :type start: int
    :type end: int

    """

    return (yield from redis.zrange(started_registry(queue), start, end))


@asyncio.coroutine
def finished_jobs(redis, queue, start=0, end=-1):
    """All finished jobs from this queue.  Jobs are added to this registry
    after they have successfully completed for monitoring purposes.

    :type redis: `aioredis.Redis`
    :type queue: str
    :type start: int
    :type end: int

    """

    return (yield from redis.zrange(finished_registry(queue), start, end))


@asyncio.coroutine
def deferred_jobs(redis, queue, start=0, end=-1):
    """All deferred jobs from this queue.  Jobs are waiting for another
    job to finish in this registry.

    :type redis: `aioredis.Redis`
    :type queue: str
    :type start: int
    :type end: int

    """

    return (yield from redis.zrange(deferred_registry(queue), start, end))


@asyncio.coroutine
def queue_length(redis, name):
    """Get length of given queue.

    :type redis: `aioredis.Redis`
    :type name: str

    """

    return (yield from redis.llen(queue_key(name)))


@asyncio.coroutine
def empty_queue(redis, name):
    """Removes all jobs on the queue.

    :type redis: `aioredis.Redis`
    :type name: str

    """

    script = """
        local prefix = "rq:job:"
        local q = KEYS[1]
        local count = 0
        while true do
            local job_id = redis.call("lpop", q)
            if job_id == false then
                break
            end

            -- Delete the relevant keys
            redis.call("del", prefix..job_id)
            redis.call("del", prefix..job_id..":dependents")
            count = count + 1
        end
        return count
    """
    return (yield from redis.eval(script, keys=[queue_key(name)]))


@asyncio.coroutine
def compact_queue(redis):
    pass


@asyncio.coroutine
def enqueue_job(redis, queue, id, data, description, timeout,
                created_at, *, result_ttl=unset, dependency_id=unset,
                at_front=False):
    """Persists the job specification to it corresponding Redis id.

    :type redis: `aioredis.Redis`
    :type queue: str
    :type id: str
    :type data: str
    :type description: str
    :type timeout: str
    :type created_at: str
    :type result_ttl: int or None or unset
    :type dependency_id: str or unset
    :type at_front: bool

    """

    if result_ttl is None:
        result_ttl = -1
    has_dependency = False
    if dependency_id is not unset:
        dependency_status = yield from job_status(redis, dependency_id)
        if dependency_status != JobStatus.FINISHED.encode():
            has_dependency = True
    if has_dependency:
        status = JobStatus.DEFERRED
    else:
        status = JobStatus.QUEUED
    fields = (
        'origin', queue,
        'data', data,
        'description', description,
        'timeout', timeout,
        'created_at', created_at,
        'status', status)
    if result_ttl is not unset:
        fields += ('result_ttl', result_ttl)
    if not has_dependency:
        fields += ('enqueued_at', utcformat(utcnow()))
    multi = redis.multi_exec()
    multi.sadd(queues_key(), queue)
    multi.hmset(job_key(id), *fields)
    if has_dependency:
        score = current_timestamp()
        multi.zadd(deferred_registry(queue), score, id)
        multi.sadd(dependents(dependency_id), id)
    else:
        if at_front:
            multi.lpush(queue_key(queue), id)
        else:
            multi.rpush(queue_key(queue), id)
    # TODO: do we need expire job hash?
    yield from multi.execute()
    # TODO: return calculated fields.


@asyncio.coroutine
def dequeue_job(redis, queue):
    """Dequeue the front-most job from this queue.

    :type redis: `aioredis.Redis`
    :type queue: str

    """

    while True:
        job_id = yield from redis.lpop(queue_key(queue))
        if not job_id:
            return None, {}
        job_hash = yield from job(redis, job_id.decode())
        if not job_hash:
            continue
        return job_id, job_hash


@asyncio.coroutine
def cancel_job(redis, queue, id):
    """Removes job from queue.

    :type redis: `aioredis.Redis`
    :type queue: str
    :type id: str

    """

    # TODO: what we need to do with job hash and dependents set?
    yield from redis.lrem(queue_key(queue), 1, id)


@asyncio.coroutine
def start_job(redis, queue, id, timeout):
    """Start given job.

    :type redis: `aioredis.Redis`
    :type queue: str
    :type id: str
    :type timeout: int

    """

    fields = ('status', JobStatus.STARTED,
              'started_at', utcformat(utcnow()))
    score = current_timestamp() + timeout + 60
    # TODO: worker heartbeat.
    multi = redis.multi_exec()
    multi.hmset(job_key(id), *fields)
    multi.zadd(started_registry(queue), score, id)
    multi.persist(job_key(id))
    yield from multi.execute()


@asyncio.coroutine
def finish_job(redis, queue, id, *, result_ttl=500):
    """Finish given job.

    :type redis: `aioredis.Redis`
    :type queue: str
    :type id: str
    :type result_ttl: int

    """

    if result_ttl == 0:
        yield from redis.delete(job_key(id))
        # TODO: enqueue dependents
        return
    # TODO: set result
    fields = ('status', JobStatus.FINISHED,
              'ended_at', utcformat(utcnow()))
    score = result_ttl if result_ttl < 0 else current_timestamp() + result_ttl
    multi = redis.multi_exec()
    multi.zrem(started_registry(queue), id)
    multi.zadd(finished_registry(queue), score, id)
    multi.hmset(job_key(id), *fields)
    if result_ttl == -1:
        multi.persist(job_key(id))
    else:
        multi.expire(job_key(id), result_ttl)
    yield from multi.execute()
    while True:
        job_id = yield from redis.spop(dependents(id))
        if not job_id:
            break
        job_id = job_id.decode()
        origin = (yield from redis.hget(job_key(job_id), 'origin')).decode()
        multi = redis.multi_exec()
        multi.zrem(deferred_registry(origin), job_id)
        multi.rpush(queue_key(origin), job_id)
        multi.hmset(job_key(job_id),
                    'status', JobStatus.QUEUED,
                    'enqueued_at', utcformat(utcnow()))
        yield from multi.execute()


@asyncio.coroutine
def fail_job(redis, queue, id, exc_info):
    """Puts the given job in failed queue.

    :type redis: `aioredis.Redis`
    :type queue: str
    :type id: str
    :type exc_info: str

    """

    multi = redis.multi_exec()
    multi.sadd(queues_key(), failed_queue_key())
    multi.rpush(failed_queue_key(), id)
    fields = ('status', JobStatus.FAILED,
              'ended_at', utcformat(utcnow()),
              'exc_info', exc_info)
    multi.hmset(job_key(id), *fields)
    multi.zrem(started_registry(queue), id)
    yield from multi.execute()


@asyncio.coroutine
def requeue_job(redis, id):
    """Requeue job with the given job ID.

    :type redis: `aioredis.Redis`
    :type id: str

    """

    job = yield from redis.hgetall(job_key(id))  # origin only?
    is_failed_job = yield from redis.lrem(failed_queue_key(), 1, id)
    if not job:
        return
    if not is_failed_job:
        raise InvalidOperationError('Cannot requeue non-failed job')
    multi = redis.multi_exec()
    multi.hset(job_key(id), 'status', JobStatus.QUEUED)
    multi.hdel(job_key(id), 'exc_info')
    multi.rpush(queue_key(job[b'origin'].decode()), id)
    yield from multi.execute()


@asyncio.coroutine
def workers(redis):
    """Worker keys.

    :type redis: `aioredis.Redis`

    """

    return (yield from redis.smembers(workers_key()))


@asyncio.coroutine
def worker_birth(redis, id, queues, ttl=None):
    """Register worker birth.

    :type redis: `aioredis.Redis`
    :type id: str
    :type queues: list
    :type ttl: int or None

    """

    if (yield from redis.exists(worker_key(id))):
        if not (yield from redis.hexists(worker_key(id), 'death')):
            msg = 'There exists an active worker named {!r} already'.format(id)
            raise ValueError(msg)
    multi = redis.multi_exec()
    multi.delete(worker_key(id))
    multi.hmset(worker_key(id),
                'birth', utcformat(utcnow()),
                'queues', ','.join(queues),
                'status', WorkerStatus.STARTED,
                'current_job', 'not supported')
    multi.expire(worker_key(id), ttl or 420)
    multi.sadd(workers_key(), worker_key(id))
    yield from multi.execute()


@asyncio.coroutine
def worker_death(redis, id):
    """Register worker death.

    :type redis: `aioredis.Redis`
    :type id: str

    """

    multi = redis.multi_exec()
    multi.srem(workers_key(), worker_key(id))
    multi.hmset(worker_key(id),
                'death', utcformat(utcnow()),
                'status', WorkerStatus.IDLE)
    multi.expire(worker_key(id), 60)
    yield from multi.execute()


@asyncio.coroutine
def worker_shutdown_requested(redis, id):
    """Set worker shutdown requested date.

    :type redis: `aioredis.Redis`
    :type id: str

    """

    yield from redis.hset(
        worker_key(id), 'shutdown_requested_date', utcformat(utcnow()))
