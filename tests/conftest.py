import pytest
import redis
from rq import Connection, Worker

from distributed_tasks.rq import Queue


@pytest.fixture
def redis_connection():
    yield redis.Redis()


@pytest.fixture
def rq_worker(rq_queue, redis_connection):
    yield Worker([rq_queue], connection=redis_connection)


@pytest.fixture
def rq_queue(redis_connection):
    yield Queue(connection=redis_connection)
