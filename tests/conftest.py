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


@pytest.fixture(scope='session')
def celery_config():
    return {
        'broker_url': 'redis://localhost:6379',
        'result_backend': 'redis://localhost:6379'
    }
