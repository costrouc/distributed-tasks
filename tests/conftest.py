import pytest
from rq import Connection, Worker

from distributed_tasks.rq import Queue


@pytest.fixture
def rq_worker(rq_queue):
    yield Worker(rq_queue)


@pytest.fixture
def rq_queue():
    with Connection():
        yield Queue()
