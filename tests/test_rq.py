import time

from distributed_tasks.rq import convert_dask_delayed_to_rq_jobs
import dask


def special_function(a: int, b: int, c: str):
    return str(a + b) + c


def test_basic_task(rq_worker, rq_queue):
    job = rq_queue.enqueue(special_function, 1, 2, 'a')
    rq_worker.work(burst=True)

    assert job.is_finished
    assert job.return_value() == '3a'


def test_basic_task_raising(rq_worker, rq_queue):
    job = rq_queue.enqueue(special_function, 1, 2, 3)
    rq_worker.work(burst=True)

    assert job.is_failed
    assert 'TypeError' in job.latest_result().exc_string


def test_dask_tasks(rq_worker, rq_queue):
    @dask.delayed
    def add(*args):
        return sum(args)

    N = 10
    f = add(*[add(1, i) for i in range(N)])

    job = rq_queue.enqueue(f)
    rq_worker.work(burst=True)

    assert job.is_finished
    assert job.return_value() == N + N*(N-1) / 2
