import time
import functools
import operator
import contextlib

from rq import Queue
from redis import Redis
from dask import delayed

import tasks


@delayed
def add(*args):
    return sum(args)

@delayed
def multiply(*args):
    return functools.reduce(operator.mul, args, 1)


results = []
for i in range(2000):
    results.append(multiply(1, 2))
f = multiply(add(*results), 5, 10)


@contextlib.contextmanager
def timer(message: str):
    start_time = time.time()
    yield
    print(f'{message} took {time.time() - start_time:.2f} [s]')


redis_conn = Redis()
queue = Queue(connection=redis_conn)

with timer('submitting jobs'):
    job = tasks.convert_dask_delayed_to_rq(f, queue)

with timer('jobs finished'):
    while job.get_status() != 'finished':
        time.sleep(0.1)
print('result', job.result)
