from rq import Queue
from redis import Redis

import tasks
import time
import contextlib

@contextlib.contextmanager
def timer(message: str):
    start_time = time.time()
    yield
    print(f'{message} took {time.time() - start_time:.2f} [s]')


redis_conn = Redis()
queue = Queue(connection=redis_conn)

with timer('submitting tasks'):
    def special_function(a: int, b: int, c: str):
        return str(a + b) + c

    job = tasks.task_enqueue(queue, special_function, 1, 2, 'a')


with timer('jobs finished'):
    while job.get_status() != 'finished':
        time.sleep(0.1)
print('result', job.result)
