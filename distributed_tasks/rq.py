import typing

import cloudpickle
import rq
from dask.delayed import Delayed


def run_task(pickled_function: bytes, *args, **kwargs):
    """Flexible RQ job which runs loads given cloudpickle dump and executed with given args and kwargs

    """
    current_job = rq.get_current_job()
    dependencies = {}
    for dependency in current_job.fetch_dependencies():
        dependencies[dependency.id] = dependency

    _args = []
    for arg in args:
        if isinstance(arg, str) and arg.startswith('rq-job-'):
            _args.append(dependencies[arg[7:]].result)
        else:
            _args.append(arg)

    f = cloudpickle.loads(pickled_function)
    return f(*_args, **kwargs)


class Queue(rq.Queue):
    def enqueue(self, f: typing.Callable | Delayed, *args, **kwargs) -> rq.job.Job:
        """This is a superclass of RQ Queue which does not require
        that the function is available on workers by pickling the
        function and if the function is a DAG of tasks converts it
        into a collection of tasks

        """
        if isinstance(f, Delayed):
            job = convert_dask_delayed_to_rq_jobs(f, self)
            return job

        pickled_function = cloudpickle.dumps(f)
        return super().enqueue(run_task, pickled_function, *args, **kwargs)


def convert_dask_delayed_to_rq_jobs(f: Delayed, queue: Queue) -> rq.job.Job:
    """Take a given Dask deleyed object and schedule Jobs on RQ and return the root RQ Job

    """
    if not isinstance(queue, Queue):
        raise ValueError(f"queue must be an instance of {Queue.__module__}.{Queue.__name__}")

    last_job = None
    job_ids = dict()

    ordered_tasks = f.dask._toposort_layers()
    for task_id in ordered_tasks:
        depends_on = []
        for dependency in f.dask.dependencies[task_id]:
            depends_on.append(job_ids[dependency])

        _args = []
        for arg in f.dask[task_id][1:]:
            if arg in job_ids:
                _args.append(f'rq-job-{arg}')
            else:
                _args.append(arg)

        job = queue.enqueue(
            f.dask[task_id][0],
            *_args,
            depends_on=depends_on,
            job_id=task_id
        )
        job_ids[task_id] = job
        last_job = job
    return last_job
