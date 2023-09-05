import cloudpickle
from rq import get_current_job


def convert_dask_delayed_to_rq(f, queue):
    last_job = None
    jobs = {}

    ordered_tasks = f.dask._toposort_layers()
    print('num_tasks', len(ordered_tasks))
    for task_id in ordered_tasks:
        depends_on = []
        for dependency in f.dask.dependencies[task_id]:
            depends_on.append(jobs[dependency])

        _args = []
        for arg in f.dask[task_id][1:]:
            if arg in jobs:
                _args.append(f'task-{arg}')
            else:
                _args.append(arg)

        job = task_enqueue(
            queue,
            f.dask[task_id][0],
            *_args,
            depends_on=depends_on,
            job_id=task_id
        )
        jobs[task_id] = job
        last_job = job
    return last_job


def run_task(pickled_function: bytes, *args, **kwargs):
    current_job = get_current_job()
    dependencies = {}
    for dependency in current_job.fetch_dependencies():
        dependencies[dependency.id] = dependency

    _args = []
    for arg in args:
        if isinstance(arg, str) and arg.startswith('task-'):
            _args.append(dependencies[arg[5:]].result)
        else:
            _args.append(arg)

    f = cloudpickle.loads(pickled_function)
    return f(*_args, **kwargs)


def task_enqueue(queue, f, *args, **kwargs):
    pickled_function = cloudpickle.dumps(f)
    job = queue.enqueue(run_task, pickled_function, *args, **kwargs)
    return job
