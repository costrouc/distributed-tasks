import typing

import cloudpickle
from celery import shared_task


@shared_task(bind=True, name="run_task")
def run_task(self, pickled_function: bytes, *args, **kwargs):
    """Flexible Celery job which runs loads given cloudpickle dump and executed with given args and kwargs

    """
    f = cloudpickle.loads(pickled_function)
    return f(*args, **kwargs)


def signature(f: typing.Callable, app, *args, **kwargs):
    pickled_function = cloudpickle.dumps(f)
    return app.signature('run_task', args=(pickled_function, *args), kwargs=kwargs)
