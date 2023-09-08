from distributed_tasks.celery import run_task, signature


def special_function(a: int, b: int, c: str):
    return str(a + b) + c


def test_create_task(celery_app, celery_worker):
    s = signature(special_function, celery_app, 1, 2, 'c').delay()
    assert s.get(timeout=10) == '3c'
