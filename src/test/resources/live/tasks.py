import os

from celery import Celery

app = Celery(
    "tasks",
    broker=os.environ["BROKER_URL"],
    backend=os.environ.get("RESULT_URL", os.environ["BROKER_URL"]),
)


@app.task(name="proj.tasks.add")
def add(left, right):
    return left + right


@app.task(name="proj.tasks.boom")
def boom():
    raise ValueError("boom")
