import os
import time

from celery import Celery

app = Celery(
    "tasks",
    broker=os.environ["BROKER_URL"],
    backend=os.environ.get("RESULT_URL", os.environ["BROKER_URL"]),
)
app.conf.task_track_started = True
app.conf.accept_content = ["json", "yaml"]


@app.task(name="proj.tasks.add")
def add(left, right):
    return left + right


@app.task(name="proj.tasks.boom")
def boom():
    raise ValueError("boom")


@app.task(name="proj.tasks.echo")
def echo(*args, **kwargs):
    if kwargs:
        return {"args": list(args), "kwargs": kwargs}
    return list(args)


@app.task(name="proj.tasks.give")
def give(kind):
    return {
        "whole": 7,
        "fraction": 2.5,
        "text": "héllo wörld",
        "yes": True,
        "no": False,
        "nothing": None,
        "list": [1, "two", 3.5],
        "map": {"a": 1, "b": [2, 3], "c": {"d": "e"}},
    }[kind]


@app.task(name="proj.tasks.slow")
def slow(seconds):
    time.sleep(seconds)
    return seconds


@app.task(name="proj.tasks.flaky", bind=True, max_retries=1)
def flaky(self):
    raise self.retry(countdown=30)


@app.task(name="proj.tasks.headers", bind=True)
def headers(self, name):
    return self.request.get(name)
