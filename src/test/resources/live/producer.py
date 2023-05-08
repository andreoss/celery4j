import os
import sys

from celery import Celery

app = Celery(
    "producer",
    broker=os.environ["BROKER_URL"],
    backend=os.environ["BROKER_URL"],
)

app.send_task(
    sys.argv[2],
    args=[2, 3],
    kwargs={"debug": True},
    task_id=sys.argv[1],
    queue=sys.argv[3],
)
