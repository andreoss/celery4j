import os
import sys

from celery import Celery

app = Celery(
    "header",
    broker=os.environ["BROKER_URL"],
    backend=os.environ.get("RESULT_URL", os.environ["BROKER_URL"]),
)

app.send_task(
    sys.argv[2],
    args=[2, 3],
    task_id=sys.argv[1],
    queue=sys.argv[3],
    headers={"x-trace": "trace-of-the-other-side", "x-depth": 7},
)
