import os
import sys

from celery import Celery

app = Celery(
    "schedule",
    broker=os.environ["BROKER_URL"],
    backend=os.environ.get("RESULT_URL", os.environ["BROKER_URL"]),
)

options = {}
if sys.argv[4] != "-":
    options["countdown"] = float(sys.argv[4])
if sys.argv[5] != "-":
    options["expires"] = float(sys.argv[5])

app.send_task(
    sys.argv[2],
    args=[2, 3],
    task_id=sys.argv[1],
    queue=sys.argv[3],
    **options,
)
