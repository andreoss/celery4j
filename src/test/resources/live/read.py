import os
import sys

from celery import Celery

app = Celery(
    "read",
    broker=os.environ["BROKER_URL"],
    backend=os.environ.get("RESULT_URL", os.environ["BROKER_URL"]),
)

result = app.AsyncResult(sys.argv[1])
try:
    print("VALUE", result.get(timeout=20))
except Exception as failure:
    print("RAISED", type(failure).__name__, failure)
print("STATE", result.state)
