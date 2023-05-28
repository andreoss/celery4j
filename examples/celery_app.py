"""The same app in Python. Run `celery -A celery_app worker` to consume."""
import os
import sys

from celery import Celery

app = Celery(
    "examples",
    broker=os.environ.get("REDIS_ADDR", "redis://localhost:6379/0"),
    backend=os.environ.get("REDIS_ADDR", "redis://localhost:6379/0"),
)


@app.task(name="examples.add")
def add(left, right):
    return left + right


if __name__ == "__main__":
    print(add.delay(2, 3).get(timeout=10) if sys.argv[1:2] == ["produce"] else app)
