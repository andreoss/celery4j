import os
import sys

from celery import Celery, chain, group

app = Celery(
    "workflow",
    broker=os.environ["BROKER_URL"],
    backend=os.environ.get("RESULT_URL", os.environ["BROKER_URL"]),
)

queue = sys.argv[2]
name = "proj.java.echo"

if sys.argv[1] == "chain":
    chain(
        app.signature(name, args=[1], queue=queue),
        app.signature(name, args=[2], queue=queue),
    ).apply_async()
elif sys.argv[1] == "group":
    group(
        app.signature(name, args=[1], queue=queue),
        app.signature(name, args=[2], queue=queue),
    ).apply_async()
elif sys.argv[1] == "error":
    app.send_task(
        "proj.java.boom",
        args=[5],
        queue=queue,
        link_error=app.signature(name, args=[], queue=queue),
    )
else:
    app.send_task(
        name,
        args=[3],
        queue=queue,
        link=app.signature(name, args=[4], queue=queue),
    )
