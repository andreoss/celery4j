FROM docker.io/library/python:3.12-slim

RUN pip install --no-cache-dir "celery[redis]==5.4.0" "pyyaml==6.0.2"

COPY tasks.py /app/tasks.py
COPY producer.py /app/producer.py
COPY schedule.py /app/schedule.py
COPY serialize.py /app/serialize.py
WORKDIR /app

CMD ["celery", "-A", "tasks", "worker", "--loglevel=info", "--concurrency=1", "-Q", "interop"]
