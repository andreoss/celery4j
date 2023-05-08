FROM docker.io/library/python:3.12-slim

RUN pip install --no-cache-dir "celery[redis]==5.4.0"

COPY tasks.py /app/tasks.py
COPY producer.py /app/producer.py
WORKDIR /app

CMD ["celery", "-A", "tasks", "worker", "--loglevel=info", "--concurrency=1", "-Q", "interop"]
