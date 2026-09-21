FROM docker.io/library/python:3.12-slim

RUN pip install --no-cache-dir "celery[redis]==5.4.0" "pyyaml==6.0.2" \
 && useradd --create-home --uid 1000 worker

COPY tasks.py /app/tasks.py
COPY producer.py /app/producer.py
COPY schedule.py /app/schedule.py
COPY serialize.py /app/serialize.py
COPY read.py /app/read.py
COPY workflow.py /app/workflow.py
COPY header.py /app/header.py
WORKDIR /app
USER worker

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s \
  CMD celery -A tasks inspect ping || exit 1

CMD ["celery", "-A", "tasks", "worker", "--loglevel=info", "--concurrency=1", "-Q", "interop"]
