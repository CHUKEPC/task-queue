"""Celery application instance configuration and queue topology."""

from celery import Celery
from kombu import Exchange, Queue

from task_queue.config import get_settings

settings = get_settings()

celery_app = Celery(
    "task_queue",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
    include=[
        "task_queue.tasks.email",
        "task_queue.tasks.report",
        "task_queue.tasks.data_processing",
    ],
)

default_exchange = Exchange("tasks", type="direct")

# Queue topology supporting priority-based scheduling
task_queues = (
    Queue(
        "critical",
        exchange=default_exchange,
        routing_key="critical",
        queue_arguments={"x-max-priority": 9},
    ),
    Queue(
        "high",
        exchange=default_exchange,
        routing_key="high",
        queue_arguments={"x-max-priority": 6},
    ),
    Queue(
        "medium",
        exchange=default_exchange,
        routing_key="medium",
        queue_arguments={"x-max-priority": 3},
    ),
    Queue(
        "low",
        exchange=default_exchange,
        routing_key="low",
        queue_arguments={"x-max-priority": 0},
    ),
    Queue(
        "celery",
        exchange=default_exchange,
        routing_key="celery",
    ),
)

celery_app.conf.update(
    task_queues=task_queues,
    task_default_queue=settings.celery_task_default_queue,
    task_default_exchange="tasks",
    task_default_routing_key="celery",
    # Enable Redis priority transport options
    broker_transport_options={
        "priority_steps": list(range(10)),
        "sep": ":",
        "queue_order_strategy": "priority",
    },
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    result_expires=settings.result_expires_seconds,
    task_always_eager=settings.celery_task_always_eager,
    task_eager_propagates=True,
)
