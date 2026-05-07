from celery import Celery

from src.config import settings

celery_app = Celery(
    "voicebot",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    # Two priority queues replacing the single "postcall_processing" queue.
    # Hot tasks have higher Celery priority within the broker.
    task_default_queue=settings.POSTCALL_COLD_QUEUE,
    task_queues={
        settings.POSTCALL_HOT_QUEUE: {
            "exchange": settings.POSTCALL_HOT_QUEUE,
            "routing_key": settings.POSTCALL_HOT_QUEUE,
        },
        settings.POSTCALL_COLD_QUEUE: {
            "exchange": settings.POSTCALL_COLD_QUEUE,
            "routing_key": settings.POSTCALL_COLD_QUEUE,
        },
    },
    task_routes={
        "process_hot_interaction": {"queue": settings.POSTCALL_HOT_QUEUE},
        "process_cold_interaction": {"queue": settings.POSTCALL_COLD_QUEUE},
    },
)
