"""Email dispatch background task simulating network transport and retry behavior."""

import logging
import time
import uuid
from datetime import UTC, datetime
from typing import Any

from task_queue.celery_app import celery_app
from task_queue.tasks.base import BaseTask

logger = logging.getLogger(__name__)


@celery_app.task(base=BaseTask, bind=True, name="tasks.send_email")
def send_email_task(
    self: Any,
    recipient: str,
    subject: str,
    body: str,
    template_name: str | None = None,
    simulate_failure: bool = False,
    delay_seconds: float = 0.2,
) -> dict[str, Any]:
    """Execute simulated transactional email delivery.

    Args:
        self: Bound Celery task context.
        recipient: Target recipient email address.
        subject: Email subject headline.
        body: Text content of the email.
        template_name: Optional template layout key.
        simulate_failure: If True, forces an exception to exercise retry policies.
        delay_seconds: Simulated network round-trip delay.

    Returns:
        dict[str, Any]: Delivery confirmation payload.

    Raises:
        ValueError: If email format is malformed.
        RuntimeError: If simulate_failure is enabled.
    """
    logger.info("Executing email task %s to %s", self.request.id, recipient)

    if "@" not in recipient:
        raise ValueError(f"Invalid email address: {recipient}")

    if delay_seconds > 0:
        time.sleep(min(delay_seconds, 5.0))

    if simulate_failure:
        logger.warning(
            "Simulated failure triggered for email to %s (attempt %d)",
            recipient,
            self.request.retries,
        )
        raise RuntimeError(
            f"SMTP relay connection failure to {recipient} (retry {self.request.retries})"
        )

    message_id = f"msg_{uuid.uuid4().hex[:12]}"
    result = {
        "status": "delivered",
        "recipient": recipient,
        "subject": subject,
        "template_name": template_name,
        "message_id": message_id,
        "bytes_sent": len(body.encode("utf-8")),
        "delivered_at": datetime.now(UTC).isoformat(),
    }
    logger.info("Email %s successfully delivered to %s", message_id, recipient)
    return result
