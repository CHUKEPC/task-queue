"""Unit tests for the email delivery Celery task."""

import pytest
from celery.exceptions import Retry

from task_queue.tasks.email import send_email_task


def test_send_email_task_success() -> None:
    """Verify that email task processes correctly with valid input."""
    result = send_email_task.apply(
        kwargs={
            "recipient": "user@example.com",
            "subject": "Welcome!",
            "body": "Hello world",
            "template_name": "welcome_v1",
            "delay_seconds": 0.0,
        }
    ).get()

    assert result["status"] == "delivered"
    assert result["recipient"] == "user@example.com"
    assert result["subject"] == "Welcome!"
    assert result["template_name"] == "welcome_v1"
    assert "message_id" in result
    assert result["bytes_sent"] == len(b"Hello world")
    assert "delivered_at" in result


def test_send_email_task_invalid_email() -> None:
    """Verify that malformed email addresses raise ValueError without retry."""
    with pytest.raises(ValueError, match="Invalid email address"):
        send_email_task.apply(
            kwargs={
                "recipient": "invalid-address-without-at",
                "subject": "Test",
                "body": "Body",
                "delay_seconds": 0.0,
            }
        ).get()


def test_send_email_task_simulate_failure() -> None:
    """Verify that simulate_failure flag triggers retry policy."""
    # 1. Direct execution raises underlying RuntimeError
    with pytest.raises(RuntimeError, match="SMTP relay connection failure"):
        send_email_task.run(
            recipient="fail@example.com",
            subject="Test Failure",
            body="Content",
            simulate_failure=True,
            delay_seconds=0.0,
        )

    # 2. Celery apply triggers Retry exception
    with pytest.raises(Retry):
        send_email_task.apply(
            kwargs={
                "recipient": "fail@example.com",
                "subject": "Test Failure",
                "body": "Content",
                "simulate_failure": True,
                "delay_seconds": 0.0,
            }
        ).get()
