from django.db import models

from pytitect.django.abstract_models import (
    AbstractIdempotencyModel,
    AbstractOutboxModel,
    AbstractReceiptModel,
)


class IdempotencyRecord(AbstractIdempotencyModel):
    class Meta:
        indexes = [models.Index(fields=["state", "expires_at"], name="ref_idem_retention_idx")]
        constraints = [
            models.UniqueConstraint(
                fields=["namespace", "subject", "operation", "idempotency_key"],
                name="ref_idempotency_identity",
            )
        ]


class ReceiptRecord(AbstractReceiptModel):
    class Meta:
        indexes = [models.Index(fields=["state", "updated_at"], name="ref_receipt_retention_idx")]
        constraints = [models.UniqueConstraint(fields=["receipt_id"], name="ref_receipt_identity")]


class OutboxRecord(AbstractOutboxModel):
    class Meta:
        indexes = [
            models.Index(fields=["delivered_at"], name="ref_outbox_delivered_idx"),
            models.Index(fields=["failed_at"], name="ref_outbox_failed_idx"),
        ]
        constraints = [models.UniqueConstraint(fields=["message_id"], name="ref_outbox_identity")]


class SyntheticMutation(models.Model):
    mutation_id = models.CharField(max_length=255, unique=True)
    value = models.IntegerField()
    created_at = models.DateTimeField()


class FailedOutboxArchive(models.Model):
    message_id = models.CharField(max_length=255, unique=True)
    topic = models.CharField(max_length=255)
    payload = models.JSONField()
    occurred_at = models.DateTimeField()
    available_at = models.DateTimeField()
    attempt = models.PositiveIntegerField()
    failure_reason = models.TextField()
    failed_at = models.DateTimeField()
