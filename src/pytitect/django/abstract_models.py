"""Consumer-owned abstract PostgreSQL model shapes.

Import this module only after Django is configured. Pytitect ships no concrete model,
migration, table name, or application registration. Consumers must add the documented
unique constraints to their concrete subclasses.
"""

from __future__ import annotations

from datetime import datetime

from django.db import models


class AbstractIdempotencyModel(models.Model):
    namespace: models.CharField[str, str] = models.CharField(max_length=255)
    subject: models.CharField[str, str] = models.CharField(max_length=255)
    operation: models.CharField[str, str] = models.CharField(max_length=255)
    idempotency_key: models.CharField[str, str] = models.CharField(max_length=255)
    fingerprint: models.CharField[str, str] = models.CharField(max_length=64)
    reservation_token: models.CharField[str, str] = models.CharField(max_length=64)
    state: models.CharField[str, str] = models.CharField(max_length=32)
    expires_at: models.DateTimeField[datetime, datetime] = models.DateTimeField()
    value: models.JSONField[object, object] = models.JSONField(null=True)
    uncertainty_reason: models.TextField[str, str] = models.TextField(null=True)
    updated_at: models.DateTimeField[datetime, datetime] = models.DateTimeField()

    class Meta:
        abstract = True


class AbstractReplayModel(models.Model):
    namespace: models.CharField[str, str] = models.CharField(max_length=255)
    digest: models.CharField[str, str] = models.CharField(max_length=64)
    expires_at: models.DateTimeField[datetime, datetime] = models.DateTimeField()

    class Meta:
        abstract = True


class AbstractInboxModel(models.Model):
    message_id: models.CharField[str, str] = models.CharField(max_length=255)
    reservation_token: models.CharField[str, str] = models.CharField(max_length=255)
    expires_at: models.DateTimeField[datetime, datetime] = models.DateTimeField()
    completed_at: models.DateTimeField[datetime, datetime] = models.DateTimeField(null=True)

    class Meta:
        abstract = True


class AbstractOutboxModel(models.Model):
    message_id: models.CharField[str, str] = models.CharField(max_length=255)
    topic: models.CharField[str, str] = models.CharField(max_length=255)
    payload: models.JSONField[object, object] = models.JSONField()
    occurred_at: models.DateTimeField[datetime, datetime] = models.DateTimeField()
    available_at: models.DateTimeField[datetime, datetime] = models.DateTimeField()
    attempt: models.PositiveIntegerField[int, int] = models.PositiveIntegerField(default=0)
    claim_id: models.CharField[str, str] = models.CharField(max_length=64, null=True)
    claimed_until: models.DateTimeField[datetime, datetime] = models.DateTimeField(null=True)
    delivered_at: models.DateTimeField[datetime, datetime] = models.DateTimeField(null=True)
    failure_reason: models.TextField[str, str] = models.TextField(null=True)

    class Meta:
        abstract = True


class AbstractCheckpointModel(models.Model):
    stream: models.CharField[str, str] = models.CharField(max_length=255)
    checkpoint: models.BinaryField[bytes, bytes] = models.BinaryField()

    class Meta:
        abstract = True


class AbstractReceiptModel(models.Model):
    receipt_id: models.CharField[str, str] = models.CharField(max_length=255)
    kind: models.CharField[str, str] = models.CharField(max_length=32)
    state: models.CharField[str, str] = models.CharField(max_length=32)
    created_at: models.DateTimeField[datetime, datetime] = models.DateTimeField()
    updated_at: models.DateTimeField[datetime, datetime] = models.DateTimeField()
    result: models.JSONField[object, object] = models.JSONField(null=True)
    metadata: models.JSONField[object, object] = models.JSONField(default=dict)

    class Meta:
        abstract = True


class AbstractLeaseAuthorityModel(models.Model):
    resource_key: models.CharField[str, str] = models.CharField(max_length=255)
    owner: models.CharField[str, str] = models.CharField(max_length=255, null=True)
    fencing_token: models.PositiveBigIntegerField[int, int] = models.PositiveBigIntegerField(
        default=0
    )
    expires_at: models.DateTimeField[datetime, datetime] = models.DateTimeField(null=True)

    class Meta:
        abstract = True


class AbstractGenerationModel(models.Model):
    dataset: models.CharField[str, str] = models.CharField(max_length=255)
    partition: models.CharField[str, str] = models.CharField(max_length=255)
    generation: models.PositiveBigIntegerField[int, int] = models.PositiveBigIntegerField()

    class Meta:
        abstract = True
