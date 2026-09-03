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


class AbstractMutationBatchModel(models.Model):
    namespace: models.CharField[str, str] = models.CharField(max_length=255)
    batch_id: models.CharField[str, str] = models.CharField(max_length=255)
    fingerprint: models.CharField[str, str] = models.CharField(max_length=64)
    reservation_token: models.CharField[str, str] = models.CharField(max_length=64)
    state: models.CharField[str, str] = models.CharField(max_length=32)
    total_items: models.PositiveIntegerField[int, int] = models.PositiveIntegerField()
    next_index: models.PositiveIntegerField[int, int] = models.PositiveIntegerField(default=0)
    receipts: models.JSONField[object, object] = models.JSONField(default=list)
    lease_expires_at: models.DateTimeField[datetime, datetime] = models.DateTimeField()
    retention_expires_at: models.DateTimeField[datetime, datetime] = models.DateTimeField(null=True)
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
    namespace: models.CharField[str, str] = models.CharField(max_length=255)
    source: models.CharField[str, str] = models.CharField(max_length=255)
    consumer: models.CharField[str, str] = models.CharField(max_length=255)
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
    failed_at: models.DateTimeField[datetime, datetime] = models.DateTimeField(null=True)

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


class AbstractProcessManagerModel(models.Model):
    process_name: models.CharField[str, str] = models.CharField(max_length=255)
    instance_id: models.CharField[str, str] = models.CharField(max_length=255)
    version: models.PositiveBigIntegerField[int, int] = models.PositiveBigIntegerField()
    status: models.CharField[str, str] = models.CharField(max_length=32)
    state: models.JSONField[object, object] = models.JSONField()
    updated_at: models.DateTimeField[datetime, datetime] = models.DateTimeField()

    class Meta:
        abstract = True


class AbstractProcessTimerModel(models.Model):
    process_name: models.CharField[str, str] = models.CharField(max_length=255)
    instance_id: models.CharField[str, str] = models.CharField(max_length=255)
    timer_id: models.CharField[str, str] = models.CharField(max_length=255)
    due_at: models.DateTimeField[datetime, datetime] = models.DateTimeField()
    effect_id: models.CharField[str, str] = models.CharField(max_length=255)
    effect_kind: models.CharField[str, str] = models.CharField(max_length=32)
    effect_name: models.CharField[str, str] = models.CharField(max_length=255)
    effect_payload: models.JSONField[object, object] = models.JSONField()
    claim_id: models.CharField[str, str] = models.CharField(max_length=64, null=True)
    claimed_until: models.DateTimeField[datetime, datetime] = models.DateTimeField(null=True)
    fencing_token: models.PositiveBigIntegerField[int, int] = models.PositiveBigIntegerField(
        default=0
    )

    class Meta:
        abstract = True


class AbstractJobModel(models.Model):
    job_id: models.CharField[str, str] = models.CharField(max_length=255)
    task: models.CharField[str, str] = models.CharField(max_length=255)
    payload: models.JSONField[object, object] = models.JSONField()
    run_at: models.DateTimeField[datetime, datetime] = models.DateTimeField()
    attempt: models.PositiveIntegerField[int, int] = models.PositiveIntegerField(default=0)
    max_attempts: models.PositiveIntegerField[int, int] = models.PositiveIntegerField()
    state: models.CharField[str, str] = models.CharField(max_length=32)
    claim_id: models.CharField[str, str] = models.CharField(max_length=64, null=True)
    claimed_until: models.DateTimeField[datetime, datetime] = models.DateTimeField(null=True)
    fencing_token: models.PositiveBigIntegerField[int, int] = models.PositiveBigIntegerField(
        default=0
    )
    last_failure: models.TextField[str, str] = models.TextField(null=True)

    class Meta:
        abstract = True


class AbstractJobScheduleModel(models.Model):
    schedule_id: models.CharField[str, str] = models.CharField(max_length=255)
    task: models.CharField[str, str] = models.CharField(max_length=255)
    payload: models.JSONField[object, object] = models.JSONField()
    next_run: models.DateTimeField[datetime, datetime] = models.DateTimeField()
    kind: models.CharField[str, str] = models.CharField(max_length=32)
    interval_seconds: models.PositiveBigIntegerField[int, int] = models.PositiveBigIntegerField(
        null=True
    )
    policy: models.CharField[str, str] = models.CharField(max_length=255, null=True)
    sequence: models.PositiveBigIntegerField[int, int] = models.PositiveBigIntegerField(default=0)
    active: models.BooleanField[bool, bool] = models.BooleanField(default=True)

    class Meta:
        abstract = True


class AbstractProjectionModel(models.Model):
    projection_name: models.CharField[str, str] = models.CharField(max_length=255)
    partition: models.CharField[str, str] = models.CharField(max_length=255)
    version: models.PositiveBigIntegerField[int, int] = models.PositiveBigIntegerField()
    checkpoint: models.PositiveBigIntegerField[int, int] = models.PositiveBigIntegerField(default=0)
    state: models.JSONField[object, object] = models.JSONField()

    class Meta:
        abstract = True


class AbstractProjectionRebuildModel(models.Model):
    run_id: models.CharField[str, str] = models.CharField(max_length=255)
    projection_name: models.CharField[str, str] = models.CharField(max_length=255)
    partition: models.CharField[str, str] = models.CharField(max_length=255)
    projection_version: models.PositiveBigIntegerField[int, int] = models.PositiveBigIntegerField()
    through_position: models.PositiveBigIntegerField[int, int] = models.PositiveBigIntegerField()
    batch_size: models.PositiveIntegerField[int, int] = models.PositiveIntegerField()
    next_position: models.PositiveBigIntegerField[int, int] = models.PositiveBigIntegerField(
        default=0
    )
    state: models.JSONField[object, object] = models.JSONField()
    status: models.CharField[str, str] = models.CharField(max_length=32)

    class Meta:
        abstract = True


class AbstractEventModel(models.Model):
    category: models.CharField[str, str] = models.CharField(max_length=255)
    stream_id: models.CharField[str, str] = models.CharField(max_length=255)
    stream_version: models.PositiveBigIntegerField[int, int] = models.PositiveBigIntegerField()
    global_position: models.PositiveBigIntegerField[int, int] = models.PositiveBigIntegerField()
    event_id: models.CharField[str, str] = models.CharField(max_length=255)
    event_type: models.CharField[str, str] = models.CharField(max_length=255)
    payload: models.JSONField[object, object] = models.JSONField()
    occurred_at: models.DateTimeField[datetime, datetime] = models.DateTimeField()
    event_metadata: models.JSONField[object, object] = models.JSONField(default=dict)

    class Meta:
        abstract = True


class AbstractSnapshotModel(models.Model):
    category: models.CharField[str, str] = models.CharField(max_length=255)
    stream_id: models.CharField[str, str] = models.CharField(max_length=255)
    version: models.PositiveBigIntegerField[int, int] = models.PositiveBigIntegerField()
    state: models.JSONField[object, object] = models.JSONField()
    created_at: models.DateTimeField[datetime, datetime] = models.DateTimeField()

    class Meta:
        abstract = True
