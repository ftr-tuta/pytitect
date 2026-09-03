from django.db import models
from pytitect.django.abstract_models import AbstractCheckpointModel
from pytitect.django.abstract_models import AbstractGenerationModel
from pytitect.django.abstract_models import AbstractIdempotencyModel
from pytitect.django.abstract_models import AbstractInboxModel
from pytitect.django.abstract_models import AbstractLeaseAuthorityModel
from pytitect.django.abstract_models import AbstractMutationBatchModel
from pytitect.django.abstract_models import AbstractOutboxModel
from pytitect.django.abstract_models import AbstractReceiptModel
from pytitect.django.abstract_models import AbstractReplayModel


class IdempotencyRecord(AbstractIdempotencyModel):
    class Meta:
        indexes = [
            models.Index(
                fields=["state", "expires_at"], name="mv2_idem_retention_idx",
            ),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["namespace", "subject", "operation", "idempotency_key"],
                name="mobile_v2_idempotency_identity",
            ),
        ]


class MutationBatchRecord(AbstractMutationBatchModel):
    class Meta:
        indexes = [
            models.Index(
                fields=["state", "retention_expires_at"],
                name="mv2_batch_retention_idx",
            ),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["namespace", "batch_id"],
                name="mobile_v2_mutation_batch_identity",
            ),
        ]


class ReplayRecord(AbstractReplayModel):
    class Meta:
        indexes = [
            models.Index(fields=["expires_at"], name="mv2_replay_retention_idx"),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["namespace", "digest"], name="mobile_v2_replay_identity",
            ),
        ]


class InboxRecord(AbstractInboxModel):
    class Meta:
        indexes = [
            models.Index(fields=["completed_at"], name="mv2_inbox_completed_idx"),
            models.Index(fields=["expires_at"], name="mv2_inbox_expires_idx"),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["namespace", "source", "consumer", "message_id"],
                name="mobile_v2_inbox_identity",
            ),
        ]


class OutboxRecord(AbstractOutboxModel):
    class Meta:
        indexes = [
            models.Index(fields=["delivered_at"], name="mv2_outbox_delivered_idx"),
            models.Index(fields=["failed_at"], name="mv2_outbox_failed_idx"),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["message_id"], name="mobile_v2_outbox_identity",
            ),
        ]


class CheckpointRecord(AbstractCheckpointModel):
    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["stream"], name="mobile_v2_checkpoint_identity",
            ),
        ]


class ReceiptRecord(AbstractReceiptModel):
    class Meta:
        indexes = [
            models.Index(
                fields=["state", "updated_at"], name="mv2_receipt_retention_idx",
            ),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["receipt_id"], name="mobile_v2_receipt_identity",
            ),
        ]


class LeaseRecord(AbstractLeaseAuthorityModel):
    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["resource_key"], name="mobile_v2_lease_identity",
            ),
        ]


class GenerationRecord(AbstractGenerationModel):
    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["dataset", "partition"], name="mobile_v2_generation_identity",
            ),
        ]


class DomainMutation(models.Model):
    protocol = models.CharField(max_length=32)
    value = models.IntegerField()


class OutboxArchive(models.Model):
    message_id = models.CharField(max_length=255, unique=True)
    topic = models.CharField(max_length=255)
    payload = models.JSONField()
    occurred_at = models.DateTimeField()
    available_at = models.DateTimeField()
    attempt = models.PositiveIntegerField()
    failure_reason = models.TextField()
    failed_at = models.DateTimeField()
