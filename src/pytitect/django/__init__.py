"""Explicit Django adapters. Importing this package does not inspect settings."""

from pytitect.django.checks import register_checks
from pytitect.django.leases import DjangoFencedCommit
from pytitect.django.maintenance import (
    DjangoRetentionMaintenance,
    DurableOutboxArchive,
    RetentionIndexModels,
    build_retention_index_check,
)
from pytitect.django.stores import (
    DjangoCheckpointStore,
    DjangoGenerationStore,
    DjangoIdempotencyStore,
    DjangoInboxStore,
    DjangoLeaseStore,
    DjangoMutationBatchStore,
    DjangoOutboxStore,
    DjangoReceiptStore,
    DjangoReplayStore,
)
from pytitect.django.transactions import (
    DjangoTransactionalOperation,
    DjangoTransactionBoundary,
    TransactionalOperationCommitted,
    TransactionalOperationRolledBack,
)

__all__ = [
    "DjangoCheckpointStore",
    "DjangoFencedCommit",
    "DjangoGenerationStore",
    "DjangoIdempotencyStore",
    "DjangoInboxStore",
    "DjangoLeaseStore",
    "DjangoMutationBatchStore",
    "DjangoOutboxStore",
    "DjangoReceiptStore",
    "DjangoReplayStore",
    "DjangoRetentionMaintenance",
    "DjangoTransactionBoundary",
    "DjangoTransactionalOperation",
    "DurableOutboxArchive",
    "RetentionIndexModels",
    "TransactionalOperationCommitted",
    "TransactionalOperationRolledBack",
    "build_retention_index_check",
    "register_checks",
]
