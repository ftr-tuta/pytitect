"""Explicit Django adapters. Importing this package does not inspect settings."""

from pytitect.django.checks import register_checks
from pytitect.django.leases import DjangoFencedCommit
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
    "DjangoTransactionBoundary",
    "DjangoTransactionalOperation",
    "TransactionalOperationCommitted",
    "TransactionalOperationRolledBack",
    "register_checks",
]
