from django.core.checks import register

from pytitect.django.maintenance import RetentionIndexModels, build_retention_index_check
from reference_app.models import IdempotencyRecord, OutboxRecord, ReceiptRecord

retention_indexes = register()(
    build_retention_index_check(
        RetentionIndexModels(
            idempotency=IdempotencyRecord,
            receipts=ReceiptRecord,
            outbox=OutboxRecord,
        )
    )
)
