"""Preview bounded synchronous operations for the explicit Django async bridge."""

from __future__ import annotations

import uuid
from collections.abc import Callable, Sequence
from datetime import datetime, timedelta
from typing import Any

from pytitect.aio.resilience import SettlementResult
from pytitect.core import JsonValue, OpaqueId
from pytitect.django.stores import DjangoOutboxStore, _outbox_envelope, _postgresql, _utc
from pytitect.outbox import OutboxAddResult, OutboxClaim, OutboxEnvelope


class DjangoRelayStore[PayloadT]:
    """Model-backed Preview operations; each call requires the caller transaction.

    Byte admission uses the PostgreSQL JSON representation, before decoding any
    payload. The consumer owns serialization and the model's unique message ID.
    """

    def __init__(
        self,
        model: Any,
        *,
        using: str,
        encode_payload: Callable[[PayloadT], JsonValue],
        decode_payload: Callable[[JsonValue], PayloadT],
    ) -> None:
        self._store = DjangoOutboxStore.from_model(
            model, using=using, encode_payload=encode_payload, decode_payload=decode_payload
        )
        self.model, self.using, self.decode = model, using, decode_payload

    def add(self, envelope: OutboxEnvelope[PayloadT]) -> OutboxAddResult:
        return self._store.add(envelope)

    def claim(
        self,
        *,
        now: datetime,
        limit: int,
        claim_ttl: timedelta,
        max_bytes: int | None = None,
    ) -> Sequence[OutboxClaim[PayloadT]]:
        from django.db.models import Func, IntegerField, Q, TextField
        from django.db.models.functions import Cast

        _postgresql(self.using)
        _utc(now)
        if isinstance(limit, bool) or limit <= 0 or claim_ttl <= timedelta(0):
            raise ValueError("positive claim limit and ttl required")
        if max_bytes is not None and (isinstance(max_bytes, bool) or max_bytes <= 0):
            raise ValueError("max_bytes must be positive")
        query = (
            self.model.objects.using(self.using)
            .select_for_update(skip_locked=True)
            .filter(
                delivered_at__isnull=True,
                uncertain_at__isnull=True,
                failure_reason__isnull=True,
                available_at__lte=now,
            )
            .filter(Q(claimed_until__isnull=True) | Q(claimed_until__lte=now))
            .order_by("available_at", "message_id")
        )
        metadata = list(
            query.annotate(
                payload_bytes=Func(
                    Cast("payload", TextField()),
                    function="octet_length",
                    output_field=IntegerField(),
                )
            ).values_list("message_id", "payload_bytes")[:limit]
        )
        retained, identities = 0, []
        for identity, size in metadata:
            if max_bytes is None or retained + size <= max_bytes:
                retained += size
                identities.append(identity)
        claims = []
        for row in query.filter(message_id__in=identities):
            row.claim_id, row.claimed_until = uuid.uuid4().hex, now + claim_ttl
            row.save(using=self.using, update_fields=["claim_id", "claimed_until"])
            claims.append(
                OutboxClaim(row.claim_id, _outbox_envelope(row, self.decode), row.claimed_until)
            )
        return claims

    def _settle(
        self, claim: OutboxClaim[PayloadT], at: datetime, **values: Any
    ) -> SettlementResult:
        from django.db.models import DateTimeField, Func
        from django.db.models.functions import Greatest

        _postgresql(self.using)
        _utc(at)
        self.model.objects.using(self.using).select_for_update().filter(
            message_id=str(claim.envelope.message_id)
        ).first()
        changed = (
            self.model.objects.using(self.using)
            .filter(
                message_id=str(claim.envelope.message_id),
                claim_id=claim.claim_id,
                claimed_until=claim.claimed_until,
                claimed_until__gt=Greatest(
                    at, Func(function="clock_timestamp", output_field=DateTimeField())
                ),
                delivered_at__isnull=True,
                uncertain_at__isnull=True,
                failure_reason__isnull=True,
            )
            .update(**values, claim_id=None, claimed_until=None)
        )
        return SettlementResult.APPLIED if changed else SettlementResult.STALE

    def delivered(self, claim: OutboxClaim[PayloadT], *, at: datetime) -> SettlementResult:
        return self._settle(claim, at, delivered_at=at)

    def retry(
        self, claim: OutboxClaim[PayloadT], *, available_at: datetime, at: datetime
    ) -> SettlementResult:
        from django.db.models import F

        _utc(available_at)
        return self._settle(claim, at, available_at=available_at, attempt=F("attempt") + 1)

    def defer(
        self, claim: OutboxClaim[PayloadT], *, available_at: datetime, at: datetime
    ) -> SettlementResult:
        _utc(available_at)
        result = self._settle(claim, at, available_at=available_at)
        return SettlementResult.DEFERRED if result else result

    def uncertain(
        self, claim: OutboxClaim[PayloadT], *, reason: str, at: datetime
    ) -> SettlementResult:
        if not reason:
            raise ValueError("uncertainty reason must not be empty")
        return self._settle(claim, at, uncertain_at=at, uncertainty_reason=reason)

    def resolve_uncertain(
        self,
        message_id: OpaqueId[object],
        *,
        expected_at: datetime,
        delivered: bool,
        available_at: datetime,
        at: datetime,
    ) -> SettlementResult:
        for stamp in (expected_at, available_at, at):
            _utc(stamp)
        changed = (
            self.model.objects.using(self.using)
            .filter(
                message_id=str(message_id),
                uncertain_at=expected_at,
                delivered_at__isnull=True,
                failure_reason__isnull=True,
            )
            .update(
                uncertain_at=None,
                uncertainty_reason=None,
                delivered_at=at if delivered else None,
                available_at=available_at,
            )
        )
        return SettlementResult.APPLIED if changed else SettlementResult.STALE

    def failed(
        self, claim: OutboxClaim[PayloadT], *, reason: str, at: datetime
    ) -> SettlementResult:
        if not reason:
            raise ValueError("failure reason must not be empty")
        return self._settle(claim, at, failure_reason=reason, failed_at=at)
