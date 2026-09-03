"""Explicit PostgreSQL stores for consumer-owned Django models and schemas."""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from datetime import datetime, timedelta
from typing import Any, TypeVar, cast

from pytitect.checkpoints import Checkpoint
from pytitect.core import JsonValue, OpaqueId, validate_json
from pytitect.idempotency import (
    AbandonReservationResult,
    CompleteReservationResult,
    Conflict,
    Execute,
    IdempotencyDecision,
    IdempotencyScope,
    InProgress,
    MarkUncertainResult,
    RenewReservationResult,
    Replay,
    RequestFingerprint,
    ReservationAbandoned,
    ReservationCompleted,
    ReservationMarkedUncertain,
    ReservationRenewed,
    ReservationToken,
    StaleReservation,
    Uncertain,
)
from pytitect.inbox import (
    InboxAccepted,
    InboxDecision,
    InboxDuplicate,
    InboxInProgress,
)
from pytitect.leases import (
    AcquireResult,
    Lease,
    LeaseAcquired,
    LeaseAuthority,
    LeaseBusy,
    LeaseReleased,
    ReleaseResult,
    RenewResult,
    StaleLease,
)
from pytitect.outbox import (
    OutboxAdded,
    OutboxAddResult,
    OutboxClaim,
    OutboxDuplicate,
    OutboxEnvelope,
)
from pytitect.receipts import (
    CommandReceipt,
    MutationReceipt,
    OperationReceipt,
    Receipt,
    ReceiptKind,
    ReceiptState,
    ReceiptTransitioned,
    RunReceipt,
    TerminalBoundaryReceipt,
)
from pytitect.security import ReplayAccepted, ReplayDetected
from pytitect.security.replay import ReplayDecision

T = TypeVar("T")
PayloadT = TypeVar("PayloadT")
ResourceT = TypeVar("ResourceT")

Encode = Callable[[T], JsonValue]
Decode = Callable[[JsonValue], T]


def _identity_encode(value: JsonValue) -> JsonValue:
    validate_json(value)
    return value


def _identity_decode(value: JsonValue) -> JsonValue:
    validate_json(value)
    return value


class DjangoIdempotencyStore[T]:
    def __init__(
        self,
        *,
        using: str,
        reserve: Callable[..., IdempotencyDecision[T]],
        renew: Callable[..., RenewReservationResult],
        complete: Callable[..., CompleteReservationResult],
        mark_uncertain: Callable[..., MarkUncertainResult],
        abandon: Callable[..., AbandonReservationResult],
    ) -> None:
        self.using = _alias(using)
        self._reserve = reserve
        self._renew = renew
        self._complete = complete
        self._uncertain = mark_uncertain
        self._abandon = abandon

    @classmethod
    def from_callbacks(
        cls,
        *,
        using: str,
        reserve: Callable[..., IdempotencyDecision[T]],
        renew: Callable[..., RenewReservationResult],
        complete: Callable[..., CompleteReservationResult],
        mark_uncertain: Callable[..., MarkUncertainResult],
        abandon: Callable[..., AbandonReservationResult],
    ) -> DjangoIdempotencyStore[T]:
        return cls(
            using=using,
            reserve=reserve,
            renew=renew,
            complete=complete,
            mark_uncertain=mark_uncertain,
            abandon=abandon,
        )

    @classmethod
    def from_model(
        cls,
        model: Any,
        *,
        using: str,
        encode_value: Encode[T],
        decode_value: Decode[T],
    ) -> DjangoIdempotencyStore[T]:
        _unique(model, {"namespace", "subject", "operation", "idempotency_key"})

        def reserve(
            scope: IdempotencyScope,
            key: str,
            fingerprint: RequestFingerprint,
            *,
            now: datetime,
            lease_ttl: timedelta,
            using: str,
        ) -> IdempotencyDecision[T]:
            _postgresql(using)
            _utc(now)
            if not key or lease_ttl <= timedelta(0):
                raise ValueError("idempotency key and a positive lease_ttl are required")
            from django.db import transaction

            lookup = {
                "namespace": scope.namespace,
                "subject": scope.subject,
                "operation": scope.operation,
                "idempotency_key": key,
            }
            with transaction.atomic(using=using):
                row = _locked_first(model, using, lookup)
                if row is not None and row.expires_at <= now:
                    row.delete(using=using)
                    row = None
                if row is None:
                    token = ReservationToken(uuid.uuid4().hex)
                    row, created = _create_or_locked(
                        model,
                        using,
                        lookup,
                        {
                            "fingerprint": str(fingerprint.value),
                            "reservation_token": token.value,
                            "state": "reserved",
                            "expires_at": now + lease_ttl,
                            "value": None,
                            "uncertainty_reason": None,
                            "updated_at": now,
                        },
                    )
                    if created:
                        return Execute(token)
                if row.fingerprint != str(fingerprint.value):
                    return Conflict()
                if row.state == "completed":
                    return Replay(decode_value(cast(JsonValue, row.value)))
                if row.state == "uncertain":
                    return Uncertain(row.uncertainty_reason or "the prior outcome is unknown")
                return InProgress(row.expires_at)

        def renew(
            token: ReservationToken,
            *,
            now: datetime,
            lease_ttl: timedelta,
            using: str,
        ) -> RenewReservationResult:
            _postgresql(using)
            _utc(now)
            if lease_ttl <= timedelta(0):
                raise ValueError("lease_ttl must be positive")
            expires_at = now + lease_ttl
            updated = (
                _manager(model, using)
                .filter(
                    reservation_token=token.value,
                    state="reserved",
                    expires_at__gt=now,
                )
                .update(expires_at=expires_at, updated_at=now)
            )
            return ReservationRenewed(expires_at) if updated else StaleReservation()

        def complete(
            token: ReservationToken,
            value: T,
            *,
            now: datetime,
            retention_ttl: timedelta,
            using: str,
        ) -> CompleteReservationResult:
            _postgresql(using)
            _utc(now)
            if retention_ttl <= timedelta(0):
                raise ValueError("retention_ttl must be positive")
            encoded = encode_value(value)
            validate_json(encoded)
            retained_until = now + retention_ttl
            updated = (
                _manager(model, using)
                .filter(
                    reservation_token=token.value,
                    state="reserved",
                    expires_at__gt=now,
                )
                .update(
                    state="completed",
                    value=encoded,
                    expires_at=retained_until,
                    updated_at=now,
                )
            )
            return ReservationCompleted(retained_until) if updated else StaleReservation()

        def mark_uncertain(
            token: ReservationToken,
            reason: str,
            *,
            now: datetime,
            retention_ttl: timedelta,
            using: str,
        ) -> MarkUncertainResult:
            _postgresql(using)
            _utc(now)
            if not reason:
                raise ValueError("an uncertainty reason is required")
            if retention_ttl <= timedelta(0):
                raise ValueError("retention_ttl must be positive")
            retained_until = now + retention_ttl
            updated = (
                _manager(model, using)
                .filter(
                    reservation_token=token.value,
                    state="reserved",
                    expires_at__gt=now,
                )
                .update(
                    state="uncertain",
                    uncertainty_reason=reason,
                    expires_at=retained_until,
                    updated_at=now,
                )
            )
            return ReservationMarkedUncertain(retained_until) if updated else StaleReservation()

        def abandon(
            token: ReservationToken,
            *,
            now: datetime,
            using: str,
        ) -> AbandonReservationResult:
            _postgresql(using)
            _utc(now)
            deleted, _ = (
                _manager(model, using)
                .filter(
                    reservation_token=token.value,
                    state="reserved",
                    expires_at__gt=now,
                )
                .delete()
            )
            return ReservationAbandoned() if deleted else StaleReservation()

        return cls.from_callbacks(
            using=using,
            reserve=reserve,
            renew=renew,
            complete=complete,
            mark_uncertain=mark_uncertain,
            abandon=abandon,
        )

    def reserve(
        self,
        scope: IdempotencyScope,
        key: str,
        fingerprint: RequestFingerprint,
        *,
        now: datetime,
        lease_ttl: timedelta,
    ) -> IdempotencyDecision[T]:
        return self._reserve(
            scope,
            key,
            fingerprint,
            now=now,
            lease_ttl=lease_ttl,
            using=self.using,
        )

    def renew(
        self,
        token: ReservationToken,
        *,
        now: datetime,
        lease_ttl: timedelta,
    ) -> RenewReservationResult:
        return self._renew(token, now=now, lease_ttl=lease_ttl, using=self.using)

    def complete(
        self,
        token: ReservationToken,
        value: T,
        *,
        now: datetime,
        retention_ttl: timedelta,
    ) -> CompleteReservationResult:
        return self._complete(
            token,
            value,
            now=now,
            retention_ttl=retention_ttl,
            using=self.using,
        )

    def mark_uncertain(
        self,
        token: ReservationToken,
        reason: str,
        *,
        now: datetime,
        retention_ttl: timedelta,
    ) -> MarkUncertainResult:
        return self._uncertain(
            token,
            reason,
            now=now,
            retention_ttl=retention_ttl,
            using=self.using,
        )

    def abandon(
        self,
        token: ReservationToken,
        *,
        now: datetime,
    ) -> AbandonReservationResult:
        return self._abandon(token, now=now, using=self.using)


class DjangoReplayStore:
    def __init__(self, *, using: str, reserve_digest: Callable[..., ReplayDecision]) -> None:
        self.using = _alias(using)
        self._reserve_digest = reserve_digest

    @classmethod
    def from_callbacks(
        cls, *, using: str, reserve_digest: Callable[..., ReplayDecision]
    ) -> DjangoReplayStore:
        return cls(using=using, reserve_digest=reserve_digest)

    @classmethod
    def from_model(cls, model: Any, *, using: str) -> DjangoReplayStore:
        _unique(model, {"namespace", "digest"})

        def reserve_digest(
            namespace: str,
            digest: str,
            *,
            now: datetime,
            ttl: timedelta,
            using: str,
        ) -> ReplayDecision:
            _postgresql(using)
            _utc(now)
            from django.db import transaction

            lookup = {"namespace": namespace, "digest": digest}
            with transaction.atomic(using=using):
                row = _locked_first(model, using, lookup)
                if row is not None and row.expires_at <= now:
                    row.delete(using=using)
                    row = None
                if row is not None:
                    return ReplayDetected(row.expires_at)
                row, created = _create_or_locked(model, using, lookup, {"expires_at": now + ttl})
                if created:
                    return ReplayAccepted(row.expires_at)
                return ReplayDetected(row.expires_at)

        return cls.from_callbacks(using=using, reserve_digest=reserve_digest)

    def reserve(
        self, namespace: str, value: str, *, now: datetime, ttl: timedelta
    ) -> ReplayDecision:
        if not namespace or not value or ttl <= timedelta(0):
            raise ValueError("namespace, value, and positive ttl are required")
        digest = hashlib.sha256(value.encode()).hexdigest()
        return self._reserve_digest(namespace, digest, now=now, ttl=ttl, using=self.using)


class DjangoInboxStore:
    def __init__(
        self,
        *,
        using: str,
        begin: Callable[..., InboxDecision],
        complete: Callable[..., bool],
        abandon: Callable[..., bool],
    ) -> None:
        self.using = _alias(using)
        self._begin = begin
        self._complete = complete
        self._abandon = abandon

    @classmethod
    def from_callbacks(
        cls,
        *,
        using: str,
        begin: Callable[..., InboxDecision],
        complete: Callable[..., bool],
        abandon: Callable[..., bool],
    ) -> DjangoInboxStore:
        return cls(using=using, begin=begin, complete=complete, abandon=abandon)

    @classmethod
    def from_model(cls, model: Any, *, using: str) -> DjangoInboxStore:
        _unique(model, {"message_id"})

        def begin(
            message_id: OpaqueId[object],
            *,
            token: str,
            now: datetime,
            ttl: timedelta,
            using: str,
        ) -> InboxDecision:
            _postgresql(using)
            _utc(now)
            from django.db import transaction

            lookup = {"message_id": str(message_id)}
            with transaction.atomic(using=using):
                row = _locked_first(model, using, lookup)
                if row is None:
                    row, created = _create_or_locked(
                        model,
                        using,
                        lookup,
                        {
                            "reservation_token": token,
                            "expires_at": now + ttl,
                            "completed_at": None,
                        },
                    )
                    if created:
                        return InboxAccepted(token)
                if row.completed_at is not None:
                    return InboxDuplicate(row.completed_at)
                if row.expires_at > now:
                    return InboxInProgress(row.expires_at)
                row.reservation_token = token
                row.expires_at = now + ttl
                row.completed_at = None
                row.save(
                    using=using,
                    update_fields=["reservation_token", "expires_at", "completed_at"],
                )
                return InboxAccepted(token)

        def complete(
            message_id: OpaqueId[object], *, token: str, now: datetime, using: str
        ) -> bool:
            _postgresql(using)
            _utc(now)
            return bool(
                _manager(model, using)
                .filter(
                    message_id=str(message_id),
                    reservation_token=token,
                    completed_at__isnull=True,
                    expires_at__gt=now,
                )
                .update(completed_at=now)
            )

        def abandon(message_id: OpaqueId[object], *, token: str, using: str) -> bool:
            _postgresql(using)
            deleted, _ = (
                _manager(model, using)
                .filter(
                    message_id=str(message_id),
                    reservation_token=token,
                    completed_at__isnull=True,
                )
                .delete()
            )
            return bool(deleted)

        return cls.from_callbacks(using=using, begin=begin, complete=complete, abandon=abandon)

    def begin(
        self,
        message_id: OpaqueId[object],
        *,
        token: str,
        now: datetime,
        ttl: timedelta,
    ) -> InboxDecision:
        if not token or ttl <= timedelta(0):
            raise ValueError("token and a positive ttl are required")
        return self._begin(message_id, token=token, now=now, ttl=ttl, using=self.using)

    def complete(self, message_id: OpaqueId[object], *, token: str, now: datetime) -> bool:
        return bool(self._complete(message_id, token=token, now=now, using=self.using))

    def abandon(self, message_id: OpaqueId[object], *, token: str) -> bool:
        return bool(self._abandon(message_id, token=token, using=self.using))


class DjangoOutboxStore[PayloadT]:
    def __init__(
        self,
        *,
        using: str,
        add: Callable[..., OutboxAddResult],
        claim: Callable[..., Sequence[OutboxClaim[PayloadT]]],
        delivered: Callable[..., bool],
        retry: Callable[..., bool],
        failed: Callable[..., bool],
    ) -> None:
        self.using = _alias(using)
        self._add = add
        self._claim = claim
        self._delivered = delivered
        self._retry = retry
        self._failed = failed

    @classmethod
    def from_callbacks(
        cls,
        *,
        using: str,
        add: Callable[..., OutboxAddResult],
        claim: Callable[..., Sequence[OutboxClaim[PayloadT]]],
        delivered: Callable[..., bool],
        retry: Callable[..., bool],
        failed: Callable[..., bool],
    ) -> DjangoOutboxStore[PayloadT]:
        return cls(
            using=using,
            add=add,
            claim=claim,
            delivered=delivered,
            retry=retry,
            failed=failed,
        )

    @classmethod
    def from_model(
        cls,
        model: Any,
        *,
        using: str,
        encode_payload: Encode[PayloadT],
        decode_payload: Decode[PayloadT],
    ) -> DjangoOutboxStore[PayloadT]:
        _unique(model, {"message_id"})

        def add(envelope: OutboxEnvelope[PayloadT], *, using: str) -> OutboxAddResult:
            _postgresql(using)
            encoded = encode_payload(envelope.payload)
            validate_json(encoded)
            _, created = _create_or_locked(
                model,
                using,
                {"message_id": str(envelope.message_id)},
                {
                    "topic": envelope.topic,
                    "payload": encoded,
                    "occurred_at": envelope.occurred_at,
                    "available_at": envelope.available_at,
                    "attempt": envelope.attempt,
                    "claim_id": None,
                    "claimed_until": None,
                    "delivered_at": None,
                    "failure_reason": None,
                },
            )
            return OutboxAdded() if created else OutboxDuplicate()

        def claim(
            *, now: datetime, limit: int, claim_ttl: timedelta, using: str
        ) -> Sequence[OutboxClaim[PayloadT]]:
            _postgresql(using)
            _utc(now)
            from django.db import transaction
            from django.db.models import Q

            with transaction.atomic(using=using):
                rows = list(
                    _manager(model, using)
                    .select_for_update(skip_locked=True)
                    .filter(
                        delivered_at__isnull=True,
                        failure_reason__isnull=True,
                        available_at__lte=now,
                    )
                    .filter(Q(claimed_until__isnull=True) | Q(claimed_until__lte=now))
                    .order_by("available_at", "message_id")[:limit]
                )
                output: list[OutboxClaim[PayloadT]] = []
                for row in rows:
                    row.claim_id = uuid.uuid4().hex
                    row.claimed_until = now + claim_ttl
                    row.save(using=using, update_fields=["claim_id", "claimed_until"])
                    envelope = _outbox_envelope(row, decode_payload)
                    output.append(OutboxClaim(row.claim_id, envelope, row.claimed_until))
                return output

        def delivered(claim: OutboxClaim[PayloadT], *, using: str) -> bool:
            _postgresql(using)
            deleted, _ = (
                _manager(model, using)
                .filter(message_id=str(claim.envelope.message_id), claim_id=claim.claim_id)
                .delete()
            )
            return bool(deleted)

        def retry(claim: OutboxClaim[PayloadT], *, available_at: datetime, using: str) -> bool:
            _postgresql(using)
            _utc(available_at)
            return bool(
                _manager(model, using)
                .filter(message_id=str(claim.envelope.message_id), claim_id=claim.claim_id)
                .update(
                    attempt=claim.envelope.attempt + 1,
                    available_at=available_at,
                    claim_id=None,
                    claimed_until=None,
                )
            )

        def failed(claim: OutboxClaim[PayloadT], *, reason: str, using: str) -> bool:
            _postgresql(using)
            if not reason:
                raise ValueError("outbox failure reason must not be empty")
            return bool(
                _manager(model, using)
                .filter(message_id=str(claim.envelope.message_id), claim_id=claim.claim_id)
                .update(failure_reason=reason, claim_id=None, claimed_until=None)
            )

        return cls.from_callbacks(
            using=using,
            add=add,
            claim=claim,
            delivered=delivered,
            retry=retry,
            failed=failed,
        )

    def add(self, envelope: OutboxEnvelope[PayloadT]) -> OutboxAddResult:
        return self._add(envelope, using=self.using)

    def claim(
        self, *, now: datetime, limit: int, claim_ttl: timedelta
    ) -> Sequence[OutboxClaim[PayloadT]]:
        if limit <= 0 or claim_ttl <= timedelta(0):
            raise ValueError("claim limit and ttl must be positive")
        return self._claim(now=now, limit=limit, claim_ttl=claim_ttl, using=self.using)

    def delivered(self, claim: OutboxClaim[PayloadT]) -> bool:
        return bool(self._delivered(claim, using=self.using))

    def retry(self, claim: OutboxClaim[PayloadT], *, available_at: datetime) -> bool:
        return bool(self._retry(claim, available_at=available_at, using=self.using))

    def failed(self, claim: OutboxClaim[PayloadT], *, reason: str) -> bool:
        return bool(self._failed(claim, reason=reason, using=self.using))


class DjangoCheckpointStore:
    def __init__(
        self,
        *,
        using: str,
        load: Callable[..., Checkpoint | None],
        load_for_update: Callable[..., Checkpoint | None],
        advance: Callable[..., bool],
    ) -> None:
        self.using = _alias(using)
        self._load = load
        self._load_for_update = load_for_update
        self._advance = advance

    @classmethod
    def from_callbacks(
        cls,
        *,
        using: str,
        load: Callable[..., Checkpoint | None],
        load_for_update: Callable[..., Checkpoint | None],
        advance: Callable[..., bool],
    ) -> DjangoCheckpointStore:
        return cls(
            using=using,
            load=load,
            load_for_update=load_for_update,
            advance=advance,
        )

    @classmethod
    def from_model(cls, model: Any, *, using: str) -> DjangoCheckpointStore:
        _unique(model, {"stream"})

        def load(stream: str, *, using: str) -> Checkpoint | None:
            _postgresql(using)
            row = _manager(model, using).filter(stream=stream).first()
            return None if row is None else Checkpoint(bytes(row.checkpoint))

        def load_for_update(stream: str, *, using: str) -> Checkpoint | None:
            _postgresql(using)
            row = _manager(model, using).select_for_update().filter(stream=stream).first()
            return None if row is None else Checkpoint(bytes(row.checkpoint))

        def advance(
            stream: str,
            *,
            expected: Checkpoint | None,
            checkpoint: Checkpoint,
            using: str,
        ) -> bool:
            _postgresql(using)
            if expected is None:
                _, created = _create_or_locked(
                    model, using, {"stream": stream}, {"checkpoint": checkpoint.value}
                )
                return created
            return bool(
                _manager(model, using)
                .filter(stream=stream, checkpoint=expected.value)
                .update(checkpoint=checkpoint.value)
            )

        return cls.from_callbacks(
            using=using, load=load, load_for_update=load_for_update, advance=advance
        )

    def load(self, stream: str) -> Checkpoint | None:
        _required(stream, "stream")
        return self._load(stream, using=self.using)

    def load_for_update(self, stream: str) -> Checkpoint | None:
        _required(stream, "stream")
        return self._load_for_update(stream, using=self.using)

    def advance(
        self,
        stream: str,
        *,
        expected: Checkpoint | None,
        checkpoint: Checkpoint,
    ) -> bool:
        _required(stream, "stream")
        return bool(
            self._advance(stream, expected=expected, checkpoint=checkpoint, using=self.using)
        )


class DjangoReceiptStore[T]:
    def __init__(
        self,
        *,
        using: str,
        get: Callable[..., Receipt[T] | None],
        add: Callable[..., bool],
        transition: Callable[..., bool],
        reconcile_uncertain: Callable[..., bool],
    ) -> None:
        self.using = _alias(using)
        self._get = get
        self._add = add
        self._transition = transition
        self._reconcile = reconcile_uncertain

    @classmethod
    def from_callbacks(
        cls,
        *,
        using: str,
        get: Callable[..., Receipt[T] | None],
        add: Callable[..., bool],
        transition: Callable[..., bool],
        reconcile_uncertain: Callable[..., bool],
    ) -> DjangoReceiptStore[T]:
        return cls(
            using=using,
            get=get,
            add=add,
            transition=transition,
            reconcile_uncertain=reconcile_uncertain,
        )

    @classmethod
    def from_model(
        cls,
        model: Any,
        *,
        using: str,
        encode_result: Encode[T],
        decode_result: Decode[T],
    ) -> DjangoReceiptStore[T]:
        _unique(model, {"receipt_id"})

        def get(receipt_id: OpaqueId[object], *, using: str) -> Receipt[T] | None:
            _postgresql(using)
            row = _manager(model, using).filter(receipt_id=str(receipt_id)).first()
            return None if row is None else _receipt(row, decode_result)

        def add(receipt: Receipt[T], *, using: str) -> bool:
            _postgresql(using)
            encoded = None if receipt.result is None else encode_result(receipt.result)
            if encoded is not None:
                validate_json(encoded)
            validate_json(cast(JsonValue, dict(receipt.metadata)))
            _, created = _create_or_locked(
                model,
                using,
                {"receipt_id": str(receipt.receipt_id)},
                {
                    "kind": receipt.kind.value,
                    "state": receipt.state.value,
                    "created_at": receipt.created_at,
                    "updated_at": receipt.updated_at,
                    "result": encoded,
                    "metadata": dict(receipt.metadata),
                },
            )
            return created

        def transition(receipt: Receipt[T], target: Receipt[T], *, using: str) -> bool:
            _postgresql(using)
            proposed = receipt.transition(
                target.state,
                at=target.updated_at,
                result=target.result,
            )
            if not isinstance(proposed, ReceiptTransitioned) or proposed.receipt != target:
                return False
            encoded = None if target.result is None else encode_result(target.result)
            if encoded is not None:
                validate_json(encoded)
            from django.db import transaction

            with transaction.atomic(using=using):
                row = (
                    _manager(model, using)
                    .select_for_update()
                    .filter(receipt_id=str(receipt.receipt_id))
                    .first()
                )
                if (
                    row is None
                    or row.state != receipt.state.value
                    or row.updated_at != receipt.updated_at
                ):
                    return False
                row.state = target.state.value
                row.updated_at = target.updated_at
                row.result = encoded
                row.save(using=using, update_fields=["state", "updated_at", "result"])
                return True

        def reconcile_uncertain(receipt: Receipt[T], target: Receipt[T], *, using: str) -> bool:
            _postgresql(using)
            if not _valid_reconciliation(receipt, target):
                return False
            encoded = None if target.result is None else encode_result(target.result)
            if encoded is not None:
                validate_json(encoded)
            from django.db import transaction

            with transaction.atomic(using=using):
                row = (
                    _manager(model, using)
                    .select_for_update()
                    .filter(receipt_id=str(receipt.receipt_id))
                    .first()
                )
                if (
                    row is None
                    or row.state != ReceiptState.UNCERTAIN.value
                    or row.updated_at != receipt.updated_at
                ):
                    return False
                row.state = target.state.value
                row.updated_at = target.updated_at
                row.result = encoded
                row.save(using=using, update_fields=["state", "updated_at", "result"])
                return True

        return cls.from_callbacks(
            using=using,
            get=get,
            add=add,
            transition=transition,
            reconcile_uncertain=reconcile_uncertain,
        )

    def get(self, receipt_id: OpaqueId[object]) -> Receipt[T] | None:
        return self._get(receipt_id, using=self.using)

    def add(self, receipt: Receipt[T]) -> bool:
        return bool(self._add(receipt, using=self.using))

    def transition(self, receipt: Receipt[T], target: Receipt[T]) -> bool:
        return bool(self._transition(receipt, target, using=self.using))

    def reconcile_uncertain(self, receipt: Receipt[T], target: Receipt[T]) -> bool:
        return bool(self._reconcile(receipt, target, using=self.using))


class DjangoLeaseStore[ResourceT]:
    def __init__(
        self,
        *,
        using: str,
        acquire: Callable[..., AcquireResult[ResourceT]],
        renew: Callable[..., RenewResult[ResourceT]],
        release: Callable[..., ReleaseResult],
        authority: Callable[..., int | None],
        lock_authority: Callable[..., LeaseAuthority | None],
    ) -> None:
        self.using = _alias(using)
        self._acquire = acquire
        self._renew = renew
        self._release = release
        self._authority = authority
        self._lock_authority = lock_authority

    @classmethod
    def from_callbacks(
        cls,
        *,
        using: str,
        acquire: Callable[..., AcquireResult[ResourceT]],
        renew: Callable[..., RenewResult[ResourceT]],
        release: Callable[..., ReleaseResult],
        authority: Callable[..., int | None],
        lock_authority: Callable[..., LeaseAuthority | None],
    ) -> DjangoLeaseStore[ResourceT]:
        return cls(
            using=using,
            acquire=acquire,
            renew=renew,
            release=release,
            authority=authority,
            lock_authority=lock_authority,
        )

    @classmethod
    def from_model(
        cls,
        model: Any,
        *,
        using: str,
        encode_resource: Callable[[ResourceT], str] = str,
        decode_resource: Callable[[str], ResourceT] | None = None,
    ) -> DjangoLeaseStore[ResourceT]:
        _unique(model, {"resource_key"})

        def acquire(
            resource: ResourceT,
            *,
            owner: str,
            now: datetime,
            ttl: timedelta,
            using: str,
        ) -> AcquireResult[ResourceT]:
            _postgresql(using)
            _utc(now)
            from django.db import transaction

            key = encode_resource(resource)
            with transaction.atomic(using=using):
                row = _locked_first(model, using, {"resource_key": key})
                if row is None:
                    row, created = _create_or_locked(
                        model,
                        using,
                        {"resource_key": key},
                        {"owner": owner, "fencing_token": 1, "expires_at": now + ttl},
                    )
                    if created:
                        return LeaseAcquired(Lease(resource, owner, 1, now + ttl))
                if row.owner is not None and row.expires_at is not None and row.expires_at > now:
                    return LeaseBusy(row.owner, row.expires_at)
                row.fencing_token += 1
                row.owner = owner
                row.expires_at = now + ttl
                row.save(
                    using=using,
                    update_fields=["fencing_token", "owner", "expires_at"],
                )
                return LeaseAcquired(Lease(resource, owner, row.fencing_token, row.expires_at))

        def renew(
            lease: Lease[ResourceT],
            *,
            now: datetime,
            ttl: timedelta,
            using: str,
        ) -> RenewResult[ResourceT]:
            _postgresql(using)
            _utc(now)
            from django.db import transaction

            with transaction.atomic(using=using):
                row = _locked_first(model, using, {"resource_key": encode_resource(lease.resource)})
                if not _authoritative(row, lease, now):
                    return StaleLease()
                row.expires_at = now + ttl
                row.save(using=using, update_fields=["expires_at"])
                return cast(RenewResult[ResourceT], _renewed(lease, row.expires_at))

        def release(lease: Lease[ResourceT], *, now: datetime, using: str) -> ReleaseResult:
            _postgresql(using)
            _utc(now)
            from django.db import transaction

            with transaction.atomic(using=using):
                row = _locked_first(model, using, {"resource_key": encode_resource(lease.resource)})
                if not _authoritative(row, lease, now):
                    return StaleLease()
                row.owner = None
                row.expires_at = None
                row.save(using=using, update_fields=["owner", "expires_at"])
                return LeaseReleased()

        def authority(resource: ResourceT, *, using: str) -> int | None:
            _postgresql(using)
            row = _manager(model, using).filter(resource_key=encode_resource(resource)).first()
            return None if row is None or row.owner is None else int(row.fencing_token)

        def lock_authority(resource: ResourceT, *, using: str) -> LeaseAuthority | None:
            _postgresql(using)
            row = (
                _manager(model, using)
                .select_for_update()
                .filter(resource_key=encode_resource(resource))
                .first()
            )
            if row is None or row.owner is None or row.expires_at is None:
                return None
            return LeaseAuthority(row.owner, row.fencing_token, row.expires_at)

        del decode_resource  # Resource values are supplied by callers; rows store only keys.
        return cls.from_callbacks(
            using=using,
            acquire=acquire,
            renew=renew,
            release=release,
            authority=authority,
            lock_authority=lock_authority,
        )

    def acquire(
        self, resource: ResourceT, *, owner: str, now: datetime, ttl: timedelta
    ) -> AcquireResult[ResourceT]:
        if not owner or ttl <= timedelta(0):
            raise ValueError("owner and positive ttl are required")
        return self._acquire(resource, owner=owner, now=now, ttl=ttl, using=self.using)

    def renew(
        self, lease: Lease[ResourceT], *, now: datetime, ttl: timedelta
    ) -> RenewResult[ResourceT]:
        if ttl <= timedelta(0):
            raise ValueError("ttl must be positive")
        return self._renew(lease, now=now, ttl=ttl, using=self.using)

    def release(self, lease: Lease[ResourceT], *, now: datetime) -> ReleaseResult:
        return self._release(lease, now=now, using=self.using)

    def authority(self, resource: ResourceT) -> int | None:
        return self._authority(resource, using=self.using)

    def lock_authority(self, resource: ResourceT) -> LeaseAuthority | None:
        return self._lock_authority(resource, using=self.using)


class DjangoGenerationStore:
    def __init__(
        self,
        *,
        using: str,
        load_for_update: Callable[..., int | None],
        compare_and_set: Callable[..., bool],
    ) -> None:
        self.using = _alias(using)
        self._load_for_update = load_for_update
        self._compare_and_set = compare_and_set

    @classmethod
    def from_callbacks(
        cls,
        *,
        using: str,
        load_for_update: Callable[..., int | None],
        compare_and_set: Callable[..., bool],
    ) -> DjangoGenerationStore:
        return cls(
            using=using,
            load_for_update=load_for_update,
            compare_and_set=compare_and_set,
        )

    @classmethod
    def from_model(cls, model: Any, *, using: str) -> DjangoGenerationStore:
        _unique(model, {"dataset", "partition"})

        def load_for_update(dataset: str, partition: str, *, using: str) -> int | None:
            _postgresql(using)
            row = (
                _manager(model, using)
                .select_for_update()
                .filter(dataset=dataset, partition=partition)
                .first()
            )
            return None if row is None else int(row.generation)

        def compare_and_set(
            dataset: str,
            partition: str,
            *,
            expected: int | None,
            generation: int,
            using: str,
        ) -> bool:
            _postgresql(using)
            if expected is None:
                _, created = _create_or_locked(
                    model,
                    using,
                    {"dataset": dataset, "partition": partition},
                    {"generation": generation},
                )
                return created
            return bool(
                _manager(model, using)
                .filter(dataset=dataset, partition=partition, generation=expected)
                .update(generation=generation)
            )

        return cls.from_callbacks(
            using=using,
            load_for_update=load_for_update,
            compare_and_set=compare_and_set,
        )

    def load_for_update(self, dataset: str, partition: str) -> int | None:
        _required(dataset, "dataset")
        _required(partition, "partition")
        return self._load_for_update(dataset, partition, using=self.using)

    def compare_and_set(
        self,
        dataset: str,
        partition: str,
        *,
        expected: int | None,
        generation: int,
    ) -> bool:
        if generation < 0:
            raise ValueError("generation must not be negative")
        return bool(
            self._compare_and_set(
                dataset,
                partition,
                expected=expected,
                generation=generation,
                using=self.using,
            )
        )


def _manager(model: Any, using: str) -> Any:
    return model._default_manager.using(using)


def _locked_first(model: Any, using: str, lookup: Mapping[str, object]) -> Any:
    return _manager(model, using).select_for_update().filter(**lookup).first()


def _create_or_locked(
    model: Any,
    using: str,
    lookup: Mapping[str, object],
    defaults: Mapping[str, object],
) -> tuple[Any, bool]:
    from django.db import IntegrityError, transaction

    with transaction.atomic(using=using):
        try:
            with transaction.atomic(using=using):
                return _manager(model, using).create(**lookup, **defaults), True
        except IntegrityError:
            row = _locked_first(model, using, lookup)
            if row is None:
                raise
            return row, False


def _postgresql(using: str) -> None:
    from django.db import connections

    if connections[using].vendor != "postgresql":
        raise ValueError("Django stores require an explicit PostgreSQL database alias")


def _unique(model: Any, required: set[str]) -> None:
    meta = model._meta
    candidates = [
        {field.name}
        for field in meta.fields
        if getattr(field, "unique", False) and not getattr(field, "primary_key", False)
    ]
    candidates.extend(set(fields) for fields in meta.unique_together)
    candidates.extend(set(constraint.fields) for constraint in meta.total_unique_constraints)
    if required not in candidates:
        joined = ", ".join(sorted(required))
        raise ValueError(f"consumer model requires a unique constraint on: {joined}")


def _outbox_envelope[PayloadT](row: Any, decode: Decode[PayloadT]) -> OutboxEnvelope[PayloadT]:
    return OutboxEnvelope(
        OpaqueId(row.message_id),
        row.topic,
        decode(cast(JsonValue, row.payload)),
        row.occurred_at,
        row.available_at,
        row.attempt,
    )


def _receipt[T](row: Any, decode: Decode[T]) -> Receipt[T]:
    result = None if row.result is None else decode(cast(JsonValue, row.result))
    args: tuple[OpaqueId[object], ReceiptState, datetime, datetime] = (
        OpaqueId(row.receipt_id),
        ReceiptState(row.state),
        row.created_at,
        row.updated_at,
    )
    kwargs: dict[str, Any] = {"result": result, "metadata": row.metadata}
    kind = ReceiptKind(row.kind)
    if kind is ReceiptKind.MUTATION:
        return MutationReceipt(*args, **kwargs)
    if kind is ReceiptKind.COMMAND:
        return CommandReceipt(*args, **kwargs)
    if kind is ReceiptKind.RUN:
        return RunReceipt(*args, **kwargs)
    if kind is ReceiptKind.OPERATION:
        return OperationReceipt(*args, **kwargs)
    return TerminalBoundaryReceipt(*args, **kwargs)


def _valid_reconciliation[T](receipt: Receipt[T], target: Receipt[T]) -> bool:
    return bool(
        receipt.state is ReceiptState.UNCERTAIN
        and target.state
        in {
            ReceiptState.COMPLETED,
            ReceiptState.REJECTED,
            ReceiptState.CONFLICTED,
        }
        and target.receipt_id == receipt.receipt_id
        and target.kind is receipt.kind
        and target.created_at == receipt.created_at
        and target.updated_at >= receipt.updated_at
        and target.metadata == receipt.metadata
        and (target.state is not ReceiptState.COMPLETED or target.result is not None)
    )


def _authoritative(row: Any, lease: Lease[Any], now: datetime) -> bool:
    return bool(
        row is not None
        and row.owner == lease.owner
        and row.fencing_token == lease.fencing_token
        and row.expires_at is not None
        and row.expires_at == lease.expires_at
        and row.expires_at > now
    )


def _renewed[ResourceT](lease: Lease[ResourceT], expires_at: datetime) -> object:
    from pytitect.leases import LeaseRenewed

    return LeaseRenewed(replace(lease, expires_at=expires_at))


def _alias(using: str) -> str:
    _required(using, "Django database alias")
    return using


def _required(value: str, name: str) -> None:
    if not value:
        raise ValueError(f"{name} must not be empty")


def _utc(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("timestamps must be timezone-aware UTC")
