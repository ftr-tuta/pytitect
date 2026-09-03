from uuid import uuid4

from django.utils import timezone
from rest_framework.exceptions import NotFound, ValidationError
from rest_framework.response import Response
from rest_framework.views import APIView

from pytitect import OpaqueId
from pytitect.django import (
    DjangoIdempotencyStore,
    DjangoOutboxStore,
    DjangoReceiptStore,
    DjangoTransactionalOperation,
    TransactionalOperationCommitted,
)
from pytitect.idempotency import (
    Conflict,
    IdempotencyPolicy,
    IdempotencyScope,
    InProgress,
    Replay,
    RequestFingerprint,
)
from pytitect.outbox import OutboxEnvelope
from pytitect.receipts import MutationReceipt, ReceiptState
from pytitect.security.canonical import canonical_json
from pytitect_protocol_matrix.mobile_v2.authentication import (
    SyntheticDPoPAuthentication,
)
from pytitect_protocol_matrix.mobile_v2.models import (
    DomainMutation,
    IdempotencyRecord,
    OutboxRecord,
    ReceiptRecord,
)
from pytitect_protocol_matrix.mobile_v2.serializers import V2MutationSerializer


def _json(value):  # type: ignore[no-untyped-def]
    return value


class MobileMutationView(APIView):
    authentication_classes = [SyntheticDPoPAuthentication]
    protocol_name = "mobile_v2"

    def post(self, request):  # type: ignore[no-untyped-def]
        if request.headers.get("X-Protocol-Version") != "2":
            raise ValidationError("Unsupported protocol version.")
        serializer = V2MutationSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        key = request.headers.get("Idempotency-Key")
        if not key:
            raise ValidationError("Idempotency-Key is required.")
        payload = {"value": serializer.validated_data["value"]}
        idempotency = DjangoIdempotencyStore.from_model(
            IdempotencyRecord,
            using="default",
            encode_value=_json,
            decode_value=_json,
        )
        receipts = DjangoReceiptStore.from_model(
            ReceiptRecord,
            using="default",
            encode_result=_json,
            decode_result=_json,
        )
        outbox = DjangoOutboxStore.from_model(
            OutboxRecord,
            using="default",
            encode_payload=_json,
            decode_payload=_json,
        )
        operation = DjangoTransactionalOperation(
            using="default",
            domain_using="default",
            idempotency=idempotency,
            receipts=receipts,
            outbox=outbox,
            idempotency_policy=IdempotencyPolicy(
                execution_lease_ttl=timezone.timedelta(minutes=5),
                result_retention_ttl=timezone.timedelta(days=1),
                uncertainty_retention_ttl=timezone.timedelta(days=7),
            ),
        )
        receipt_id = OpaqueId(str(uuid4()))

        def mutate(using):  # type: ignore[no-untyped-def]
            mutation = DomainMutation.objects.using(using).create(
                protocol=self.protocol_name,
                value=payload["value"],
            )
            return {"mutation_id": mutation.pk, "receipt_id": str(receipt_id)}

        def make_receipt(result):  # type: ignore[no-untyped-def]
            now = timezone.now()
            return MutationReceipt(
                receipt_id,
                ReceiptState.COMPLETED,
                now,
                now,
                result=result,
            )

        def make_outbox(result):  # type: ignore[no-untyped-def]
            now = timezone.now()
            return (
                OutboxEnvelope(
                    OpaqueId(str(uuid4())),
                    f"{self.protocol_name}.mutation",
                    result,
                    now,
                    now,
                ),
            )

        result = operation.execute(
            scope=IdempotencyScope(self.protocol_name, "synthetic", "mutation"),
            key=key,
            fingerprint=RequestFingerprint.from_json(
                payload, canonicalizer=canonical_json
            ),
            mutate=mutate,
            make_receipt=make_receipt,
            make_outbox=make_outbox,
        )
        if isinstance(result, TransactionalOperationCommitted):
            return Response(result.value, status=201)
        if isinstance(result, Replay):
            return Response(result.value)
        if isinstance(result, Conflict):
            return Response({"code": "idempotency_conflict"}, status=409)
        if isinstance(result, InProgress):
            return Response({"code": "in_progress"}, status=409)
        return Response({"code": "uncertain"}, status=503)


class MobileReceiptView(APIView):
    authentication_classes = [SyntheticDPoPAuthentication]

    def get(self, request, receipt_id):  # type: ignore[no-untyped-def]
        del request
        row = ReceiptRecord.objects.filter(receipt_id=receipt_id).first()
        if row is None:
            raise NotFound()
        return Response(
            {"receipt_id": row.receipt_id, "state": row.state, "result": row.result}
        )
