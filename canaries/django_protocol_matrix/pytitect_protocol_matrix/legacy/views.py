from rest_framework.response import Response
from rest_framework.views import APIView

from pytitect_protocol_matrix.legacy.authentication import (
    LegacyCredentialAuthentication,
)
from pytitect_protocol_matrix.legacy.models import LegacyMutation
from pytitect_protocol_matrix.legacy.serializers import LegacyMutationSerializer


class LegacyMutationView(APIView):
    authentication_classes = [LegacyCredentialAuthentication]

    def post(self, request):  # type: ignore[no-untyped-def]
        serializer = LegacyMutationSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        mutation, created = LegacyMutation.objects.get_or_create(
            request_id=serializer.validated_data["request_id"],
            defaults={"value": serializer.validated_data["value"]},
        )
        return Response({"id": mutation.pk, "created": created})
