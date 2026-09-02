from rest_framework import serializers


class LegacyMutationSerializer(serializers.Serializer):
    request_id = serializers.CharField(max_length=255)
    value = serializers.IntegerField()
