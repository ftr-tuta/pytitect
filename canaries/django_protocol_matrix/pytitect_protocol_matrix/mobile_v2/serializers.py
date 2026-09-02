from pytitect.drf import ClosedSerializer, StrictIntegerField


class V2MutationSerializer(ClosedSerializer):
    value = StrictIntegerField()
