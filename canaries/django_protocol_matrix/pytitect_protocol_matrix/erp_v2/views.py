from pytitect_protocol_matrix.erp_v2.authentication import (
    SyntheticHttpMessageSignatureAuthentication,
)
from pytitect_protocol_matrix.mobile_v2.views import (
    MobileMutationView,
    MobileReceiptView,
)


class ErpMutationView(MobileMutationView):
    authentication_classes = [SyntheticHttpMessageSignatureAuthentication]
    protocol_name = "erp_v2"


class ErpReceiptView(MobileReceiptView):
    authentication_classes = [SyntheticHttpMessageSignatureAuthentication]
