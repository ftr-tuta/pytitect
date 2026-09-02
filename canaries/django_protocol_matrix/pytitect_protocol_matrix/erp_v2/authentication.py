from dataclasses import dataclass

from rest_framework.authentication import BaseAuthentication
from rest_framework.exceptions import AuthenticationFailed


@dataclass(frozen=True)
class ErpPrincipal:
    name: str = "erp-v2-canary"
    is_authenticated: bool = True


class SyntheticHttpMessageSignatureAuthentication(BaseAuthentication):
    """Canary binding; production key resolution remains consumer-owned."""

    def authenticate(self, request):  # type: ignore[no-untyped-def]
        signature_input = request.headers.get("Signature-Input")
        signature = request.headers.get("Signature")
        if (
            signature_input
            != 'sig1=("@method" "@path");keyid="synthetic-erp";alg="hmac-sha256"'
            or signature != "sig1=:cHl0aXRlY3QtY2FuYXJ5:"
        ):
            raise AuthenticationFailed("Invalid synthetic HTTP Message Signature.")
        return ErpPrincipal(), signature
