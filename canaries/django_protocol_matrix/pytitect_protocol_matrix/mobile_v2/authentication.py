from dataclasses import dataclass

from rest_framework.authentication import BaseAuthentication
from rest_framework.exceptions import AuthenticationFailed


@dataclass(frozen=True)
class MobilePrincipal:
    name: str = "mobile-v2-canary"
    is_authenticated: bool = True


class SyntheticDPoPAuthentication(BaseAuthentication):
    """Canary binding; production proof verification remains consumer-owned."""

    def authenticate(self, request):  # type: ignore[no-untyped-def]
        authorization = request.headers.get("Authorization")
        proof = request.headers.get("DPoP")
        if (
            authorization != "DPoP synthetic-mobile-token"
            or proof != "synthetic-dpop-proof"
        ):
            raise AuthenticationFailed("Invalid synthetic DPoP credential.")
        return MobilePrincipal(), proof
