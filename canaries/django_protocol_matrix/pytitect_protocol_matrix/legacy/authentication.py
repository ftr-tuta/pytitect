from dataclasses import dataclass

from rest_framework.authentication import BaseAuthentication
from rest_framework.exceptions import AuthenticationFailed


@dataclass(frozen=True)
class LegacyPrincipal:
    name: str = "legacy-canary"
    is_authenticated: bool = True


class LegacyCredentialAuthentication(BaseAuthentication):
    def authenticate(self, request):  # type: ignore[no-untyped-def]
        credential = request.headers.get("X-Legacy-Credential")
        if credential != "synthetic-legacy-credential":
            raise AuthenticationFailed("Invalid synthetic legacy credential.")
        return LegacyPrincipal(), credential
