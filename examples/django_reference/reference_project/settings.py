from __future__ import annotations

import os
from urllib.parse import urlparse

SECRET_KEY = "synthetic-reference-only"
DEBUG = False
USE_TZ = True
TIME_ZONE = "UTC"
ROOT_URLCONF = "reference_project.urls"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "reference_app",
]
MIDDLEWARE: list[str] = []

database_url = os.environ.get("REFERENCE_POSTGRES_DSN")
if not database_url:
    raise RuntimeError(
        "REFERENCE_POSTGRES_DSN is required; this example intentionally tests PostgreSQL"
    )
parsed = urlparse(database_url)
if parsed.scheme not in {"postgres", "postgresql"} or not parsed.hostname or not parsed.path:
    raise RuntimeError("REFERENCE_POSTGRES_DSN must be a PostgreSQL URL")

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": parsed.path.lstrip("/"),
        "USER": parsed.username or "",
        "PASSWORD": parsed.password or "",
        "HOST": parsed.hostname,
        "PORT": str(parsed.port or 5432),
        "CONN_MAX_AGE": 0,
    }
}
