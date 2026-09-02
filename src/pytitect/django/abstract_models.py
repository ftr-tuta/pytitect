"""Opt-in abstract model mixins.

Import this module only after Django is configured. All models are abstract and Pytitect
ships no migrations or concrete schema.
"""

from __future__ import annotations

from datetime import datetime

from django.db import models


class AbstractReceiptModel(models.Model):
    receipt_id: models.CharField[str, str] = models.CharField(max_length=255, unique=True)
    state: models.CharField[str, str] = models.CharField(max_length=32)
    created_at: models.DateTimeField[datetime, datetime] = models.DateTimeField()
    updated_at: models.DateTimeField[datetime, datetime] = models.DateTimeField()

    class Meta:
        abstract = True


class AbstractLeaseAuthorityModel(models.Model):
    resource_key: models.CharField[str, str] = models.CharField(max_length=255, unique=True)
    owner: models.CharField[str, str] = models.CharField(max_length=255)
    fencing_token: models.PositiveBigIntegerField[int, int] = models.PositiveBigIntegerField()
    expires_at: models.DateTimeField[datetime, datetime] = models.DateTimeField()

    class Meta:
        abstract = True
