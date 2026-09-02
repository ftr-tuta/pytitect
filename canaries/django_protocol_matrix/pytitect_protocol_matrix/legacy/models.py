from django.db import models


class LegacyMutation(models.Model):
    request_id = models.CharField(max_length=255, unique=True)
    value = models.IntegerField()
