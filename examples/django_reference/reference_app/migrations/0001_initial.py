from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True
    dependencies = []
    operations = [
        migrations.CreateModel(
            name="IdempotencyRecord",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                ("namespace", models.CharField(max_length=255)),
                ("subject", models.CharField(max_length=255)),
                ("operation", models.CharField(max_length=255)),
                ("idempotency_key", models.CharField(max_length=255)),
                ("fingerprint", models.CharField(max_length=64)),
                ("reservation_token", models.CharField(max_length=64)),
                ("state", models.CharField(max_length=32)),
                ("expires_at", models.DateTimeField()),
                ("value", models.JSONField(null=True)),
                ("uncertainty_reason", models.TextField(null=True)),
                ("updated_at", models.DateTimeField()),
            ],
            options={
                "indexes": [
                    models.Index(fields=["state", "expires_at"], name="ref_idem_retention_idx")
                ],
                "constraints": [
                    models.UniqueConstraint(
                        fields=("namespace", "subject", "operation", "idempotency_key"),
                        name="ref_idempotency_identity",
                    )
                ],
            },
        ),
        migrations.CreateModel(
            name="ReceiptRecord",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                ("receipt_id", models.CharField(max_length=255)),
                ("kind", models.CharField(max_length=32)),
                ("state", models.CharField(max_length=32)),
                ("created_at", models.DateTimeField()),
                ("updated_at", models.DateTimeField()),
                ("result", models.JSONField(null=True)),
                ("metadata", models.JSONField(default=dict)),
            ],
            options={
                "indexes": [
                    models.Index(fields=["state", "updated_at"], name="ref_receipt_retention_idx")
                ],
                "constraints": [
                    models.UniqueConstraint(fields=("receipt_id",), name="ref_receipt_identity")
                ],
            },
        ),
        migrations.CreateModel(
            name="OutboxRecord",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                ("message_id", models.CharField(max_length=255)),
                ("topic", models.CharField(max_length=255)),
                ("payload", models.JSONField()),
                ("occurred_at", models.DateTimeField()),
                ("available_at", models.DateTimeField()),
                ("attempt", models.PositiveIntegerField(default=0)),
                ("claim_id", models.CharField(max_length=64, null=True)),
                ("claimed_until", models.DateTimeField(null=True)),
                ("delivered_at", models.DateTimeField(null=True)),
                ("failure_reason", models.TextField(null=True)),
                ("failed_at", models.DateTimeField(null=True)),
            ],
            options={
                "indexes": [
                    models.Index(fields=["delivered_at"], name="ref_outbox_delivered_idx"),
                    models.Index(fields=["failed_at"], name="ref_outbox_failed_idx"),
                ],
                "constraints": [
                    models.UniqueConstraint(fields=("message_id",), name="ref_outbox_identity")
                ],
            },
        ),
        migrations.CreateModel(
            name="SyntheticMutation",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                ("mutation_id", models.CharField(max_length=255, unique=True)),
                ("value", models.IntegerField()),
                ("created_at", models.DateTimeField()),
            ],
        ),
        migrations.CreateModel(
            name="FailedOutboxArchive",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                ("message_id", models.CharField(max_length=255, unique=True)),
                ("topic", models.CharField(max_length=255)),
                ("payload", models.JSONField()),
                ("occurred_at", models.DateTimeField()),
                ("available_at", models.DateTimeField()),
                ("attempt", models.PositiveIntegerField()),
                ("failure_reason", models.TextField()),
                ("failed_at", models.DateTimeField()),
            ],
        ),
    ]
