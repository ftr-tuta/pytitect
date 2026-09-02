from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True
    dependencies = []
    operations = [
        migrations.CreateModel(
            name="DomainMutation",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("protocol", models.CharField(max_length=32)),
                ("value", models.IntegerField()),
            ],
        ),
        migrations.CreateModel(
            name="IdempotencyRecord",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
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
                "constraints": [
                    models.UniqueConstraint(
                        fields=("namespace", "subject", "operation", "idempotency_key"),
                        name="mobile_v2_idempotency_identity",
                    )
                ]
            },
        ),
        migrations.CreateModel(
            name="ReplayRecord",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("namespace", models.CharField(max_length=255)),
                ("digest", models.CharField(max_length=64)),
                ("expires_at", models.DateTimeField()),
            ],
            options={
                "constraints": [
                    models.UniqueConstraint(
                        fields=("namespace", "digest"), name="mobile_v2_replay_identity"
                    )
                ]
            },
        ),
        migrations.CreateModel(
            name="InboxRecord",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("message_id", models.CharField(max_length=255)),
                ("reservation_token", models.CharField(max_length=255)),
                ("expires_at", models.DateTimeField()),
                ("completed_at", models.DateTimeField(null=True)),
            ],
            options={
                "constraints": [
                    models.UniqueConstraint(
                        fields=("message_id",), name="mobile_v2_inbox_identity"
                    )
                ]
            },
        ),
        migrations.CreateModel(
            name="OutboxRecord",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
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
            ],
            options={
                "constraints": [
                    models.UniqueConstraint(
                        fields=("message_id",), name="mobile_v2_outbox_identity"
                    )
                ]
            },
        ),
        migrations.CreateModel(
            name="CheckpointRecord",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("stream", models.CharField(max_length=255)),
                ("checkpoint", models.BinaryField()),
            ],
            options={
                "constraints": [
                    models.UniqueConstraint(
                        fields=("stream",), name="mobile_v2_checkpoint_identity"
                    )
                ]
            },
        ),
        migrations.CreateModel(
            name="ReceiptRecord",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
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
                "constraints": [
                    models.UniqueConstraint(
                        fields=("receipt_id",), name="mobile_v2_receipt_identity"
                    )
                ]
            },
        ),
        migrations.CreateModel(
            name="LeaseRecord",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("resource_key", models.CharField(max_length=255)),
                ("owner", models.CharField(max_length=255, null=True)),
                ("fencing_token", models.PositiveBigIntegerField(default=0)),
                ("expires_at", models.DateTimeField(null=True)),
            ],
            options={
                "constraints": [
                    models.UniqueConstraint(
                        fields=("resource_key",), name="mobile_v2_lease_identity"
                    )
                ]
            },
        ),
        migrations.CreateModel(
            name="GenerationRecord",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("dataset", models.CharField(max_length=255)),
                ("partition", models.CharField(max_length=255)),
                ("generation", models.PositiveBigIntegerField()),
            ],
            options={
                "constraints": [
                    models.UniqueConstraint(
                        fields=("dataset", "partition"),
                        name="mobile_v2_generation_identity",
                    )
                ]
            },
        ),
    ]
