from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("mobile_v2", "0003_scope_inbox_identity")]

    operations = [
        migrations.AddField(
            model_name="outboxrecord",
            name="failed_at",
            field=models.DateTimeField(null=True),
        ),
        migrations.CreateModel(
            name="OutboxArchive",
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
        migrations.AddIndex(
            model_name="idempotencyrecord",
            index=models.Index(
                fields=["state", "expires_at"], name="mv2_idem_retention_idx"
            ),
        ),
        migrations.AddIndex(
            model_name="mutationbatchrecord",
            index=models.Index(
                fields=["state", "retention_expires_at"],
                name="mv2_batch_retention_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="replayrecord",
            index=models.Index(fields=["expires_at"], name="mv2_replay_retention_idx"),
        ),
        migrations.AddIndex(
            model_name="inboxrecord",
            index=models.Index(fields=["completed_at"], name="mv2_inbox_completed_idx"),
        ),
        migrations.AddIndex(
            model_name="inboxrecord",
            index=models.Index(fields=["expires_at"], name="mv2_inbox_expires_idx"),
        ),
        migrations.AddIndex(
            model_name="receiptrecord",
            index=models.Index(
                fields=["state", "updated_at"], name="mv2_receipt_retention_idx"
            ),
        ),
        migrations.AddIndex(
            model_name="outboxrecord",
            index=models.Index(fields=["delivered_at"], name="mv2_outbox_delivered_idx"),
        ),
        migrations.AddIndex(
            model_name="outboxrecord",
            index=models.Index(fields=["failed_at"], name="mv2_outbox_failed_idx"),
        ),
    ]
