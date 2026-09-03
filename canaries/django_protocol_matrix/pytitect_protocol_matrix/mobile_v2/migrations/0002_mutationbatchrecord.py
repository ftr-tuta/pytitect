from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("mobile_v2", "0001_initial")]

    operations = [
        migrations.CreateModel(
            name="MutationBatchRecord",
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
                ("batch_id", models.CharField(max_length=255)),
                ("fingerprint", models.CharField(max_length=64)),
                ("reservation_token", models.CharField(max_length=64)),
                ("state", models.CharField(max_length=32)),
                ("total_items", models.PositiveIntegerField()),
                ("next_index", models.PositiveIntegerField(default=0)),
                ("receipts", models.JSONField(default=list)),
                ("lease_expires_at", models.DateTimeField()),
                ("retention_expires_at", models.DateTimeField(null=True)),
                ("uncertainty_reason", models.TextField(null=True)),
                ("updated_at", models.DateTimeField()),
            ],
            options={
                "constraints": [
                    models.UniqueConstraint(
                        fields=("namespace", "batch_id"),
                        name="mobile_v2_mutation_batch_identity",
                    )
                ]
            },
        )
    ]
