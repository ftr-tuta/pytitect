from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("mobile_v2", "0002_mutationbatchrecord")]

    operations = [
        migrations.RemoveConstraint(
            model_name="inboxrecord",
            name="mobile_v2_inbox_identity",
        ),
        migrations.AddField(
            model_name="inboxrecord",
            name="consumer",
            field=models.CharField(default="processor", max_length=255),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name="inboxrecord",
            name="namespace",
            field=models.CharField(default="protocol-matrix", max_length=255),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name="inboxrecord",
            name="source",
            field=models.CharField(default="canary", max_length=255),
            preserve_default=False,
        ),
        migrations.AddConstraint(
            model_name="inboxrecord",
            constraint=models.UniqueConstraint(
                fields=("namespace", "source", "consumer", "message_id"),
                name="mobile_v2_inbox_identity",
            ),
        ),
    ]
