from django.apps import AppConfig


class ReferenceAppConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "reference_app"

    def ready(self) -> None:
        from reference_app import checks

        del checks
