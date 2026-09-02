from django.urls import path

from pytitect_protocol_matrix.legacy.views import LegacyMutationView

app_name = "legacy"
urlpatterns = [path("mutations/", LegacyMutationView.as_view(), name="mutation")]
