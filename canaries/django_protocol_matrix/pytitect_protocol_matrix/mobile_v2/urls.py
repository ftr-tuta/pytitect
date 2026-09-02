from django.urls import path

from pytitect_protocol_matrix.mobile_v2.views import (
    MobileMutationView,
    MobileReceiptView,
)

app_name = "mobile_v2"
urlpatterns = [
    path("mutations/", MobileMutationView.as_view(), name="mutation"),
    path("receipts/<str:receipt_id>/", MobileReceiptView.as_view(), name="receipt"),
]
