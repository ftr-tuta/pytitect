from django.urls import path

from pytitect_protocol_matrix.erp_v2.views import ErpMutationView, ErpReceiptView

app_name = "erp_v2"
urlpatterns = [
    path("mutations/", ErpMutationView.as_view(), name="mutation"),
    path("receipts/<str:receipt_id>/", ErpReceiptView.as_view(), name="receipt"),
]
