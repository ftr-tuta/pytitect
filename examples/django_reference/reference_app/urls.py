from django.urls import path

from reference_app import views

urlpatterns = [
    path("legacy/mutations/<str:mutation_id>", views.legacy_mutation),
    path("sync/1/mutations/<str:mutation_id>", views.sync_mutation),
]
