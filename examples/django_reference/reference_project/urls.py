from django.urls import include, path

urlpatterns = [path("reference/", include("reference_app.urls"))]
