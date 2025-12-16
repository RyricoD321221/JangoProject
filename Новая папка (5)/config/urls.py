from django.contrib import admin
from django.urls import path, include

from analytics import views as analytics_views
from django.contrib.auth import views as auth_views

urlpatterns = [
    path("admin/", admin.site.urls),
    path(
        "accounts/login/",
        auth_views.LoginView.as_view(template_name="registration/login.html"),
        name="login",
    ),
    path(
        "accounts/logout/",
        analytics_views.logout_view,
        name="logout",
    ),
    path("", include("analytics.urls")),
]

