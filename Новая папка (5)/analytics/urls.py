from django.urls import path

from django.contrib.auth.decorators import login_required

from . import views

app_name = "analytics"

urlpatterns = [
    path("", views.news, name="news"),
    path("dashboard/", login_required(views.dashboard), name="dashboard"),
    path("news/", views.news, name="news"),  # алиас
    path("cabinet/", login_required(views.cabinet), name="cabinet"),
    path(
        "cabinet/analysis/<int:pk>/",
        login_required(views.analysis_detail),
        name="analysis_detail",
    ),
    path(
        "cabinet/analysis/<int:pk>/export/",
        login_required(views.analysis_export),
        name="analysis_export",
    ),
    path("upload/", login_required(views.upload_csv), name="upload"),
    path("register/", views.register, name="register"),
]

