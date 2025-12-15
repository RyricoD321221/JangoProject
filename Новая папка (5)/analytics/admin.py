from django.contrib import admin

from .models import DatasetSnapshot


@admin.register(DatasetSnapshot)
class DatasetSnapshotAdmin(admin.ModelAdmin):
    list_display = ("title", "created_at")
    search_fields = ("title",)

