from django.contrib import admin

from .models import CsvAnalysisRun, DatasetSnapshot


@admin.register(DatasetSnapshot)
class DatasetSnapshotAdmin(admin.ModelAdmin):
    list_display = ("title", "created_at")
    search_fields = ("title",)


@admin.register(CsvAnalysisRun)
class CsvAnalysisRunAdmin(admin.ModelAdmin):
    list_display = ("id", "user", "original_filename", "created_at", "top_n")
    list_filter = ("created_at",)
    search_fields = ("original_filename", "user__username")
    readonly_fields = ("created_at", "results_payload")
    raw_id_fields = ("user",)

