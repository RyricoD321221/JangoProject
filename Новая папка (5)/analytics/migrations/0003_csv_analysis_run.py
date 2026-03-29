import django.db.models.deletion
from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ("analytics", "0002_user"),
    ]

    operations = [
        migrations.CreateModel(
            name="CsvAnalysisRun",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("created_at", models.DateTimeField(auto_now_add=True, verbose_name="Дата анализа")),
                ("original_filename", models.CharField(max_length=255, verbose_name="Исходный файл")),
                ("top_n", models.PositiveSmallIntegerField(default=5, verbose_name="Top-N строковых")),
                ("show_all_string_values", models.BooleanField(default=False, verbose_name="Все строковые значения")),
                ("column_mapping", models.JSONField(default=dict, verbose_name="Сопоставление колонок")),
                ("results_payload", models.JSONField(verbose_name="Результаты анализа")),
                (
                    "user",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="csv_analysis_runs",
                        to=settings.AUTH_USER_MODEL,
                        verbose_name="Пользователь",
                    ),
                ),
            ],
            options={
                "verbose_name": "Анализ CSV",
                "verbose_name_plural": "История анализов CSV",
                "ordering": ["-created_at"],
            },
        ),
    ]
