from django.conf import settings
from django.db import models


class User(models.Model):
    username = models.CharField("Логин", max_length=150, unique=True)
    email = models.EmailField("Email", unique=True)
    first_name = models.CharField("Имя", max_length=150, blank=True)
    last_name = models.CharField("Фамилия", max_length=150, blank=True)
    created_at = models.DateTimeField("Дата регистрации", auto_now_add=True)

    class Meta:
        verbose_name = "Пользователь"
        verbose_name_plural = "Пользователи"

    def __str__(self):
        return self.username


class DatasetSnapshot(models.Model):
    title = models.CharField(max_length=255)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at"]
        verbose_name = "Срез данных"
        verbose_name_plural = "Срезы данных"

    def __str__(self):
        return self.title


class CsvAnalysisRun(models.Model):
    """Сохранённый результат Spark-анализа CSV (история по пользователю)."""

    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="csv_analysis_runs",
        verbose_name="Пользователь",
    )
    created_at = models.DateTimeField("Дата анализа", auto_now_add=True)
    original_filename = models.CharField("Исходный файл", max_length=255)
    top_n = models.PositiveSmallIntegerField("Top-N строковых", default=5)
    show_all_string_values = models.BooleanField("Все строковые значения", default=False)
    column_mapping = models.JSONField("Сопоставление колонок", default=dict)
    results_payload = models.JSONField("Результаты анализа")

    class Meta:
        ordering = ["-created_at"]
        verbose_name = "Анализ CSV"
        verbose_name_plural = "История анализов CSV"

    def __str__(self):
        return f"{self.original_filename} ({self.created_at:%Y-%m-%d %H:%M})"
