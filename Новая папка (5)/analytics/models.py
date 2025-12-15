from django.db import models


class DatasetSnapshot(models.Model):
    title = models.CharField(max_length=255)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at"]
        verbose_name = "Срез данных"
        verbose_name_plural = "Срезы данных"

    def __str__(self):
        return self.title

