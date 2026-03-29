import random
import io
import os
import tempfile
from datetime import datetime, timedelta

import pandas as pd
from django.contrib.auth import login, logout
from django.contrib.auth.decorators import login_required
from django.contrib.auth.forms import AuthenticationForm
from django.shortcuts import render, redirect

from .forms import RegistrationForm

NEWS_ITEMS = [
    {
        "title": "Новый поток данных",
        "excerpt": "Подключили свежий источник событий для тестовых дашбордов.",
        "tag": "Обновление",
        "date": "15.12.2025",
    },
    {
        "title": "Библиотека визуализаций",
        "excerpt": "Добавили библиотеку компонентов на базе Chart.js и ECharts.",
        "tag": "UI",
        "date": "10.12.2025",
    },
    {
        "title": "Оптимизация запросов",
        "excerpt": "Сократили время отклика аналитики на 30% благодаря кешам.",
        "tag": "Performance",
        "date": "05.12.2025",
    },
]


def _generate_sample_dataframe(user, rows: int = 1000) -> pd.DataFrame:
    """Create a small synthetic dataset to demonstrate processing.

    Используем seed на основе пользователя, чтобы набор данных отличался.
    """
    seed = f"user-{getattr(user, 'pk', None) or user.username}"
    rng = random.Random(seed)

    now = datetime.utcnow()
    timestamps = [now - timedelta(minutes=i) for i in range(rows)]
    categories = ["stream", "batch", "api"]
    data = {
        "ts": timestamps,
        "category": [rng.choice(categories) for _ in range(rows)],
        "value": [rng.gauss(100, 25) for _ in range(rows)],
    }
    df = pd.DataFrame(data)
    df["value"] = df["value"].clip(lower=0)
    return df


@login_required
def dashboard(request):
    latest_analysis = request.session.get("latest_analysis")
    if latest_analysis:
        summary = latest_analysis.get("summary", {})
        by_category = latest_analysis.get("by_category", [])
        timeseries = latest_analysis.get("timeseries", [])
        context = {
            "summary": {
                "rows": summary.get("rows", 0),
                "avg": summary.get("avg", "—"),
                "max": summary.get("max", "—"),
                "min": summary.get("min", "—"),
            },
            "by_category": by_category,
            "timeseries": timeseries,
            "dashboard_mode": "real",
            "analysis_meta": request.session.get("latest_analysis_meta", {}),
        }
    else:
        df = _generate_sample_dataframe(request.user)

        summary = {
            "rows": int(df.shape[0]),
            "avg": round(df["value"].mean(), 2),
            "max": round(df["value"].max(), 2),
            "min": round(df["value"].min(), 2),
        }

        by_category = (
            df.groupby("category")["value"]
            .agg(["count", "mean"])
            .reset_index()
            .rename(columns={"count": "rows", "mean": "avg_value"})
        )

        by_time = (
            df.set_index("ts")
            .resample("1H")
            .agg({"value": "mean"})
            .reset_index()
            .tail(24)
        )

        context = {
            "summary": summary,
            "by_category": by_category.to_dict(orient="records"),
            "timeseries": [
                {"ts": row.ts.strftime("%Y-%m-%d %H:%M"), "value": round(row.value, 2)}
                for row in by_time.itertuples()
            ],
            "dashboard_mode": "demo",
            "analysis_meta": {},
        }

    return render(request, "analytics/dashboard.html", context)


def news(request):
    hero = {
        "title": "Лента обновлений",
        "subtitle": "Свежие заметки о том, что происходит с платформой аналитики.",
        "cta": "Перейти к дашборду",
    }
    context = {"news": NEWS_ITEMS, "hero": hero}
    return render(request, "analytics/news.html", context)


@login_required
def cabinet(request):
    df = _generate_sample_dataframe(request.user, rows=120)
    recent_stats = df.tail(10)

    account_info = {
        "username": request.user.username,
        "email": getattr(request.user, "email", "") or "не указан",
        "first_name": getattr(request.user, "first_name", "") or "—",
        "last_name": getattr(request.user, "last_name", "") or "—",
        "joined": getattr(request.user, "date_joined", None),
        "last_login": getattr(request.user, "last_login", None),
    }

    activity = {
        "rows_24h": int(df.shape[0]),
        "avg_24h": round(df["value"].mean(), 2),
        "last_values": [
            {
                "ts": row.ts.strftime("%H:%M"),
                "value": round(row.value, 2),
                "category": row.category,
            }
            for row in recent_stats.itertuples()
        ],
    }

    context = {"account": account_info, "activity": activity}
    return render(request, "analytics/cabinet.html", context)


def register(request):
    if request.user.is_authenticated:
        return redirect("analytics:cabinet")

    if request.method == "POST":
        form = RegistrationForm(request.POST)
        if form.is_valid():
            user = form.save()
            login(request, user)
            return redirect("analytics:cabinet")
    else:
        form = RegistrationForm()

    return render(request, "registration/register.html", {"form": form})


def logout_view(request):
    logout(request)
    return redirect("analytics:news")


def _validate_csv_preview(uploaded_file):
    """
    Проверяет формат CSV по заголовку и типам на небольшом сэмпле строк.
    Возвращает (error_message: str|None, preview_info: dict|None).
    """
    uploaded_file.seek(0)
    raw_sample = uploaded_file.read(64 * 1024)  # первые ~64KB обычно достаточно для проверки

    # pandas иногда падает на bytes при sep=None, поэтому декодируем в текст.
    if isinstance(raw_sample, bytes):
        decoded = None
        for enc in ("utf-8-sig", "utf-8", "cp1251", "latin1"):
            try:
                decoded = raw_sample.decode(enc)
                break
            except Exception:
                continue
        if decoded is None:
            return "Не удалось декодировать CSV. Попробуй UTF-8.", None
    else:
        # если вдруг Django вернул строку
        decoded = str(raw_sample)

    try:
        # sep=None + engine="python" позволяет pandas попробовать угадать разделитель (; или ,).
        df = pd.read_csv(io.StringIO(decoded), nrows=20, sep=None, engine="python")
    except Exception as exc:
        return f"Не удалось прочитать CSV. Ошибка парсинга: {exc}", None

    if df.empty:
        return "CSV-файл пустой (нет строк данных).", None

    # Адаптивный режим: обязательных колонок нет.
    if len(df.columns) < 1:
        return "В CSV не удалось определить колонки.", None

    preview_info = {
        "columns": [str(c) for c in df.columns],
        "preview_rows": int(df.shape[0]),
    }
    # Восстанавливаем указатель, чтобы дальше можно было прочитать файл заново.
    uploaded_file.seek(0)
    return None, preview_info


@login_required
def upload_csv(request):
    error = None
    success = None
    results = None
    top_n = 5
    show_all_string_values = False
    map_ts = ""
    map_category = ""
    map_value = ""
    if request.method == "POST":
        try:
            top_n = max(1, int(request.POST.get("top_n", "5")))
        except Exception:
            top_n = 5
        show_all_string_values = request.POST.get("show_all_string_values") == "on"
        map_ts = (request.POST.get("map_ts") or "").strip().lower()
        map_category = (request.POST.get("map_category") or "").strip().lower()
        map_value = (request.POST.get("map_value") or "").strip().lower()

        uploaded = request.FILES.get("csv_file")
        if not uploaded:
            error = "Не удалось получить файл. Выбери CSV и попробуй снова."
        else:
            # Файл будет читаться в двух местах: сначала для проверки, затем для передачи в Spark.
            uploaded.seek(0)
            err, preview_info = _validate_csv_preview(uploaded)
            if err:
                error = err
            else:
                tmp_path = None
                try:
                    # Сохраняем файл во временный путь, чтобы Spark смог его прочитать.
                    with tempfile.NamedTemporaryFile(mode="wb", suffix=".csv", delete=False) as tmp:
                        for chunk in uploaded.chunks():
                            tmp.write(chunk)
                        tmp_path = tmp.name

                    from .spark_service import (
                        analyze_csv_with_spark,
                        SparkDockerUnavailableError,
                        SparkInvalidCsvError,
                        SparkExecutionTimeoutError,
                    )

                    column_mapping = {
                        "ts": map_ts,
                        "category": map_category,
                        "value": map_value,
                    }
                    results = analyze_csv_with_spark(
                        tmp_path,
                        top_n=top_n,
                        show_all_string_values=show_all_string_values,
                        column_mapping=column_mapping,
                    )
                    request.session["latest_analysis"] = {
                        "summary": results.get("summary", {}),
                        "by_category": results.get("by_category", []),
                        "timeseries": results.get("timeseries", []),
                    }
                    request.session["latest_analysis_meta"] = {
                        "top_n": top_n,
                        "show_all_string_values": show_all_string_values,
                        "columns_count": results.get("columns_count"),
                    }
                    success = "CSV прошёл проверку и успешно обработан Apache Spark."
                except SparkInvalidCsvError as exc:
                    error = f"Некорректные данные CSV: {exc}"
                except SparkDockerUnavailableError as exc:
                    error = f"Spark/Docker недоступен: {exc}"
                except SparkExecutionTimeoutError as exc:
                    error = f"Таймаут Spark-анализа: {exc}"
                except Exception as exc:
                    error = f"Ошибка при запуске анализа Spark: {exc}"
                finally:
                    # Удаляем временный файл, чтобы не копить загрузки.
                    if tmp_path and os.path.exists(tmp_path):
                        try:
                            os.remove(tmp_path)
                        except Exception:
                            pass

    return render(
        request,
        "analytics/upload.html",
        {
            "error": error,
            "success": success,
            "results": results,
            "top_n": top_n,
            "show_all_string_values": show_all_string_values,
            "map_ts": map_ts,
            "map_category": map_category,
            "map_value": map_value,
        },
    )

