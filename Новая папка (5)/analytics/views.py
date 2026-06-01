import json
import random
import io
import os
import re
import tempfile
from datetime import datetime, timedelta
from typing import Any, Dict, List

import pandas as pd
from django.contrib import messages
from django.contrib.auth import login, logout
from django.contrib.auth.decorators import login_required
from django.http import HttpResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone

from .forms import RegistrationForm
from .models import CsvAnalysisRun

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
        summary_ctx = {
            "rows": summary.get("rows", 0),
            "avg": summary.get("avg", "—"),
            "max": summary.get("max", "—"),
            "min": summary.get("min", "—"),
        }
        if "sum" in summary:
            summary_ctx["sum"] = summary.get("sum")
        context = {
            "summary": summary_ctx,
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
    account_info = {
        "username": request.user.username,
        "email": getattr(request.user, "email", "") or "не указан",
        "first_name": getattr(request.user, "first_name", "") or "—",
        "last_name": getattr(request.user, "last_name", "") or "—",
        "joined": getattr(request.user, "date_joined", None),
        "last_login": getattr(request.user, "last_login", None),
    }

    runs = CsvAnalysisRun.objects.filter(user=request.user).order_by("-created_at")[:100]
    analysis_history = []
    for run in runs:
        payload = run.results_payload or {}
        summ = payload.get("summary") or {}
        analysis_history.append(
            {
                "id": run.pk,
                "created_at": run.created_at,
                "original_filename": run.original_filename,
                "rows": summ.get("rows"),
                "columns_count": payload.get("columns_count"),
            }
        )

    context = {"account": account_info, "analysis_history": analysis_history}
    return render(request, "analytics/cabinet.html", context)


@login_required
def analysis_detail(request, pk):
    record = get_object_or_404(CsvAnalysisRun, pk=pk, user=request.user)
    return render(
        request,
        "analytics/analysis_detail.html",
        {
            "record": record,
            "results": record.results_payload,
            "saved_analysis_id": record.pk,
            "analysis_detail_mode": True,
        },
    )


@login_required
def analysis_export(request, pk):
    record = get_object_or_404(CsvAnalysisRun, pk=pk, user=request.user)
    export_doc = {
        "exported_at": timezone.now().isoformat(),
        "analysis_id": record.pk,
        "original_filename": record.original_filename,
        "created_at": record.created_at.isoformat(),
        "options": {
            "top_n": record.top_n,
            "show_all_string_values": record.show_all_string_values,
            "column_mapping": record.column_mapping,
        },
        "results": record.results_payload,
    }
    fmt = (request.GET.get("format") or "json").strip().lower()
    if fmt not in ("json", "txt"):
        fmt = "json"

    if fmt == "txt":
        raw = _render_analysis_txt(export_doc)
        response = HttpResponse(raw, content_type="text/plain; charset=utf-8")
    else:
        raw = json.dumps(export_doc, ensure_ascii=False, indent=2)
        response = HttpResponse(raw, content_type="application/json; charset=utf-8")

    stub = re.sub(r"[^\w\-.]+", "_", record.original_filename, flags=re.UNICODE).strip(
        "_"
    )[:80] or "data"
    ext = "txt" if fmt == "txt" else "json"
    filename = f"analysis_{record.pk}_{record.created_at:%Y%m%d_%H%M}_{stub}.{ext}"
    response["Content-Disposition"] = f'attachment; filename="{filename}"'
    return response


def _render_analysis_txt(export_doc: Dict[str, Any]) -> str:
    """Читаемый текстовый экспорт (без требований к обратному парсингу)."""

    def line() -> str:
        return "-" * 72

    def fmt_dt(s: Any) -> str:
        return str(s) if s is not None else "—"

    def fmt_val(v: Any) -> str:
        if v is None:
            return "—"
        if isinstance(v, bool):
            return "да" if v else "нет"
        if isinstance(v, (int,)):
            return str(v)
        if isinstance(v, float):
            return f"{v:.4f}".rstrip("0").rstrip(".")
        return str(v)

    def section(title: str, out: List[str]) -> None:
        out.append(title)
        out.append(line())

    out: List[str] = []
    results = export_doc.get("results") or {}
    options = export_doc.get("options") or {}

    section("ОТЧЁТ АНАЛИЗА CSV (Apache Spark)", out)
    out.append(f"Дата экспорта: {fmt_dt(export_doc.get('exported_at'))}")
    out.append(f"ID анализа: {fmt_val(export_doc.get('analysis_id'))}")
    out.append(f"Файл: {fmt_val(export_doc.get('original_filename'))}")
    out.append(f"Дата запуска: {fmt_dt(export_doc.get('created_at'))}")
    out.append("")

    section("ПАРАМЕТРЫ ЗАПУСКА", out)
    out.append(f"Top-N: {fmt_val(options.get('top_n'))}")
    out.append(f"Показывать все строковые значения: {fmt_val(options.get('show_all_string_values'))}")
    mapping = (results.get("column_mapping_resolved") or options.get("column_mapping") or {}) or {}
    out.append(
        "Колонки (ts/category/value): "
        f"{fmt_val(mapping.get('ts'))} / {fmt_val(mapping.get('category'))} / {fmt_val(mapping.get('value'))}"
    )
    out.append("")

    warnings = results.get("warnings") or []
    if warnings:
        section("ПРЕДУПРЕЖДЕНИЯ", out)
        for w in warnings:
            out.append(f"- {w}")
        out.append("")

    section("ОБЩИЕ ПОКАЗАТЕЛИ", out)
    summary = results.get("summary") or {}
    out.append(f"Строк (в сводке): {fmt_val(summary.get('rows'))}")
    out.append(f"Строк в файле (всего): {fmt_val(results.get('total_rows_file'))}")
    out.append(f"Колонок: {fmt_val(results.get('columns_count'))}")
    if "sum" in summary:
        out.append(f"Сумма (по выбранной метрике): {fmt_val(summary.get('sum'))}")
    if summary.get("avg") is not None:
        out.append(f"Среднее: {fmt_val(summary.get('avg'))}")
        out.append(f"Мин: {fmt_val(summary.get('min'))}")
        out.append(f"Макс: {fmt_val(summary.get('max'))}")
    out.append("")

    schema = results.get("schema") or []
    if schema:
        section("СХЕМА (КОЛОНКА — ТИП)", out)
        for col in schema:
            name = col.get("display_name") or col.get("name") or "—"
            typ = col.get("type") or "—"
            out.append(f"- {name}: {typ}")
        out.append("")

    numeric_stats = results.get("numeric_stats") or []
    if numeric_stats:
        section("ЧИСЛОВЫЕ ДАННЫЕ (СВОДКА)", out)
        for r in numeric_stats:
            col = r.get("column_ru") or r.get("column") or "—"
            out.append(
                f"* {col}: count={fmt_val(r.get('count'))}, sum={fmt_val(r.get('sum'))}, "
                f"avg={fmt_val(r.get('avg'))}, median={fmt_val(r.get('median'))}, "
                f"min={fmt_val(r.get('min'))}, max={fmt_val(r.get('max'))}"
            )
        out.append("")

    # Частоты по категориям
    by_category = results.get("by_category") or []
    if by_category:
        section("КАТЕГОРИАЛЬНЫЕ ДАННЫЕ (TOP)", out)
        for r in by_category[:50]:
            out.append(
                f"- {fmt_val(r.get('category'))}: rows={fmt_val(r.get('rows'))}, "
                f"avg_value={fmt_val(r.get('avg_value'))}"
            )
        if len(by_category) > 50:
            out.append(f"... ещё {len(by_category) - 50} строк(и) не показаны")
        out.append("")

    # Временные ряды (коротко)
    ts_by = results.get("timeseries_by_granularity") or {}
    if ts_by:
        section("ДАННЫЕ ПО ДАТАМ (КРАТКО)", out)
        for unit in ("day", "month", "year", "hour"):
            rows = ts_by.get(unit) or []
            if not rows:
                continue
            out.append(f"[{unit}] точек: {len(rows)}")
            for r in rows[:50]:
                out.append(
                    f"  - {fmt_val(r.get('ts'))}: rows={fmt_val(r.get('rows'))}, "
                    f"sum={fmt_val(r.get('sum_value'))}, avg={fmt_val(r.get('value'))}"
                )
            if len(rows) > 50:
                out.append(f"  ... ещё {len(rows) - 50} точек не показаны")
        out.append("")

    # Топы значений по тексту/числам (как в UI)
    string_top = results.get("string_top_values") or {}
    if string_top:
        section("УНИКАЛЬНЫЕ ЗНАЧЕНИЯ (ТЕКСТОВЫЕ КОЛОНКИ)", out)
        for col_name, block in list(string_top.items())[:50]:
            col_ru = block.get("column_ru") or col_name
            mode = block.get("mode")
            out.append(f"* {col_ru} (mode={mode}, unique={fmt_val(block.get('unique_count'))})")
            if mode in ("small", "medium"):
                for item in (block.get("values") or [])[:50]:
                    if item.get("type") == "other":
                        out.append(f"  - Другие: {fmt_val(item.get('other_unique_count'))} уникальных")
                    else:
                        out.append(
                            f"  - {fmt_val(item.get('value'))}: {fmt_val(item.get('count'))} "
                            f"({fmt_val(item.get('pct_of_all_rows'))}%)"
                        )
        out.append("")

    numeric_top = results.get("numeric_top_values") or {}
    if numeric_top:
        section("УНИКАЛЬНЫЕ ЗНАЧЕНИЯ (ЧИСЛОВЫЕ КОЛОНКИ)", out)
        for col_name, block in list(numeric_top.items())[:50]:
            col_ru = block.get("column_ru") or col_name
            mode = block.get("mode")
            out.append(f"* {col_ru} (mode={mode}, unique={fmt_val(block.get('unique_count'))})")
            if mode in ("small", "medium"):
                for item in (block.get("values") or [])[:50]:
                    if item.get("type") == "other":
                        out.append(f"  - Другие: {fmt_val(item.get('other_unique_count'))} уникальных")
                    else:
                        out.append(
                            f"  - {fmt_val(item.get('value'))}: {fmt_val(item.get('count'))} "
                            f"({fmt_val(item.get('pct_of_all_rows'))}%)"
                        )
        out.append("")

    return "\n".join(out).rstrip() + "\n"


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
    saved_analysis_id = None
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
                    try:
                        run = CsvAnalysisRun.objects.create(
                            user=request.user,
                            original_filename=(
                                (getattr(uploaded, "name", None) or "upload.csv")
                            )[:255],
                            top_n=top_n,
                            show_all_string_values=show_all_string_values,
                            column_mapping=column_mapping,
                            results_payload=results,
                        )
                        saved_analysis_id = run.pk
                    except Exception as exc:
                        messages.warning(
                            request,
                            "Анализ выполнен, но сохранить историю в базе не удалось: %s" % exc,
                        )
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
            "saved_analysis_id": saved_analysis_id,
            "top_n": top_n,
            "show_all_string_values": show_all_string_values,
            "map_ts": map_ts,
            "map_category": map_category,
            "map_value": map_value,
        },
    )

