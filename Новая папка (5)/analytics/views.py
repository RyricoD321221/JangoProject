import random
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

