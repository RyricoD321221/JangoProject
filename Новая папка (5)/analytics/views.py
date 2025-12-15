import random
from datetime import datetime, timedelta

import pandas as pd
from django.contrib.auth import login
from django.contrib.auth.decorators import login_required
from django.contrib.auth.forms import UserCreationForm
from django.shortcuts import render, redirect


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


def register(request):
    if request.user.is_authenticated:
        return redirect("analytics:dashboard")

    if request.method == "POST":
        form = UserCreationForm(request.POST)
        if form.is_valid():
            user = form.save()
            login(request, user)
            return redirect("analytics:dashboard")
    else:
        form = UserCreationForm()

    return render(request, "registration/register.html", {"form": form})

