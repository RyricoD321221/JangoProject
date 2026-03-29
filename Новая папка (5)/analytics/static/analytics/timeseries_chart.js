(() => {
  const rawEl = document.getElementById("spark-ts-gran-json");
  const granular = rawEl ? JSON.parse(rawEl.textContent || "{}") : {};
  const ctx = document.getElementById("timeChart");
  const scaleSelect = document.getElementById("timeScaleSelect");
  const aggSelect = document.getElementById("timeAggSelect");
  if (!ctx || typeof Chart === "undefined") return;

  const X_POINT_WARN = 50;

  function seriesFor(scale, agg) {
    const arr = granular[scale];
    if (!Array.isArray(arr)) return [];
    return arr
      .map((p) => {
        if (agg === "sum") return { ts: p.ts, y: Number(p.sum_value) };
        if (agg === "count") return { ts: p.ts, y: Number(p.rows) };
        return { ts: p.ts, y: Number(p.value) };
      })
      .filter((p) => p.ts && Number.isFinite(p.y));
  }

  function fmt(x) {
    return Number.isFinite(x) ? x.toFixed(2) : "—";
  }

  function updateScaleAvailability() {
    if (!scaleSelect) return;
    const nHour = (granular.hour && granular.hour.length) || 0;
    const nDay = (granular.day && granular.day.length) || 0;
    const optHour = scaleSelect.querySelector('option[value="hour"]');
    const optDay = scaleSelect.querySelector('option[value="day"]');
    if (optHour) {
      optHour.disabled = nHour > X_POINT_WARN;
      optHour.title = optHour.disabled
        ? `Слишком много временных точек (${nHour} > ${X_POINT_WARN}). Выберите день или крупнее.`
        : "";
    }
    if (optDay) {
      optDay.disabled = nDay > X_POINT_WARN;
      optDay.title = optDay.disabled
        ? `Слишком много дней (${nDay} > ${X_POINT_WARN}). Выберите месяц или год.`
        : "";
    }
    if (scaleSelect.value === "hour" && optHour && optHour.disabled) scaleSelect.value = "day";
    if (scaleSelect.value === "day" && optDay && optDay.disabled) scaleSelect.value = "month";
  }

  let chart = null;
  const render = () => {
    updateScaleAvailability();
    const scale = (scaleSelect && scaleSelect.value) || "day";
    const agg = (aggSelect && aggSelect.value) || "avg";
    const data = seriesFor(scale, agg);
    const values = data.map((p) => p.y).filter(Number.isFinite);
    const sum = values.reduce((a, v) => a + v, 0);
    const avg = values.length ? sum / values.length : NaN;
    const min = values.length ? Math.min(...values) : NaN;
    const max = values.length ? Math.max(...values) : NaN;

    const set = (id, val) => {
      const el = document.getElementById(id);
      if (el) el.textContent = val;
    };
    set("chartStatCount", String(values.length));
    set("chartStatSum", fmt(sum));
    set("chartStatAvg", fmt(avg));
    set("chartStatMin", fmt(min));
    set("chartStatMax", fmt(max));

    if (chart) chart.destroy();
    const aggLabels = { avg: "Среднее", sum: "Сумма", count: "Количество записей" };
    chart = new Chart(ctx, {
      type: "line",
      data: {
        labels: data.map((p) => p.ts),
        datasets: [
          {
            label: `${aggLabels[agg] || "Среднее"} (${scale})`,
            data: data.map((p) => p.y),
            borderColor: "#f5f5f5",
            backgroundColor: "rgba(245, 245, 245, 0.08)",
            fill: true,
            tension: 0.3,
          },
        ],
      },
      options: {
        plugins: { legend: { labels: { color: "#e5e7eb" } } },
        scales: {
          x: {
            ticks: {
              color: "#9ca3af",
              maxRotation: 45,
              minRotation: 0,
              autoSkip: true,
              maxTicksLimit: 24,
            },
            grid: { color: "rgba(255,255,255,0.05)" },
          },
          y: { ticks: { color: "#9ca3af" }, grid: { color: "rgba(255,255,255,0.05)" } },
        },
      },
    });
  };

  if (Object.keys(granular).length) {
    if (scaleSelect) scaleSelect.addEventListener("change", render);
    if (aggSelect) aggSelect.addEventListener("change", render);
    render();
  }
})();
