(() => {
  const labels = timeseries.map(p => p.ts);
  const values = timeseries.map(p => p.value);

  const ctx = document.getElementById("trendChart");
  if (!ctx) return;

  new Chart(ctx, {
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label: "Среднее значение за час",
          data: values,
          borderColor: "#f5f5f5",
          backgroundColor: "rgba(245, 245, 245, 0.08)",
          tension: 0.3,
          fill: true,
        },
      ],
    },
    options: {
      plugins: {
        legend: { labels: { color: "#e5e7eb" } },
      },
      scales: {
        x: { ticks: { color: "#9ca3af" }, grid: { color: "rgba(255,255,255,0.05)" } },
        y: { ticks: { color: "#9ca3af" }, grid: { color: "rgba(255,255,255,0.05)" } },
      },
    },
  });
})();

