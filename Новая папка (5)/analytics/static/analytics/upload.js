(() => {
  const form = document.getElementById("csvUploadForm");
  const fileInput = document.getElementById("csvFile");
  const analyzeBtn = document.getElementById("analyzeBtn");
  const messageEl = document.getElementById("validationMessage");
  const clientHintEl = document.getElementById("clientHint");

  if (!form || !fileInput || !analyzeBtn || !messageEl) return;

  const expected = (window.__EXPECTED_COLUMNS__ || []).map((s) => String(s).trim().toLowerCase());

  function setMessage(kind, text) {
    messageEl.textContent = text;
    if (kind === "error") messageEl.style.color = "#ff6b6b";
    if (kind === "ok") messageEl.style.color = "#a7f3d0";
    if (kind === "hint") messageEl.style.color = "";
  }

  function guessDelimiter(header) {
    // Берём наиболее вероятный разделитель по первой строке.
    const hasComma = header.includes(",");
    const hasSemicolon = header.includes(";");
    const hasTab = header.includes("\t");
    if (hasTab) return "\t";
    if (hasSemicolon && !hasComma) return ";";
    return ",";
  }

  function normalizeHeaders(headers) {
    return headers.map((h) => String(h).trim().replace(/^"|"$/g, "").toLowerCase());
  }

  async function validateFile(file) {
    if (!file) return;

    const name = file.name || "";
    if (!name.toLowerCase().endsWith(".csv")) {
      analyzeBtn.disabled = true;
      setMessage("error", "Загрузи файл с расширением .csv.");
      clientHintEl.textContent = "";
      return;
    }

    // Читаем только начало файла, чтобы быстро валидировать.
    const slice = file.slice(0, 64 * 1024);
    const text = await slice.text();
    const lines = text.split(/\r?\n/).filter((l) => l.trim().length > 0);
    if (lines.length < 2) {
      analyzeBtn.disabled = true;
      setMessage("error", "В CSV недостаточно строк (должны быть заголовок и минимум 1 строка данных).");
      clientHintEl.textContent = "";
      return;
    }

    const header = lines[0];
    const delimiter = guessDelimiter(header);

    const headersRaw = header.split(delimiter);
    const headers = normalizeHeaders(headersRaw);

    analyzeBtn.disabled = false;
    setMessage("ok", "CSV прочитан. Можно запускать анализ.");
    const hasBase = expected.every((c) => headers.includes(c));
    clientHintEl.textContent = hasBase
      ? "Найдены базовые колонки ts/category/value."
      : "Базовые колонки ts/category/value не найдены — будет выполнен адаптивный анализ по всем колонкам.";
  }

  fileInput.addEventListener("change", () => {
    analyzeBtn.disabled = true;
    setMessage("hint", "Проверяю файл...");
    clientHintEl.textContent = "";
    const file = fileInput.files && fileInput.files[0];
    validateFile(file).catch(() => {
      analyzeBtn.disabled = true;
      setMessage("error", "Не удалось прочитать CSV. Проверь, что файл корректный.");
      clientHintEl.textContent = "";
    });
  });

  form.addEventListener("submit", (e) => {
    if (analyzeBtn.disabled) {
      e.preventDefault();
      setMessage("error", "Сначала убедись, что CSV прошёл проверку формата.");
    } else {
      analyzeBtn.disabled = true;
      analyzeBtn.textContent = "Анализ запускается...";
    }
  });
})();

