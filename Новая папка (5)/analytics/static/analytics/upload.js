(() => {
  const form = document.getElementById("csvUploadForm");
  const fileInput = document.getElementById("csvFile");
  const analyzeBtn = document.getElementById("analyzeBtn");
  const analyzeWrap = document.getElementById("analyzeBtnWrap");
  const messageEl = document.getElementById("validationMessage");
  const encodingMsg = document.getElementById("encodingMessage");
  const clientHintEl = document.getElementById("clientHint");
  const analysisProgress = document.getElementById("analysisProgress");
  const mapTs = document.getElementById("mapTsSelect");
  const mapCat = document.getElementById("mapCategorySelect");
  const mapVal = document.getElementById("mapValueSelect");

  if (!form || !fileInput || !analyzeBtn || !messageEl) return;

  const mapDefaults = window.__MAP_DEFAULTS__ || {};
  let analyzing = false;

  function setMessage(kind, text) {
    messageEl.textContent = text;
    if (kind === "error") messageEl.style.color = "#ff6b6b";
    if (kind === "ok") messageEl.style.color = "#a7f3d0";
    if (kind === "hint") messageEl.style.color = "";
  }

  function guessDelimiter(header) {
    const hasComma = header.includes(",");
    const hasSemicolon = header.includes(";");
    const hasTab = header.includes("\t");
    if (hasTab) return "\t";
    if (hasSemicolon && !hasComma) return ";";
    return ",";
  }

  function normalizeHeader(h) {
    return String(h).trim().replace(/^"|"$/g, "").toLowerCase();
  }

  function fillMappingSelects(headersDisplay, headersNorm) {
    const selects = [mapTs, mapCat, mapVal];
    const defaults = [
      mapDefaults.ts || "",
      mapDefaults.category || "",
      mapDefaults.value || "",
    ];
    const emptyLabels = ["Авто", "Авто / нет", "Авто"];
    selects.forEach((sel, idx) => {
      if (!sel) return;
      const cur = sel.value;
      sel.innerHTML = "";
      const opt0 = document.createElement("option");
      opt0.value = "";
      opt0.textContent = emptyLabels[idx];
      sel.appendChild(opt0);
      headersDisplay.forEach((disp, i) => {
        const opt = document.createElement("option");
        opt.value = headersNorm[i];
        opt.textContent = disp;
        sel.appendChild(opt);
      });
      if (defaults[idx] && headersNorm.includes(defaults[idx])) {
        sel.value = defaults[idx];
      } else if (cur && headersNorm.includes(normalizeHeader(cur))) {
        sel.value = normalizeHeader(cur);
      }
    });
  }

  function updateBtnTooltip() {
    if (!analyzeWrap) return;
    if (analyzing) {
      analyzeWrap.title = "Идёт анализ данных на сервере (Apache Spark)…";
      return;
    }
    if (analyzeBtn.disabled) {
      const f = fileInput.files && fileInput.files[0];
      if (!f) {
        analyzeWrap.title =
          "Кнопка неактивна: не выбран CSV-файл. Выберите файл для проверки формата.";
      } else {
        analyzeWrap.title =
          "Кнопка неактивна: выполняется проверка файла или обнаружены ошибки (см. сообщение выше).";
      }
      return;
    }
    analyzeWrap.title = "";
  }

  async function checkUtf8Sample(file) {
    const n = Math.min(file.size, 65536);
    const buf = await file.slice(0, n).arrayBuffer();
    try {
      new TextDecoder("utf-8", { fatal: true }).decode(buf);
      return true;
    } catch {
      return false;
    }
  }

  async function validateFile(file) {
    if (!file) return;

    if (encodingMsg) {
      encodingMsg.hidden = true;
      encodingMsg.textContent = "";
    }

    const name = file.name || "";
    if (!name.toLowerCase().endsWith(".csv")) {
      analyzeBtn.disabled = true;
      setMessage("error", "Загрузи файл с расширением .csv.");
      clientHintEl.textContent = "";
      updateBtnTooltip();
      return;
    }

    const utf8ok = await checkUtf8Sample(file);
    if (!utf8ok && encodingMsg) {
      encodingMsg.hidden = false;
      encodingMsg.textContent =
        "Предупреждение: начало файла не похоже на строгий UTF-8. Сохраните CSV в кодировке UTF-8, иначе анализ в Spark может быть некорректным или завершится ошибкой.";
    }

    const slice = file.slice(0, 64 * 1024);
    const text = await slice.text();
    const lines = text.split(/\r?\n/).filter((l) => l.trim().length > 0);
    if (lines.length < 2) {
      analyzeBtn.disabled = true;
      setMessage("error", "В CSV недостаточно строк (должны быть заголовок и минимум 1 строка данных).");
      clientHintEl.textContent = "";
      updateBtnTooltip();
      return;
    }

    const header = lines[0];
    const delimiter = guessDelimiter(header);
    const headersRaw = header.split(delimiter);
    const headersNorm = headersRaw.map(normalizeHeader);
    const headersDisplay = headersRaw.map((h) => String(h).trim().replace(/^"|"$/g, ""));

    fillMappingSelects(headersDisplay, headersNorm);

    analyzeBtn.disabled = false;
    setMessage("ok", "CSV прочитан. Можно запускать анализ или уточнить сопоставление колонок.");
    clientHintEl.textContent =
      "При необходимости укажите колонки даты и метрики вручную — так можно обойтись без имён ts / value.";
    updateBtnTooltip();
  }

  fileInput.addEventListener("change", () => {
    analyzeBtn.disabled = true;
    analyzing = false;
    setMessage("hint", "Проверяю файл…");
    clientHintEl.textContent = "";
    if (analysisProgress) {
      analysisProgress.hidden = true;
      analysisProgress.setAttribute("aria-busy", "false");
    }
    if (analyzeBtn) analyzeBtn.textContent = "Проанализировать";
    const file = fileInput.files && fileInput.files[0];
    validateFile(file).catch(() => {
      analyzeBtn.disabled = true;
      setMessage("error", "Не удалось прочитать CSV. Проверь, что файл корректный.");
      clientHintEl.textContent = "";
      updateBtnTooltip();
    });
  });

  form.addEventListener("submit", (e) => {
    if (analyzeBtn.disabled) {
      e.preventDefault();
      setMessage("error", "Сначала выберите CSV и дождитесь успешной проверки.");
      return;
    }
    analyzing = true;
    analyzeBtn.disabled = true;
    analyzeBtn.textContent = "Анализ…";
    if (analysisProgress) {
      analysisProgress.hidden = false;
      analysisProgress.setAttribute("aria-busy", "true");
    }
    updateBtnTooltip();
  });

  function initFromResultColumns() {
    const cols = window.__RESULT_COLUMNS__;
    if (!cols || !cols.length) return;
    const headersNorm = cols.map(normalizeHeader);
    fillMappingSelects(cols, headersNorm);
  }

  initFromResultColumns();
  updateBtnTooltip();
  if (fileInput.files && fileInput.files[0]) {
    validateFile(fileInput.files[0]).catch(() => {});
  }
})();
