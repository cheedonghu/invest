const uiText = {
  loading: "\u52a0\u8f7d\u4e2d...",
  save: "\u4fdd\u5b58\u6807\u8bb0",
  refresh: "\u5237\u65b0\u5217\u8868",
  saving: "\u4fdd\u5b58\u4e2d...",
  invalidSymbol: "\u8bf7\u8f93\u5165 6 \u4f4d\u80a1\u7968\u4ee3\u7801",
  emptyReason: "\u8bf7\u8f93\u5165\u6807\u8bb0\u539f\u56e0",
  requestFailed: "\u63a5\u53e3\u8bf7\u6c42\u5931\u8d25",
  noData: "\u6682\u65f6\u6ca1\u6709\u5df2\u6807\u8bb0\u7684\u80a1\u7968",
  latestUpdatedFallback: "-",
  updated: "\u5df2\u66f4\u65b0",
  saveRow: "\u4fdd\u5b58",
};

const markedCountElement = document.getElementById("marked-count");
const markedLatestUpdatedElement = document.getElementById("marked-latest-updated");
const tableBody = document.getElementById("marked-table-body");
const emptyStateElement = document.getElementById("marked-empty");
const markedForm = document.getElementById("marked-stock-form");
const markedSymbolInput = document.getElementById("marked-symbol");
const markedReasonInput = document.getElementById("marked-reason");
const markedSaveButton = document.getElementById("marked-save-btn");
const markedRefreshButton = document.getElementById("marked-refresh-btn");
const markedFeedbackElement = document.getElementById("marked-feedback");

function sanitizeSymbolInput(value) {
  return value.replace(/\D/g, "").slice(0, 6);
}

function normalizeReason(value) {
  return String(value || "").trim();
}

function escapeHtml(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function setFeedback(message, type = "info") {
  markedFeedbackElement.textContent = message || "";
  markedFeedbackElement.dataset.state = type;
}

function setFormLoading(loading) {
  markedSaveButton.disabled = loading;
  markedRefreshButton.disabled = loading;
  markedSaveButton.textContent = loading ? uiText.saving : uiText.save;
  markedRefreshButton.textContent = loading ? uiText.loading : uiText.refresh;
}

function buildRow(item) {
  const row = document.createElement("tr");
  const symbol = escapeHtml(item.symbol);
  const name = escapeHtml(item.name || "-");
  const markReason = escapeHtml(item.mark_reason || "");
  const createdAt = escapeHtml(item.created_at || "-");
  const updatedAt = escapeHtml(item.updated_at || "-");
  const rawName = escapeHtml(item.name || "");
  row.innerHTML = `
    <td><strong>${symbol}</strong></td>
    <td>${name}</td>
    <td>
      <label class="sr-only" for="reason-${symbol}">${symbol}</label>
      <textarea id="reason-${symbol}" class="table-reason-input" rows="3">${markReason}</textarea>
    </td>
    <td>${createdAt}</td>
    <td>${updatedAt}</td>
    <td>
      <button type="button" class="table-save-btn secondary-btn" data-symbol="${symbol}" data-name="${rawName}">${uiText.saveRow}</button>
    </td>
  `;
  return row;
}

function renderTable(items) {
  tableBody.innerHTML = "";
  emptyStateElement.hidden = items.length > 0;
  if (!items.length) {
    markedCountElement.textContent = "0";
    markedLatestUpdatedElement.textContent = uiText.latestUpdatedFallback;
    return;
  }

  items.forEach((item) => tableBody.appendChild(buildRow(item)));
  markedCountElement.textContent = String(items.length);
  markedLatestUpdatedElement.textContent = items[0]?.updated_at || uiText.latestUpdatedFallback;
}

async function fetchMarkedStocks() {
  setFormLoading(true);
  try {
    const response = await fetch("/api/stocks/marked/");
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.detail || `${uiText.requestFailed}: ${response.status}`);
    }
    renderTable(payload.items || []);
  } catch (error) {
    renderTable([]);
    setFeedback(error.message, "error");
  } finally {
    setFormLoading(false);
  }
}

async function upsertMarkedStock({ symbol, mark_reason, name }, successMessage) {
  const response = await fetch("/api/stocks/marked/", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ symbol, mark_reason, name }),
  });
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.detail || `${uiText.requestFailed}: ${response.status}`);
  }
  setFeedback(successMessage || `${payload.symbol} ${uiText.updated}`, "success");
  await fetchMarkedStocks();
  return payload;
}

markedSymbolInput.addEventListener("input", (event) => {
  event.target.value = sanitizeSymbolInput(event.target.value);
});

markedForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  const symbol = sanitizeSymbolInput(markedSymbolInput.value);
  const markReason = normalizeReason(markedReasonInput.value);

  if (symbol.length !== 6) {
    setFeedback(uiText.invalidSymbol, "error");
    return;
  }
  if (!markReason) {
    setFeedback(uiText.emptyReason, "error");
    return;
  }

  setFormLoading(true);
  try {
    await upsertMarkedStock({ symbol, mark_reason: markReason }, `${symbol} ${uiText.updated}`);
    markedSymbolInput.value = symbol;
  } catch (error) {
    setFeedback(error.message, "error");
  } finally {
    setFormLoading(false);
  }
});

markedRefreshButton.addEventListener("click", async () => {
  setFeedback("", "info");
  await fetchMarkedStocks();
});

tableBody.addEventListener("click", async (event) => {
  const target = event.target;
  if (!(target instanceof HTMLButtonElement) || !target.dataset.symbol) {
    return;
  }

  const symbol = target.dataset.symbol;
  const name = target.dataset.name || undefined;
  const reasonInput = document.getElementById(`reason-${symbol}`);
  const markReason = normalizeReason(reasonInput?.value || "");
  if (!markReason) {
    setFeedback(`${symbol}: ${uiText.emptyReason}`, "error");
    return;
  }

  const previousText = target.textContent;
  target.disabled = true;
  target.textContent = uiText.saving;
  try {
    await upsertMarkedStock({ symbol, mark_reason: markReason, name }, `${symbol} ${uiText.updated}`);
  } catch (error) {
    setFeedback(error.message, "error");
  } finally {
    target.disabled = false;
    target.textContent = previousText;
  }
});

fetchMarkedStocks();
