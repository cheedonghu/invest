const markedCountElement = document.getElementById("marked-count");
const detailElement = document.getElementById("marked-detail");
const directoryList = document.getElementById("marked-list");
const directory = document.getElementById("marked-directory");
const directorySummary = document.getElementById("marked-directory-summary");
const addDialog = document.getElementById("marked-add-dialog");
const closeDialogButton = document.getElementById("marked-dialog-close");
const dialogFeedback = document.getElementById("marked-dialog-feedback");
const newNoteButton = document.getElementById("marked-new-btn");
const emptyStateElement = document.getElementById("marked-empty");
const resultCountElement = document.getElementById("marked-result-count");
const markedForm = document.getElementById("marked-stock-form");
const markedSymbolInput = document.getElementById("marked-symbol");
const newNoteEditor = StockNotes.mount(document.getElementById("marked-note-editor"), { id: "marked-reason" });
const markedReasonInput = newNoteEditor.input;
const markedSaveButton = document.getElementById("marked-save-btn");
const markedRefreshButton = document.getElementById("marked-refresh-btn");
const markedFeedbackElement = document.getElementById("marked-feedback");
const searchInput = document.getElementById("marked-search");
const sortInput = document.getElementById("marked-sort");

let markedItems = [];
let isFetching = false;
let isAdding = false;
let hasLoaded = false;
let selectedSymbol = null;
const drafts = new Map();
const savingSymbols = new Set();
const deletingSymbols = new Set();
const noteModes = new Map();

function sanitizeSymbolInput(value) {
  return value.replace(/\D/g, "").slice(0, 6);
}

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, (char) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[char]);
}

function setFeedback(message, type = "info") {
  markedFeedbackElement.textContent = message || "";
  markedFeedbackElement.dataset.state = type;
  dialogFeedback.textContent = message || "";
  dialogFeedback.dataset.state = type;
}

function updateBusyState() {
  const busy = isFetching || isAdding || savingSymbols.size > 0 || deletingSymbols.size > 0;
  markedSaveButton.disabled = busy;
  markedRefreshButton.disabled = busy;
  markedSaveButton.textContent = isAdding ? "保存中…" : "保存备注";
  markedRefreshButton.textContent = isFetching ? "加载中…" : "刷新";
  newNoteButton.disabled = busy;
  closeDialogButton.disabled = isAdding;
  directoryList.setAttribute("aria-busy", String(isFetching));
  detailElement.setAttribute("aria-busy", String(isFetching));
  detailElement.querySelectorAll(".note-save-btn").forEach((button) => {
    const symbol = button.dataset.symbol;
    button.disabled = isFetching || isAdding || savingSymbols.has(symbol) || deletingSymbols.has(symbol) || !drafts.has(symbol);
    button.textContent = savingSymbols.has(symbol) ? "保存中…" : drafts.has(symbol) ? "保存修改" : "已保存";
    button.hidden = !drafts.has(symbol) && (noteModes.get(symbol) ?? true);
  });
  detailElement.querySelectorAll(".note-delete-btn").forEach((button) => {
    const symbol = button.dataset.symbol;
    button.disabled = isFetching || isAdding || savingSymbols.has(symbol) || deletingSymbols.has(symbol);
    button.textContent = deletingSymbols.has(symbol) ? "删除中…" : "删除";
  });
  detailElement.querySelectorAll("article[data-symbol]").forEach((row) => {
    row.querySelectorAll(".note-editor button, textarea, .document-edit-btn").forEach((control) => {
      control.disabled = deletingSymbols.has(row.dataset.symbol);
    });
  });
}

function formatTime(value) {
  if (!value) return "<span>—</span>";
  const [date, time] = String(value).split(/[T ]/);
  return `<time datetime="${escapeHtml(value)}">${escapeHtml(date)}<span>${escapeHtml((time || "").slice(0, 8))}</span></time>`;
}

function buildNoteArticle(item) {
  const row = document.createElement("article");
  row.dataset.symbol = item.symbol;
  const symbol = escapeHtml(item.symbol);
  const query = encodeURIComponent(item.symbol);
  const name = item.name && item.name !== item.symbol ? item.name : "名称待同步";
  row.innerHTML = `
    <header class="document-header">
      <div><p class="eyebrow">公司研究 / 我的备注</p><h1>${escapeHtml(name)}</h1><p class="document-code">${symbol} · 研究备注</p></div>
      <div class="document-actions">
        <button type="button" class="document-edit-btn secondary-btn">编辑备注</button>
        <button type="button" class="note-save-btn" data-symbol="${symbol}">已保存</button>
      </div>
    </header>
    <div class="document-body"><div class="list-note-editor"></div></div>
    <p class="marked-draft-state" aria-live="polite">${drafts.has(item.symbol) ? "有未保存修改，切换公司时会保留。" : ""}</p>
    <footer class="document-footer">
      <div><p>首次记录 ${formatTime(item.created_at)}</p><p>最近更新 ${formatTime(item.updated_at)}</p></div>
      <div class="document-links"><a href="/?symbol=${query}">查看估值 ↗</a><a href="/balance-sheet?symbol=${query}">资产负债表 ↗</a>
      <button type="button" class="note-delete-btn danger-btn" data-symbol="${symbol}">删除</button></div>
    </footer>`;
  const editButton = row.querySelector(".document-edit-btn");
  const editor = StockNotes.mount(row.querySelector(".list-note-editor"), {
    id: `reason-${item.symbol}`,
    label: `${item.symbol} 的备注`,
    value: drafts.get(item.symbol) ?? item.mark_reason,
    viewing: noteModes.get(item.symbol) ?? true,
    onModeChange: (viewing) => {
      noteModes.set(item.symbol, viewing);
      row.querySelector(".note-editor-toolbar").hidden = viewing;
      editButton.hidden = !viewing;
      updateBusyState();
    },
    onPreview: () => {
      const focusTarget = row.querySelector(".note-save-btn:not(:disabled):not([hidden])") || editButton;
      focusTarget.focus({ preventScroll: true });
    },
  });
  editButton.addEventListener("click", () => { editor.showEdit(); updateBusyState(); });
  return row;
}

function noteExcerpt(value) {
  return String(value || "").replace(/[#*`>_\[\]]/g, "").replace(/\s+/g, " ").trim().slice(0, 100);
}

function buildDirectoryItem(item) {
  const button = document.createElement("button");
  button.type = "button";
  button.className = "stock-directory-item";
  button.dataset.symbol = item.symbol;
  button.setAttribute("aria-current", String(item.symbol === selectedSymbol));
  button.setAttribute("aria-controls", "marked-detail");
  button.innerHTML = `<span class="directory-name">${escapeHtml(item.name === item.symbol ? "名称待同步" : item.name)}</span>
    <span class="directory-code">${escapeHtml(item.symbol)}</span><span class="directory-excerpt"></span><span class="directory-draft"></span>`;
  button.querySelector(".directory-excerpt").textContent = noteExcerpt(drafts.get(item.symbol) ?? item.mark_reason);
  button.querySelector(".directory-draft").textContent = drafts.has(item.symbol) ? "未保存修改" : "";
  return button;
}

function updateResultCount(visibleCount) {
  resultCountElement.textContent = hasLoaded
    ? `共 ${markedItems.length} 只 · 当前显示 ${visibleCount} 只${drafts.size ? ` · ${drafts.size} 只待保存` : ""}`
    : markedItems.length ? "当前仅显示已保存股票，请刷新加载完整清单。" : "正在加载关注清单…";
}

function renderNotebook() {
  const activeInput = document.activeElement;
  const selection = activeInput instanceof HTMLTextAreaElement && detailElement.contains(activeInput)
    ? { id: activeInput.id, start: activeInput.selectionStart, end: activeInput.selectionEnd } : null;
  const query = searchInput.value.trim().toLocaleLowerCase();
  const items = markedItems.filter((item) => [item.symbol, item.name, item.mark_reason, drafts.get(item.symbol)]
    .some((value) => String(value || "").toLocaleLowerCase().includes(query)));
  const sortField = { updated: "updated_at", created: "created_at", symbol: "symbol", name: "name" }[sortInput.value] || "updated_at";
  items.sort((a, b) => {
    const order = String(a[sortField] || "").localeCompare(String(b[sortField] || ""), "zh-CN");
    return (sortField.endsWith("_at") ? -order : order) || a.symbol.localeCompare(b.symbol);
  });
  const focusedStock = directoryList.contains(activeInput) ? activeInput.closest(".stock-directory-item")?.dataset.symbol : null;
  if (!items.some((item) => item.symbol === selectedSymbol)) selectedSymbol = items[0]?.symbol || null;
  const selectedItem = items.find((item) => item.symbol === selectedSymbol);
  directoryList.replaceChildren(...items.map(buildDirectoryItem));
  if (selectedItem) {
    detailElement.replaceChildren(buildNoteArticle(selectedItem));
    directorySummary.textContent = `切换公司 · ${selectedItem.name}`;
  } else {
    detailElement.innerHTML = `<div class="document-empty"><p class="eyebrow">研究手记</p><h2>${query ? "没有匹配的公司" : "写下你的第一份研究备注"}</h2><p>${query ? "换一个代码、名称或备注关键词再试试。" : "从右上方新增一家公司，记录判断、疑问与后续核对事项。"}</p></div>`;
    directorySummary.textContent = "选择一家公司";
  }
  markedCountElement.textContent = hasLoaded ? String(markedItems.length) : "—";
  updateResultCount(items.length);
  emptyStateElement.hidden = items.length > 0 || !hasLoaded;
  emptyStateElement.textContent = query ? "没有匹配的股票，试试其他名称、代码或备注。" : "还没有关注的股票。从上方添加第一只，记录你的投资想法。";
  updateBusyState();
  if (selection) {
    const input = document.getElementById(selection.id);
    if (input && !input.hidden) {
      input.focus({ preventScroll: true });
      input.setSelectionRange(selection.start, selection.end);
    } else {
      detailElement.querySelector(".document-edit-btn")?.focus({ preventScroll: true });
    }
  }
  if (focusedStock) directoryList.querySelector(`[data-symbol="${focusedStock}"]`)?.focus({ preventScroll: true });
}

async function requestMarkedStocks(options, symbol = "") {
  const response = await fetch(`/api/stocks/marked/${encodeURIComponent(symbol)}`, options);
  let payload;
  try {
    payload = await response.json();
  } catch {
    throw new Error(`服务响应异常（${response.status}），请稍后重试。`);
  }
  if (!response.ok) {
    if (response.status === 503) throw new Error("关注清单暂不可用，请检查数据库配置后重试。");
    throw new Error(typeof payload.detail === "string" ? payload.detail : `请求失败（${response.status}），请稍后重试。`);
  }
  return payload;
}

async function fetchMarkedStocks() {
  if (isFetching || isAdding || savingSymbols.size || deletingSymbols.size) return;
  isFetching = true;
  updateBusyState();
  try {
    const payload = await requestMarkedStocks();
    markedItems = payload.items || [];
    hasLoaded = true;
    markedItems.forEach((item) => {
      if (drafts.get(item.symbol) === item.mark_reason) drafts.delete(item.symbol);
    });
    renderNotebook();
  } catch (error) {
    setFeedback(error.message, "error");
    if (!hasLoaded && !markedItems.length) {
      resultCountElement.textContent = "清单加载失败";
      emptyStateElement.hidden = false;
      emptyStateElement.textContent = "暂时无法加载关注清单，请点击「刷新列表」重试。";
      detailElement.innerHTML = '<div class="document-empty"><h2>暂时无法打开备注</h2><p>请点击关注清单旁的“刷新”重试。</p></div>';
    }
  } finally {
    isFetching = false;
    updateBusyState();
  }
}

async function saveMarkedStock(symbol, markReason) {
  const payload = await requestMarkedStocks({
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ symbol, mark_reason: markReason }),
  });
  const index = markedItems.findIndex((item) => item.symbol === payload.symbol);
  if (index < 0) markedItems.push(payload);
  else markedItems[index] = payload;
  if (drafts.get(symbol) === markReason) drafts.delete(symbol);
  if (!drafts.has(symbol)) noteModes.set(symbol, true);
  if (!hasLoaded) {
    try {
      const list = await requestMarkedStocks();
      markedItems = list.items || [];
      hasLoaded = true;
    } catch {
      setFeedback(`${symbol} 已保存；完整清单暂未加载，请刷新列表重试。`, "info");
      renderNotebook();
      return payload;
    }
  }
  setFeedback(`${symbol} 的备注已保存。`, "success");
  renderNotebook();
  return payload;
}

markedSymbolInput.addEventListener("input", (event) => {
  event.target.value = sanitizeSymbolInput(event.target.value);
});

markedForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (isFetching || isAdding || savingSymbols.size || deletingSymbols.size) return;
  const symbol = sanitizeSymbolInput(markedSymbolInput.value);
  const markReason = markedReasonInput.value;
  const noteError = StockNotes.validate(markReason);
  if (symbol.length !== 6 || noteError) {
    setFeedback(symbol.length !== 6 ? "请输入 6 位股票代码。" : noteError, "error");
    if (noteError) newNoteEditor.showEdit();
    return;
  }
  if (drafts.has(symbol)) {
    setFeedback("该股票在列表中有未保存修改，请先保存列表中的修改。", "error");
    return;
  }
  isAdding = true;
  let didCloseDialog = false;
  savingSymbols.add(symbol);
  updateBusyState();
  try {
    await saveMarkedStock(symbol, markReason);
    if (markedSymbolInput.value === symbol && markedReasonInput.value === markReason) {
      markedForm.reset();
      newNoteEditor.setValue("", false);
      addDialog.close();
      didCloseDialog = true;
    }
    selectedSymbol = symbol;
    renderNotebook();
  } catch (error) {
    setFeedback(error.message, "error");
  } finally {
    isAdding = false;
    savingSymbols.delete(symbol);
    updateBusyState();
    if (didCloseDialog) detailElement.querySelector(".document-edit-btn")?.focus({ preventScroll: true });
  }
});

detailElement.addEventListener("input", (event) => {
  const input = event.target;
  if (!(input instanceof HTMLTextAreaElement)) return;
  const row = input.closest("article[data-symbol]");
  const symbol = row.dataset.symbol;
  const item = markedItems.find((candidate) => candidate.symbol === symbol);
  if (input.value === item.mark_reason && !savingSymbols.has(symbol)) drafts.delete(symbol);
  else drafts.set(symbol, input.value);
  row.querySelector(".marked-draft-state").textContent = drafts.has(symbol) ? "有未保存修改" : "";
  const directoryItem = directoryList.querySelector(`[data-symbol="${symbol}"]`);
  if (directoryItem) {
    directoryItem.querySelector(".directory-draft").textContent = drafts.has(symbol) ? "未保存修改" : "";
    directoryItem.querySelector(".directory-excerpt").textContent = noteExcerpt(input.value);
  }
  updateResultCount(directoryList.children.length);
  updateBusyState();
});

detailElement.addEventListener("click", async (event) => {
  const deleteButton = event.target.closest(".note-delete-btn");
  if (deleteButton && !deleteButton.disabled) {
    const symbol = deleteButton.dataset.symbol;
    const draftWarning = drafts.has(symbol) ? "该股票未保存的修改也会丢弃。" : "";
    if (!window.confirm(`删除 ${symbol} 的备注并移出标记列表？${draftWarning}行情和财报数据会保留。`)) return;
    deletingSymbols.add(symbol);
    updateBusyState();
    try {
      await requestMarkedStocks({ method: "DELETE" }, symbol);
      markedItems = markedItems.filter((item) => item.symbol !== symbol);
      drafts.delete(symbol);
      noteModes.delete(symbol);
      renderNotebook();
      setFeedback(`${symbol} 的备注已删除，已移出标记列表。`, "success");
    } catch (error) {
      setFeedback(error.message, "error");
    } finally {
      deletingSymbols.delete(symbol);
      updateBusyState();
    }
    return;
  }
  const button = event.target.closest(".note-save-btn");
  if (!button || button.disabled) return;
  const symbol = button.dataset.symbol;
  const markReason = drafts.get(symbol) || "";
  const noteError = StockNotes.validate(markReason);
  if (noteError) {
    setFeedback(`${symbol}：${noteError}`, "error");
    return;
  }
  savingSymbols.add(symbol);
  updateBusyState();
  try {
    await saveMarkedStock(symbol, markReason);
  } catch (error) {
    setFeedback(error.message, "error");
  } finally {
    savingSymbols.delete(symbol);
    updateBusyState();
  }
});

markedRefreshButton.addEventListener("click", () => {
  setFeedback("");
  fetchMarkedStocks();
});
newNoteButton.addEventListener("click", () => {
  dialogFeedback.textContent = "";
  addDialog.showModal();
  markedSymbolInput.focus();
});
closeDialogButton.addEventListener("click", () => addDialog.close());
addDialog.addEventListener("cancel", (event) => { if (isAdding) event.preventDefault(); });
directoryList.addEventListener("click", (event) => {
  const button = event.target.closest(".stock-directory-item");
  if (!button) return;
  selectedSymbol = button.dataset.symbol;
  renderNotebook();
  if (window.matchMedia("(max-width: 760px)").matches) {
    directory.open = false;
    directorySummary.focus({ preventScroll: true });
  }
});
const mobileDirectory = window.matchMedia("(max-width: 760px)");
directory.open = !mobileDirectory.matches;
mobileDirectory.addEventListener("change", () => { directory.open = !mobileDirectory.matches; });
searchInput.addEventListener("input", renderNotebook);
sortInput.addEventListener("change", renderNotebook);
window.addEventListener("beforeunload", (event) => {
  if (!drafts.size && !markedSymbolInput.value && !markedReasonInput.value) return;
  event.preventDefault();
  event.returnValue = "";
});

fetchMarkedStocks();
