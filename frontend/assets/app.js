const uiText = {
  latestRangeTrend: "\u8fd1 10 \u5e74\u8d70\u52bf",
  pb: "\u5e02\u51c0\u7387",
  ps: "\u5e02\u9500\u7387",
  marketCap: "\u603b\u5e02\u503c",
  revenue: "\u8425\u6536(TTM)",
  netProfit: "\u51c0\u5229\u6da6(TTM)",
  mean: "\u5747\u503c",
  plusStd: "+1\u03c3",
  minusStd: "-1\u03c3",
  latest: "\u6700\u65b0",
  std: "\u6807\u51c6\u5dee",
  yi: "\u4ebf",
  loading: "\u52a0\u8f7d\u4e2d...",
  query: "\u67e5\u8be2\u6570\u636e",
  invalidSymbol: "\u8bf7\u8f93\u5165 6 \u4f4d\u80a1\u7968\u4ee3\u7801",
  requestFailed: "\u63a5\u53e3\u8bf7\u6c42\u5931\u8d25",
  markSuccess: "备注已保存",
};

const stockNameElement = document.getElementById("stock-name");
const latestPeElement = document.getElementById("latest-pe");
const latestPbElement = document.getElementById("latest-pb");
const latestPsElement = document.getElementById("latest-ps");
const dateRangeElement = document.getElementById("date-range");
const peSummaryElement = document.getElementById("pe-summary");
const pbSummaryElement = document.getElementById("pb-summary");
const psSummaryElement = document.getElementById("ps-summary");
const marketPerformanceSummaryElement = document.getElementById("market-performance-summary");
const marketPerformanceLatestDateElement = document.getElementById("market-performance-latest-date");
const stockForm = document.getElementById("stock-form");
const symbolInput = document.getElementById("stock-symbol");
const reloadButton = document.getElementById("reload-btn");
const markButton = document.getElementById("mark-btn");
const stockNoteEditor = StockNotes.mount(document.getElementById("stock-note-editor"), { id: "stock-mark-reason" });
const markReasonInput = stockNoteEditor.input;
const markFeedbackElement = document.getElementById("mark-feedback");

let currentValuationPayload = null;
let currentPerformancePayload = null;
let currentNamePayload = null;
let activeRequestId = 0;

const chartMap = {
  pe: echarts.init(document.getElementById("pe-chart"), "paper-notebook"),
  pb: echarts.init(document.getElementById("pb-chart"), "paper-notebook"),
  ps: echarts.init(document.getElementById("ps-chart"), "paper-notebook"),
  marketPerformance: echarts.init(document.getElementById("market-performance-chart"), "paper-notebook"),
};

const metricMeta = {
  pe: { label: "PE(TTM)", color: "#4e6750" },
  pb: { label: uiText.pb, color: "#677c91" },
  ps: { label: uiText.ps, color: "#af8b4e" },
};

const LAST_SYMBOL_STORAGE_KEY = "invest:last-symbol";

function sanitizeSymbolInput(value) {
  return value.replace(/\D/g, "").slice(0, 6);
}

function persistLastSymbol(symbol) {
  const normalizedSymbol = sanitizeSymbolInput(symbol);
  if (normalizedSymbol.length === 6) {
    window.localStorage.setItem(LAST_SYMBOL_STORAGE_KEY, normalizedSymbol);
  }
}

function readLastSymbol() {
  return sanitizeSymbolInput(window.localStorage.getItem(LAST_SYMBOL_STORAGE_KEY) || "");
}

function formatValue(value) {
  return Number(value).toFixed(2);
}

function formatYiValue(value) {
  return value == null || !Number.isFinite(Number(value)) ? "暂无数据" : `${Number(value).toFixed(2)} ${uiText.yi}`;
}

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, (char) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[char]);
}

function buildStockLabel(symbol, namePayload) {
  if (namePayload && namePayload.name) {
    return `${namePayload.name} (${symbol})`;
  }
  return symbol;
}

function renderMetricChart(metricKey, metricPayload, stockLabel) {
  const chart = chartMap[metricKey];
  const meta = metricMeta[metricKey];
  const dates = metricPayload.series.map((item) => item.date);
  const values = metricPayload.series.map((item) => item.value);
  const { mean, std, upper, lower } = metricPayload.stats;
  const compact = chart.getWidth() < 720;

  chart.setOption({
    backgroundColor: "transparent",
    title: {
      text: compact ? `${stockLabel}\n${meta.label} · ${uiText.latestRangeTrend}` : `${stockLabel} ${meta.label} ${uiText.latestRangeTrend}`,
      left: 12,
      top: 12,
      textStyle: {
        color: "#343b35",
        fontSize: compact ? 13 : 16,
        lineHeight: 20,
        fontWeight: 500,
      },
    },
    tooltip: {
      trigger: "axis",
    },
    legend: {
      selected: chart.getOption()?.legend?.[0]?.selected || {},
      top: compact ? 58 : 10,
      left: compact ? 12 : undefined,
      right: 12,
      textStyle: {
        color: "#4d574b",
      },
    },
    grid: {
      left: 56,
      right: 32,
      top: compact ? 110 : 72,
      bottom: 48,
    },
    xAxis: {
      type: "category",
      data: dates,
      boundaryGap: false,
      axisLabel: {
        color: "#6f7769",
      },
      axisLine: {
        lineStyle: {
          color: "rgba(111, 119, 105, 0.35)",
        },
      },
    },
    yAxis: {
      type: "value",
      name: meta.label,
      axisLabel: {
        color: "#6f7769",
      },
      splitLine: {
        lineStyle: {
          color: "rgba(111, 119, 105, 0.15)",
        },
      },
    },
    series: [
      {
        name: meta.label,
        type: "line",
        smooth: true,
        showSymbol: values.length === 1,
        data: values,
        lineStyle: {
          width: 2,
          color: meta.color,
        },
        areaStyle: {
          color: `${meta.color}22`,
        },
      },
      {
        name: uiText.mean,
        type: "line",
        symbol: "none",
        data: new Array(values.length).fill(mean),
        lineStyle: {
          width: 2,
          type: "dashed",
          color: "#8b897f",
        },
      },
      {
        name: uiText.plusStd,
        type: "line",
        symbol: "none",
        data: new Array(values.length).fill(upper),
        lineStyle: {
          width: 2,
          type: "dashed",
          color: "#b57a5d",
        },
      },
      {
        name: uiText.minusStd,
        type: "line",
        symbol: "none",
        data: new Array(values.length).fill(lower),
        lineStyle: {
          width: 2,
          type: "dashed",
          color: "#79916c",
        },
      },
    ],
  }, true);

  const summaryText = `${uiText.latest} ${meta.label}: ${formatValue(metricPayload.latest.value)} | ${uiText.mean}: ${formatValue(mean)} | ${uiText.std}: ${formatValue(std)}`;
  if (metricKey === "pe") peSummaryElement.textContent = summaryText;
  if (metricKey === "pb") pbSummaryElement.textContent = summaryText;
  if (metricKey === "ps") psSummaryElement.textContent = summaryText;
}

function renderMarketPerformanceChart(payload, stockLabel, latestAvailableDate) {
  const chart = chartMap.marketPerformance;
  const compact = chart.getWidth() < 720;
  const dates = payload.series.map((item) => item.date);
  const marketCap = payload.series.map((item) => item.market_cap);
  const revenue = payload.series.map((item) => ({
    value: item.revenue,
    auditOpinion: item.audit_opinion || null,
    itemStyle: {
      color: item.highlight_revenue ? "rgba(164, 87, 77, 0.85)" : "rgba(78, 103, 80, 0.72)",
      borderRadius: [6, 6, 0, 0],
    },
  }));
  const netProfit = payload.series.map((item) => item.net_profit);

  chart.setOption({
    backgroundColor: "transparent",
    title: {
      text: compact ? `${stockLabel}\n${payload.label}` : `${stockLabel} ${payload.label}`,
      left: 12,
      top: 12,
      textStyle: {
        color: "#343b35",
        fontSize: compact ? 13 : 16,
        lineHeight: 20,
        fontWeight: 500,
      },
    },
    tooltip: {
      trigger: "axis",
      // Follow the cursor directly: ECharts' default glide and band slide trail it and read as dropped frames.
      transitionDuration: 0,
      axisPointer: {
        type: "shadow",
        animation: false,
      },
      formatter(params) {
        const point = payload.series[params[0]?.dataIndex];
        const lines = [escapeHtml(point?.report_date || params[0]?.axisValue || "")];
        if (point?.market_cap_date) lines.push(`市值交易日: ${escapeHtml(point.market_cap_date)}`);
        params.forEach((item) => {
          const rawValue = typeof item.data === "object" && item.data !== null ? item.data.value : item.data;
          lines.push(`${item.marker}${escapeHtml(item.seriesName)}: ${formatYiValue(rawValue)}`);
          if (item.seriesName === uiText.revenue && item.data?.auditOpinion) {
            lines.push(`\u5ba1\u8ba1\u610f\u89c1: ${escapeHtml(item.data.auditOpinion)}`);
          }
        });
        return lines.join("<br/>");
      },
    },
    legend: {
      selected: chart.getOption()?.legend?.[0]?.selected || {},
      top: compact ? 58 : 10,
      left: compact ? 12 : undefined,
      right: 12,
      textStyle: {
        color: "#4d574b",
      },
    },
    grid: {
      left: 56,
      right: 56,
      top: compact ? 125 : 72,
      bottom: 48,
    },
    xAxis: {
      type: "category",
      data: dates,
      axisLabel: {
        color: "#6f7769",
      },
      axisLine: {
        lineStyle: {
          color: "rgba(111, 119, 105, 0.35)",
        },
      },
    },
    yAxis: [
      {
        type: "value",
        name: `${uiText.marketCap}(${uiText.yi})`,
        axisLabel: {
          color: "#6f7769",
        },
        splitLine: {
          lineStyle: {
            color: "rgba(111, 119, 105, 0.15)",
          },
        },
      },
      {
        type: "value",
        name: `${uiText.revenue}/${uiText.netProfit}(${uiText.yi})`,
        axisLabel: {
          color: "#6f7769",
        },
        splitLine: {
          show: false,
        },
      },
    ],
    series: [
      {
        name: uiText.marketCap,
        type: "line",
        smooth: false,
        showSymbol: marketCap.length === 1,
        data: marketCap,
        yAxisIndex: 0,
        lineStyle: {
          width: 2,
          color: "#677c91",
        },
      },
      {
        name: uiText.revenue,
        type: "bar",
        data: revenue,
        yAxisIndex: 1,
      },
      {
        name: uiText.netProfit,
        type: "bar",
        data: netProfit,
        yAxisIndex: 1,
        itemStyle: {
          color: "rgba(181, 122, 93, 0.8)",
          borderRadius: [6, 6, 0, 0],
        },
      },
    ],
  }, true);

  const latestMarket = payload.latest_market_cap;
  marketPerformanceSummaryElement.textContent = `最新市值: ${formatYiValue(latestMarket?.value)}（${latestMarket?.date || "-"}） | 财报 ${payload.latest.report_date || payload.latest.date}：营收(TTM) ${formatYiValue(payload.latest.revenue)} / 净利润(TTM) ${formatYiValue(payload.latest.net_profit)}`;
  marketPerformanceLatestDateElement.textContent = latestMarket?.date || latestAvailableDate || "-";
}

function renderCharts(valuationPayload, performancePayload, namePayload) {
  const stockLabel = buildStockLabel((valuationPayload || performancePayload).symbol, namePayload);
  stockNameElement.textContent = stockLabel;
  if (valuationPayload) {
    latestPeElement.textContent = formatValue(valuationPayload.metrics.pe.latest.value);
    latestPbElement.textContent = formatValue(valuationPayload.metrics.pb.latest.value);
    latestPsElement.textContent = formatValue(valuationPayload.metrics.ps.latest.value);

    renderMetricChart("pe", valuationPayload.metrics.pe, stockLabel);
    renderMetricChart("pb", valuationPayload.metrics.pb, stockLabel);
    renderMetricChart("ps", valuationPayload.metrics.ps, stockLabel);
  }
  const valuationDates = Object.values(valuationPayload?.metrics || {})
    .flatMap((metric) => metric.series)
    .map((item) => item.date);
  const performanceDates = (performancePayload?.market_performance.series || []).map((item) => item.report_date).filter(Boolean);
  const ranges = [...valuationDates, ...performanceDates].sort();
  dateRangeElement.textContent = ranges.length ? `${ranges[0]} ~ ${ranges[ranges.length - 1]}` : "-";

  if (performancePayload) {
    renderMarketPerformanceChart(
      performancePayload.market_performance,
      stockLabel,
      performancePayload.provider_summary?.latest_available_date,
    );
  }
}

function renderChartError(chart, message) {
  chart.clear();
  chart.setOption({ title: { text: message, left: "center", top: "middle", textStyle: { color: "#a4574d", fontSize: 16 } } }, true);
}

function clearCharts(message) {
  stockNameElement.textContent = "-";
  latestPeElement.textContent = "-";
  latestPbElement.textContent = "-";
  latestPsElement.textContent = "-";
  dateRangeElement.textContent = "-";
  peSummaryElement.textContent = "-";
  pbSummaryElement.textContent = "-";
  psSummaryElement.textContent = "-";
  marketPerformanceSummaryElement.textContent = "-";
  marketPerformanceLatestDateElement.textContent = "-";

  Object.values(chartMap).forEach((chart) => {
    renderChartError(chart, message);
  });
}

function setFeedback(message, type = "info") {
  markFeedbackElement.textContent = message || "";
  markFeedbackElement.dataset.state = type;
}

function setLoadingState(loading) {
  reloadButton.disabled = loading;
  reloadButton.textContent = loading ? uiText.loading : uiText.query;
}

function setMarkingState(loading) {
  markButton.disabled = loading;
  markButton.textContent = loading ? uiText.loading : "保存备注";
}

async function markCurrentStock() {
  const normalizedSymbol = sanitizeSymbolInput(symbolInput.value);
  const markReason = markReasonInput.value;

  if (normalizedSymbol.length !== 6) {
    setFeedback(uiText.invalidSymbol, "error");
    return;
  }
  const noteError = StockNotes.validate(markReason);
  if (noteError) {
    setFeedback(noteError, "error");
    document.querySelector(".home-note-disclosure").open = true;
    stockNoteEditor.showEdit();
    return;
  }

  setMarkingState(true);
  try {
    const payload = {
      symbol: normalizedSymbol,
      mark_reason: markReason,
    };
    const response = await fetch("/api/stocks/marked/", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    });
    const result = await response.json();
    if (!response.ok) {
      throw new Error(result.detail || `${uiText.requestFailed}: ${response.status}`);
    }
    if (!currentNamePayload && result.name && (currentValuationPayload || currentPerformancePayload)?.symbol === result.symbol) {
      currentNamePayload = { symbol: result.symbol, name: result.name };
      renderCurrentDashboard();
    }
    if (sanitizeSymbolInput(symbolInput.value) === normalizedSymbol && markReasonInput.value === markReason) {
      stockNoteEditor.showPreview();
    }
    setFeedback(`${result.symbol} ${uiText.markSuccess}`, "success");
  } catch (error) {
    setFeedback(error.message, "error");
  } finally {
    setMarkingState(false);
  }
}

function fetchStockName(symbol, timeoutMs = 2500) {
  const controller = new AbortController();
  const timeoutId = window.setTimeout(() => controller.abort(), timeoutMs);

  return fetch(`/api/stocks/name?symbol=${symbol}`, { signal: controller.signal })
    .then(async (response) => {
      if (!response.ok) {
        return null;
      }
      return response.json();
    })
    .catch(() => null)
    .finally(() => window.clearTimeout(timeoutId));
}

function renderCurrentDashboard() {
  if (currentValuationPayload || currentPerformancePayload) {
    renderCharts(currentValuationPayload, currentPerformancePayload, currentNamePayload);
  }
}

async function loadData(symbol) {
  const requestId = ++activeRequestId;
  const normalizedSymbol = sanitizeSymbolInput(symbol);
  if (normalizedSymbol.length !== 6) {
    currentValuationPayload = null;
    currentPerformancePayload = null;
    currentNamePayload = null;
    setLoadingState(false);
    clearCharts(uiText.invalidSymbol);
    return;
  }

  symbolInput.value = normalizedSymbol;
  setFeedback("", "info");
  setLoadingState(true);
  clearCharts(uiText.loading);

  try {
    currentValuationPayload = null;
    currentPerformancePayload = null;
    currentNamePayload = null;

    const namePromise = fetchStockName(normalizedSymbol);
    const [valuationResult, performanceResult] = await Promise.allSettled([
      fetchStockData(`/api/stocks/valuation-metrics?symbol=${normalizedSymbol}&years=10`),
      fetchStockData(`/api/stocks/market-performance?symbol=${normalizedSymbol}&years=10`),
    ]);
    if (requestId !== activeRequestId) return;
    currentValuationPayload = valuationResult.status === "fulfilled" ? valuationResult.value : null;
    currentPerformancePayload = performanceResult.status === "fulfilled" ? performanceResult.value : null;
    if (!currentValuationPayload) {
      ["pe", "pb", "ps"].forEach((key) => renderChartError(chartMap[key], valuationResult.reason.message));
    }
    if (!currentPerformancePayload) renderChartError(chartMap.marketPerformance, performanceResult.reason.message);
    if (!currentValuationPayload && !currentPerformancePayload) stockNameElement.textContent = normalizedSymbol;
    persistLastSymbol(normalizedSymbol);
    renderCurrentDashboard();
    setLoadingState(false);

    namePromise.then((namePayload) => {
      if (requestId !== activeRequestId) return;
      currentNamePayload = namePayload;
      renderCurrentDashboard();
    });
  } catch (error) {
    if (requestId !== activeRequestId) return;
    clearCharts(error.message);
    setLoadingState(false);
  }
}

async function fetchStockData(url) {
  const response = await fetch(url);
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.detail || `${uiText.requestFailed}: ${response.status}`);
  return payload;
}

symbolInput.addEventListener("input", (event) => {
  event.target.value = sanitizeSymbolInput(event.target.value);
  persistLastSymbol(event.target.value);
});

stockForm.addEventListener("submit", (event) => {
  event.preventDefault();
  persistLastSymbol(symbolInput.value);
  loadData(symbolInput.value);
});

markButton.addEventListener("click", () => {
  markCurrentStock();
});

window.addEventListener("resize", () => {
  const wasCompact = chartMap.pe.getWidth() < 720;
  Object.values(chartMap).forEach((chart) => chart.resize());
  // Only rebuild layout at a breakpoint, not on small viewport-size changes.
  if (wasCompact !== (chartMap.pe.getWidth() < 720)) renderCurrentDashboard();
});

loadData(new URLSearchParams(window.location.search).get("symbol") || readLastSymbol() || symbolInput.value);
