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
  emptyMarkReason: "\u8bf7\u8f93\u5165\u6807\u8bb0\u539f\u56e0",
  markSuccess: "\u6807\u8bb0\u6210\u529f",
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
const markReasonInput = document.getElementById("stock-mark-reason");
const markFeedbackElement = document.getElementById("mark-feedback");

let currentValuationPayload = null;
let currentPerformancePayload = null;
let currentNamePayload = null;

const chartMap = {
  pe: echarts.init(document.getElementById("pe-chart")),
  pb: echarts.init(document.getElementById("pb-chart")),
  ps: echarts.init(document.getElementById("ps-chart")),
  marketPerformance: echarts.init(document.getElementById("market-performance-chart")),
};

const metricMeta = {
  pe: { label: "PE(TTM)", color: "#34d399" },
  pb: { label: uiText.pb, color: "#60a5fa" },
  ps: { label: uiText.ps, color: "#f59e0b" },
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
  return `${Number(value).toFixed(2)} ${uiText.yi}`;
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

  chart.setOption({
    backgroundColor: "transparent",
    title: {
      text: `${stockLabel} ${meta.label} ${uiText.latestRangeTrend}`,
      left: 12,
      top: 12,
      textStyle: {
        color: "#f8fafc",
        fontSize: 18,
        fontWeight: 600,
      },
    },
    tooltip: {
      trigger: "axis",
    },
    legend: {
      top: 10,
      right: 12,
      textStyle: {
        color: "#dbe4ff",
      },
    },
    grid: {
      left: 56,
      right: 32,
      top: 72,
      bottom: 48,
    },
    xAxis: {
      type: "category",
      data: dates,
      boundaryGap: false,
      axisLabel: {
        color: "#9fb0d6",
      },
      axisLine: {
        lineStyle: {
          color: "rgba(159, 176, 214, 0.35)",
        },
      },
    },
    yAxis: {
      type: "value",
      name: meta.label,
      axisLabel: {
        color: "#9fb0d6",
      },
      splitLine: {
        lineStyle: {
          color: "rgba(159, 176, 214, 0.12)",
        },
      },
    },
    series: [
      {
        name: meta.label,
        type: "line",
        smooth: true,
        showSymbol: false,
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
          color: "#e879f9",
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
          color: "#f97316",
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
          color: "#22c55e",
        },
      },
    ],
  });

  const summaryText = `${uiText.latest} ${meta.label}: ${formatValue(metricPayload.latest.value)} | ${uiText.mean}: ${formatValue(mean)} | ${uiText.std}: ${formatValue(std)}`;
  if (metricKey === "pe") peSummaryElement.textContent = summaryText;
  if (metricKey === "pb") pbSummaryElement.textContent = summaryText;
  if (metricKey === "ps") psSummaryElement.textContent = summaryText;
}

function renderMarketPerformanceChart(payload, stockLabel, latestAvailableDate) {
  const chart = chartMap.marketPerformance;
  const dates = payload.series.map((item) => item.date);
  const marketCap = payload.series.map((item) => item.market_cap);
  const revenue = payload.series.map((item) => ({
    value: item.revenue,
    auditOpinion: item.audit_opinion || null,
    itemStyle: {
      color: item.highlight_revenue ? "rgba(239, 68, 68, 0.82)" : "rgba(52, 211, 153, 0.72)",
      borderRadius: [6, 6, 0, 0],
    },
  }));
  const netProfit = payload.series.map((item) => item.net_profit);

  chart.setOption({
    backgroundColor: "transparent",
    title: {
      text: `${stockLabel} ${payload.label}`,
      left: 12,
      top: 12,
      textStyle: {
        color: "#f8fafc",
        fontSize: 18,
        fontWeight: 600,
      },
    },
    tooltip: {
      trigger: "axis",
      axisPointer: {
        type: "shadow",
      },
      formatter(params) {
        const lines = [params[0]?.axisValue || ""];
        params.forEach((item) => {
          const rawValue = typeof item.data === "object" && item.data !== null ? item.data.value : item.data;
          lines.push(`${item.marker}${item.seriesName}: ${formatYiValue(Number(rawValue || 0))}`);
          if (item.seriesName === uiText.revenue && item.data?.auditOpinion) {
            lines.push(`\u5ba1\u8ba1\u610f\u89c1: ${item.data.auditOpinion}`);
          }
        });
        return lines.join("<br/>");
      },
    },
    legend: {
      top: 10,
      right: 12,
      textStyle: {
        color: "#dbe4ff",
      },
    },
    grid: {
      left: 56,
      right: 56,
      top: 72,
      bottom: 48,
    },
    xAxis: {
      type: "category",
      data: dates,
      axisLabel: {
        color: "#9fb0d6",
      },
      axisLine: {
        lineStyle: {
          color: "rgba(159, 176, 214, 0.35)",
        },
      },
    },
    yAxis: [
      {
        type: "value",
        name: `${uiText.marketCap}(${uiText.yi})`,
        axisLabel: {
          color: "#9fb0d6",
        },
        splitLine: {
          lineStyle: {
            color: "rgba(159, 176, 214, 0.12)",
          },
        },
      },
      {
        type: "value",
        name: `${uiText.revenue}/${uiText.netProfit}(${uiText.yi})`,
        axisLabel: {
          color: "#9fb0d6",
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
        smooth: true,
        showSymbol: false,
        data: marketCap,
        yAxisIndex: 0,
        lineStyle: {
          width: 2,
          color: "#7dd3fc",
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
          color: "rgba(249, 115, 22, 0.72)",
          borderRadius: [6, 6, 0, 0],
        },
      },
    ],
  });

  marketPerformanceSummaryElement.textContent = `${uiText.latest} ${uiText.marketCap}: ${formatYiValue(payload.latest.market_cap)} | ${uiText.latest} ${uiText.revenue}: ${formatYiValue(payload.latest.revenue)} | ${uiText.latest} ${uiText.netProfit}: ${formatYiValue(payload.latest.net_profit)}`;
  marketPerformanceLatestDateElement.textContent = latestAvailableDate || "-";
}

function renderCharts(valuationPayload, performancePayload, namePayload) {
  const stockLabel = buildStockLabel(valuationPayload.symbol, namePayload);
  stockNameElement.textContent = stockLabel;
  latestPeElement.textContent = formatValue(valuationPayload.metrics.pe.latest.value);
  latestPbElement.textContent = formatValue(valuationPayload.metrics.pb.latest.value);
  latestPsElement.textContent = formatValue(valuationPayload.metrics.ps.latest.value);

  const valuationDates = Object.values(valuationPayload.metrics)
    .flatMap((metric) => metric.series)
    .map((item) => item.date);
  const performanceDates = performancePayload.market_performance.series.map((item) => item.date);
  const ranges = [...valuationDates, ...performanceDates].sort();
  dateRangeElement.textContent = `${ranges[0]} ~ ${ranges[ranges.length - 1]}`;

  renderMetricChart("pe", valuationPayload.metrics.pe, stockLabel);
  renderMetricChart("pb", valuationPayload.metrics.pb, stockLabel);
  renderMetricChart("ps", valuationPayload.metrics.ps, stockLabel);
  renderMarketPerformanceChart(
    performancePayload.market_performance,
    stockLabel,
    performancePayload.provider_summary?.latest_available_date,
  );
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
    chart.clear();
    chart.setOption({
      title: {
        text: message,
        left: "center",
        top: "middle",
        textStyle: {
          color: "#fca5a5",
          fontSize: 16,
        },
      },
    });
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
  markButton.textContent = loading ? uiText.loading : "\u6807\u8bb0\u80a1\u7968";
}

async function markCurrentStock() {
  const normalizedSymbol = sanitizeSymbolInput(symbolInput.value);
  const markReason = String(markReasonInput.value || "").trim();

  if (normalizedSymbol.length !== 6) {
    setFeedback(uiText.invalidSymbol, "error");
    return;
  }
  if (!markReason) {
    setFeedback(uiText.emptyMarkReason, "error");
    return;
  }

  setMarkingState(true);
  try {
    const payload = {
      symbol: normalizedSymbol,
      mark_reason: markReason,
      name: currentNamePayload?.name || undefined,
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
    if (!currentNamePayload && result.name) {
      currentNamePayload = { symbol: result.symbol, name: result.name };
      renderCurrentDashboard();
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
  if (currentValuationPayload && currentPerformancePayload) {
    renderCharts(currentValuationPayload, currentPerformancePayload, currentNamePayload);
  }
}

async function loadData(symbol) {
  const normalizedSymbol = sanitizeSymbolInput(symbol);
  if (normalizedSymbol.length !== 6) {
    clearCharts(uiText.invalidSymbol);
    return;
  }

  symbolInput.value = normalizedSymbol;
  setFeedback("", "info");
  setLoadingState(true);

  try {
    currentValuationPayload = null;
    currentPerformancePayload = null;
    currentNamePayload = null;

    const namePromise = fetchStockName(normalizedSymbol);
    const [valuationResponse, performanceResponse] = await Promise.all([
      fetch(`/api/stocks/valuation-metrics?symbol=${normalizedSymbol}&years=10`),
      fetch(`/api/stocks/market-performance?symbol=${normalizedSymbol}&years=10`),
    ]);

    const valuationPayload = await valuationResponse.json();
    const performancePayload = await performanceResponse.json();

    if (!valuationResponse.ok) {
      throw new Error(valuationPayload.detail || `${uiText.requestFailed}: ${valuationResponse.status}`);
    }
    if (!performanceResponse.ok) {
      throw new Error(performancePayload.detail || `${uiText.requestFailed}: ${performanceResponse.status}`);
    }

    currentValuationPayload = valuationPayload;
    currentPerformancePayload = performancePayload;
    persistLastSymbol(normalizedSymbol);
    renderCurrentDashboard();
    setLoadingState(false);

    namePromise.then((namePayload) => {
      currentNamePayload = namePayload;
      renderCurrentDashboard();
    });
  } catch (error) {
    clearCharts(error.message);
    setLoadingState(false);
  }
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
  Object.values(chartMap).forEach((chart) => chart.resize());
});

loadData(readLastSymbol() || symbolInput.value);
