const UI_TEXT = {
  yi: "\u4ebf",
  noDetails: "\u65e0\u660e\u7ec6",
  displayOnly: "\u4ec5\u5c55\u793a",
  loading: "\u52a0\u8f7d\u4e2d...",
  query: "\u67e5\u8be2\u6570\u636e",
  balanceSheet: "\u8d44\u4ea7\u8d1f\u503a\u8868",
  detail: "\u660e\u7ec6",
  assets: "\u8d44\u4ea7",
  liabilities: "\u8d1f\u503a",
  yiYuan: "\u4ebf\u5143",
  noPeriodData: "\u6ca1\u6709\u53ef\u5c55\u793a\u7684\u5b63\u5ea6\u8d44\u4ea7\u8d1f\u503a\u8868\u6570\u636e",
  invalidSymbol: "\u8bf7\u8f93\u5165 6 \u4f4d\u80a1\u7968\u4ee3\u7801",
  invalidYears: "\u8bf7\u8f93\u5165\u6b63\u6574\u6570\u5e74\u9650",
  requestFailed: "\u63a5\u53e3\u8bf7\u6c42\u5931\u8d25",
};

const stockNameElement = document.getElementById("balance-stock-name");
const reportDateElement = document.getElementById("balance-report-date");
const assetsTotalElement = document.getElementById("balance-assets-total");
const liabilitiesTotalElement = document.getElementById("balance-liabilities-total");
const chartTitleElement = document.getElementById("balance-chart-title");
const balanceForm = document.getElementById("balance-form");
const symbolInput = document.getElementById("balance-symbol");
const yearsInput = document.getElementById("balance-years");
const submitButton = document.getElementById("balance-submit-btn");
const prevButton = document.getElementById("prev-period-btn");
const currentButton = document.getElementById("current-period-btn");
const nextButton = document.getElementById("next-period-btn");
const chart = echarts.init(document.getElementById("balance-chart"));

let currentPayload = null;
let currentNamePayload = null;
let currentIndex = 0;

function sanitizeSymbolInput(value) {
  return value.replace(/\D/g, "").slice(0, 6);
}

function sanitizeYearsInput(value) {
  return value.replace(/\D/g, "").slice(0, 2);
}

function parseYearsInput(value) {
  const parsed = Number.parseInt(value, 10);
  if (Number.isNaN(parsed) || parsed < 1) {
    throw new Error(UI_TEXT.invalidYears);
  }
  return Math.min(parsed, 20);
}

function buildStockLabel(symbol, namePayload) {
  if (namePayload && namePayload.name) {
    return `${namePayload.name} (${symbol})`;
  }
  return symbol;
}

function formatYi(value) {
  return `${Number(value || 0).toFixed(2)} ${UI_TEXT.yi}`;
}

function formatDetails(details = []) {
  if (!details.length) {
    return `<span style="color:#9fb0d6">${UI_TEXT.noDetails}</span>`;
  }

  return details
    .map((detail) => {
      const suffix = detail.included_in_total ? "" : ` <span style="color:#fbbf24">(${UI_TEXT.displayOnly})</span>`;
      return `${detail.label}: ${formatYi(detail.value)}${suffix}`;
    })
    .join("<br/>");
}

function setLoadingState(loading) {
  submitButton.disabled = loading;
  submitButton.textContent = loading ? UI_TEXT.loading : UI_TEXT.query;
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

function updateNavigationState() {
  const hasPeriods = currentPayload && currentPayload.periods.length > 0;
  prevButton.disabled = !hasPeriods || currentIndex >= currentPayload.periods.length - 1;
  nextButton.disabled = !hasPeriods || currentIndex <= 0;
  currentButton.disabled = !hasPeriods || currentIndex === 0;
}

function renderError(message) {
  stockNameElement.textContent = "-";
  reportDateElement.textContent = "-";
  assetsTotalElement.textContent = "-";
  liabilitiesTotalElement.textContent = "-";
  chartTitleElement.textContent = UI_TEXT.balanceSheet;
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
  updateNavigationState();
}

function buildSeriesData(primaryItems, secondaryCount) {
  return [
    ...primaryItems.map((item) => ({
      value: item.value,
      itemLabel: item.label,
      details: item.details || [],
    })),
    ...new Array(secondaryCount).fill(0),
  ];
}

function renderChart(period) {
  const assetLabels = period.assets.map((item) => item.label);
  const liabilityLabels = period.liabilities.map((item) => item.label);
  const labels = [...assetLabels, ...liabilityLabels];
  const assetValues = buildSeriesData(period.assets, liabilityLabels.length);
  const liabilityValues = [
    ...new Array(assetLabels.length).fill(0),
    ...period.liabilities.map((item) => ({
      value: item.value,
      itemLabel: item.label,
      details: item.details || [],
    })),
  ];
  const assetsTotal = period.assets.reduce((sum, item) => sum + item.value, 0);
  const liabilitiesTotal = period.liabilities.reduce((sum, item) => sum + item.value, 0);
  const stockDisplay = buildStockLabel(currentPayload.symbol, currentNamePayload);
  const stockTitle = `${stockDisplay} ${UI_TEXT.balanceSheet}`;

  stockNameElement.textContent = stockDisplay;
  reportDateElement.textContent = period.report_date;
  assetsTotalElement.textContent = formatYi(assetsTotal);
  liabilitiesTotalElement.textContent = formatYi(liabilitiesTotal);
  chartTitleElement.textContent = stockTitle;

  chart.setOption({
    backgroundColor: "transparent",
    title: {
      text: stockTitle,
      subtext: period.report_date,
      left: "center",
      top: 8,
      textStyle: {
        color: "#f8fafc",
        fontSize: 24,
        fontWeight: 700,
      },
      subtextStyle: {
        color: "#9fb0d6",
        fontSize: 16,
      },
    },
    tooltip: {
      trigger: "axis",
      axisPointer: {
        type: "shadow",
      },
      formatter(params) {
        const activeItem = params.find((item) => item.data && typeof item.data === "object" && item.data.value);
        if (!activeItem) {
          return params[0]?.axisValue || "";
        }

        const lines = [
          `<strong>${activeItem.data.itemLabel || activeItem.axisValue}</strong>`,
          `${activeItem.marker}${activeItem.seriesName}: ${formatYi(activeItem.data.value)}`,
          `<span style="color:#9fb0d6">${UI_TEXT.detail}</span>`,
          formatDetails(activeItem.data.details),
        ];
        return lines.join("<br/>");
      },
    },
    grid: {
      left: 52,
      right: 32,
      top: 96,
      bottom: 72,
    },
    xAxis: {
      type: "category",
      data: labels,
      axisLabel: {
        color: "#dbe4ff",
        interval: 0,
        rotate: 0,
        formatter(value) {
          return value.length > 4 ? value.replace(/&/g, "&\n").replace(/(.{4})/g, "$1\n").trim() : value;
        },
      },
      axisLine: {
        lineStyle: {
          color: "rgba(159, 176, 214, 0.35)",
        },
      },
    },
    yAxis: {
      type: "value",
      name: UI_TEXT.yiYuan,
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
        name: UI_TEXT.assets,
        type: "bar",
        data: assetValues,
        barWidth: 18,
        itemStyle: {
          color: "#4f9cf7",
          borderRadius: [6, 6, 0, 0],
        },
        label: {
          show: true,
          position: "top",
          color: "#dbe4ff",
          formatter: ({ value }) => (value ? Number(value).toFixed(2) : ""),
        },
      },
      {
        name: UI_TEXT.liabilities,
        type: "bar",
        data: liabilityValues,
        barWidth: 18,
        itemStyle: {
          color: "#ff5b3a",
          borderRadius: [6, 6, 0, 0],
        },
        label: {
          show: true,
          position: "top",
          color: "#ffd2ca",
          formatter: ({ value }) => (value ? Number(value).toFixed(2) : ""),
        },
      },
    ],
  });

  updateNavigationState();
}

function renderCurrentPeriod() {
  if (!currentPayload || !currentPayload.periods.length) {
    renderError(UI_TEXT.noPeriodData);
    return;
  }
  renderChart(currentPayload.periods[currentIndex]);
}

async function loadBalanceSheet(symbol, years = "3") {
  const normalizedSymbol = sanitizeSymbolInput(symbol);
  if (normalizedSymbol.length !== 6) {
    renderError(UI_TEXT.invalidSymbol);
    return;
  }

  let normalizedYears;
  try {
    normalizedYears = parseYearsInput(years);
  } catch (error) {
    renderError(error.message);
    return;
  }

  symbolInput.value = normalizedSymbol;
  yearsInput.value = String(normalizedYears);
  setLoadingState(true);

  try {
    currentPayload = null;
    currentNamePayload = null;
    currentIndex = 0;

    const namePromise = fetchStockName(normalizedSymbol);
    const response = await fetch(`/api/stocks/balance-sheet?symbol=${normalizedSymbol}&years=${normalizedYears}`);
    const payload = await response.json();

    if (!response.ok) {
      throw new Error(payload.detail || `${UI_TEXT.requestFailed}: ${response.status}`);
    }

    currentPayload = payload;
    renderCurrentPeriod();
    setLoadingState(false);

    namePromise.then((namePayload) => {
      currentNamePayload = namePayload;
      renderCurrentPeriod();
    });
  } catch (error) {
    currentPayload = null;
    currentNamePayload = null;
    currentIndex = 0;
    renderError(error.message);
    setLoadingState(false);
  }
}

symbolInput.addEventListener("input", (event) => {
  event.target.value = sanitizeSymbolInput(event.target.value);
});

yearsInput.addEventListener("input", (event) => {
  event.target.value = sanitizeYearsInput(event.target.value);
});

balanceForm.addEventListener("submit", (event) => {
  event.preventDefault();
  loadBalanceSheet(symbolInput.value, yearsInput.value);
});

prevButton.addEventListener("click", () => {
  if (currentPayload && currentIndex < currentPayload.periods.length - 1) {
    currentIndex += 1;
    renderCurrentPeriod();
  }
});

currentButton.addEventListener("click", () => {
  if (currentPayload && currentPayload.periods.length) {
    currentIndex = 0;
    renderCurrentPeriod();
  }
});

nextButton.addEventListener("click", () => {
  if (currentPayload && currentIndex > 0) {
    currentIndex -= 1;
    renderCurrentPeriod();
  }
});

window.addEventListener("resize", () => chart.resize());
loadBalanceSheet(symbolInput.value, yearsInput.value);
