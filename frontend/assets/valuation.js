const valuationText = {
  yi: "亿",
  perShare: "元/股",
};

function parsePositiveNumber(value, label) {
  const normalized = String(value).trim();
  const parsed = Number.parseFloat(normalized);
  if (Number.isNaN(parsed) || parsed <= 0) {
    throw new Error(`${label}需要输入大于 0 的数值`);
  }
  return parsed;
}

function formatYi(value) {
  return `${value.toFixed(2)} ${valuationText.yi}`;
}

function formatPerShare(value) {
  return `${value.toFixed(2)} ${valuationText.perShare}`;
}

function renderResult(elementId, title, lines) {
  const element = document.getElementById(elementId);
  element.innerHTML = `
    <h4>${title}</h4>
    ${lines.map((line) => `<p>${line}</p>`).join("")}
  `;
}

function renderError(elementId, message) {
  const element = document.getElementById(elementId);
  element.innerHTML = `<h4>输入有误</h4><p>${message}</p>`;
}

document.getElementById("dcf-form").addEventListener("submit", (event) => {
  event.preventDefault();
  try {
    const baseCashflow = parsePositiveNumber(document.getElementById("dcf-base-cashflow").value, "基准自由现金流");
    const years = Math.round(parsePositiveNumber(document.getElementById("dcf-years").value, "持续时间"));
    const growthRate = parsePositiveNumber(document.getElementById("dcf-growth").value, "成长性") / 100;
    const discountRate = parsePositiveNumber(document.getElementById("dcf-discount-rate").value, "折现率") / 100;

    if (discountRate <= growthRate) {
      throw new Error("折现率需要高于成长性，否则现值会被放大失真");
    }

    let presentValue = 0;
    let currentCashflow = baseCashflow;
    for (let year = 1; year <= years; year += 1) {
      currentCashflow *= 1 + growthRate;
      presentValue += currentCashflow / ((1 + discountRate) ** year);
    }

    const terminalValue = (currentCashflow * (1 + growthRate)) / (discountRate - growthRate);
    const terminalPresentValue = terminalValue / ((1 + discountRate) ** years);
    const totalValue = presentValue + terminalPresentValue;

    renderResult("dcf-result", "DCF 估值结果", [
      `明确预测期现值：${formatYi(presentValue)}`,
      `终值折现：${formatYi(terminalPresentValue)}`,
      `总估值：${formatYi(totalValue)}`,
    ]);
  } catch (error) {
    renderError("dcf-result", error.message);
  }
});

document.getElementById("ddm-form").addEventListener("submit", (event) => {
  event.preventDefault();
  try {
    const dividend = parsePositiveNumber(document.getElementById("ddm-dividend").value, "每股股息");
    const growthRate = parsePositiveNumber(document.getElementById("ddm-growth").value, "股息增速") / 100;
    const requiredRate = parsePositiveNumber(document.getElementById("ddm-required-rate").value, "要求回报率") / 100;

    if (requiredRate <= growthRate) {
      throw new Error("要求回报率需要高于股息增速");
    }

    const nextDividend = dividend * (1 + growthRate);
    const intrinsicValue = nextDividend / (requiredRate - growthRate);

    renderResult("ddm-result", "DDM 估值结果", [
      `下一期股息：${formatPerShare(nextDividend)}`,
      `内在价值：${formatPerShare(intrinsicValue)}`,
      "适合现金分红比较稳定的公司。",
    ]);
  } catch (error) {
    renderError("ddm-result", error.message);
  }
});

document.getElementById("nav-form").addEventListener("submit", (event) => {
  event.preventDefault();
  try {
    const bookValue = parsePositiveNumber(document.getElementById("nav-book-value").value, "净资产");
    const qualityFactor = parsePositiveNumber(document.getElementById("nav-quality-factor").value, "资产质量系数");
    const safetyMargin = parsePositiveNumber(document.getElementById("nav-safety-margin").value, "安全边际") / 100;

    if (safetyMargin >= 1) {
      throw new Error("安全边际需要小于 100%。");
    }

    const adjustedBookValue = bookValue * qualityFactor;
    const targetValue = adjustedBookValue * (1 - safetyMargin);

    renderResult("nav-result", "净资产估值结果", [
      `调整后净资产：${formatYi(adjustedBookValue)}`,
      `扣除安全边际后参考价值：${formatYi(targetValue)}`,
      "适合重资产、资产负债表比较关键的公司。",
    ]);
  } catch (error) {
    renderError("nav-result", error.message);
  }
});

document.getElementById("dcf-form").dispatchEvent(new Event("submit"));
document.getElementById("ddm-form").dispatchEvent(new Event("submit"));
document.getElementById("nav-form").dispatchEvent(new Event("submit"));
