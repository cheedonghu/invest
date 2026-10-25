// Run with Node and Playwright; API responses are isolated from the real database.
const assert = require("node:assert/strict");
const http = require("node:http");
const fs = require("node:fs/promises");
const path = require("node:path");
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || "playwright");

const frontend = path.resolve(__dirname, "../frontend");
const sample = "# 投资判断\n\n**长期观察**\n\n- 盈利改善\n- 估值合理\n\n> 持续跟踪\n\n[报告](https://www.cninfo.com.cn/)\n\n```js\nconst value = 1;\n```\n\n| 科目 | 判断 |\n| --- | --- |\n| 现金流 | 充足 |\n\n<script>window.__noteXss = 1</script><img src=x onerror=\"window.__noteXss=2\"><svg onload=\"window.__noteXss=3\"></svg>\n\n[危险链接](javascript:alert(1))";
let items = ["600036", "000001", "300750"].map((symbol, index) => ({
  symbol, name: ["招商银行", "平安银行", "宁德时代"][index], mark_reason: index ? "旧的纯文本备注" : sample,
  created_at: "2026-10-24 10:00:00", updated_at: "2026-10-25 10:00:00",
}));
let failSave = false;
let failDelete = false;
let saveGate;
let releaseSave;
let checks = 0;

const server = http.createServer(async (request, response) => {
  try {
    const pathname = new URL(request.url, "http://localhost").pathname;
    const relative = pathname === "/" ? "index.html" : pathname === "/marked-stocks" ? "marked-stocks.html" : pathname === "/balance-sheet" ? "balance-sheet.html" : pathname === "/valuation" ? "valuation.html" : pathname;
    const filename = path.resolve(frontend, `.${relative.startsWith("/") ? relative : `/${relative}`}`);
    if (!filename.startsWith(frontend + path.sep)) throw new Error("Invalid path");
    const content = await fs.readFile(filename);
    const contentType = { ".html": "text/html", ".js": "text/javascript", ".css": "text/css" }[path.extname(filename)] || "text/plain";
    response.writeHead(200, { "Content-Type": `${contentType}; charset=utf-8` });
    response.end(content);
  } catch {
    response.writeHead(404);
    response.end();
  }
});

async function check(name, run) {
  await run();
  checks += 1;
  console.log(`PASS ${name}`);
}

(async () => {
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const baseUrl = `http://127.0.0.1:${server.address().port}`;
  const browser = await chromium.launch({ headless: true, ...(process.platform === "win32" ? { channel: "msedge" } : {}) });
  try {
    const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
    page.setDefaultTimeout(5000);
    const errors = [];
    page.on("pageerror", (error) => errors.push(error.message));
    const chartScript = process.env.ECHARTS_TEST_SCRIPT ? await fs.readFile(process.env.ECHARTS_TEST_SCRIPT, "utf8") : "window.echarts={registerTheme(){},init:()=>({setOption(){},clear(){},resize(){},getWidth(){return 800;},getOption(){return {};},getZr(){return {on(){}};},on(){},isDisposed(){return false;},dispatchAction(){}})};";
    await page.route("https://cdn.jsdelivr.net/**", (route) => route.fulfill({ contentType: "text/javascript", body: chartScript }));
    await page.route("**/api/stocks/**", async (route) => {
      const request = route.request();
      const url = new URL(request.url());
      const json = (body, status = 200) => route.fulfill({ status, contentType: "application/json", body: JSON.stringify(body) });
      if (url.pathname.startsWith("/api/stocks/marked/")) {
        if (request.method() === "POST") {
          const payload = request.postDataJSON();
          if (saveGate) await saveGate;
          if (failSave) return json({ detail: "模拟保存失败" }, 500);
          let item = items.find((candidate) => candidate.symbol === payload.symbol);
          if (!item) {
            item = { symbol: payload.symbol, name: payload.symbol, created_at: "2026-10-25 11:00:00" };
            items.push(item);
          }
          Object.assign(item, payload, { updated_at: "2026-10-25 11:00:00" });
          return json(item);
        }
        if (request.method() === "DELETE") {
          if (failDelete) return json({ detail: "模拟删除失败" }, 500);
          const symbol = url.pathname.split("/").at(-1);
          items = items.filter((item) => item.symbol !== symbol);
          return json({ symbol });
        }
        return json({ count: items.length, items });
      }
      const symbol = url.searchParams.get("symbol") || "600036";
      if (url.pathname.endsWith("/name")) return json({ symbol, name: "招商银行" });
      if (url.pathname.endsWith("/valuation-metrics")) {
        const series = Array.from({ length: 12 }, (_, index) => ({ date: `2025-${String(index + 1).padStart(2, "0")}-28`, value: 8 + Math.sin(index) }));
        const metric = { series, latest: series.at(-1), stats: { mean: 8, std: 1, upper: 9, lower: 7 } };
        return json({ symbol, metrics: { pe: metric, pb: metric, ps: metric } });
      }
      if (url.pathname.endsWith("/market-performance")) {
        const series = Array.from({ length: 34 }, (_, index) => ({ date: `${2018 + Math.floor(index / 4)}-${["03-31", "06-30", "09-30", "12-31"][index % 4]}`, report_date: `${2018 + Math.floor(index / 4)}-${["03-31", "06-30", "09-30", "12-31"][index % 4]}`, market_cap: 900 + index * 50, revenue: 100 + index * 5, net_profit: 30 + index, audit_opinion: "标准无保留意见" }));
        return json({ symbol, market_performance: { label: "市值与业绩走势", series, latest: series.at(-1), latest_market_cap: { value: 1250, date: "2026-10-23" } } });
      }
      if (url.pathname.endsWith("/balance-sheet")) {
        return json({ symbol, periods: ["2026-06-30", "2026-03-31"].map((report_date) => ({ report_date, assets_total: 980, liabilities_total: 570, assets: [{ label: "货币资金", value: 400 }, { label: "其他流动资产", value: 580 }], liabilities: [{ label: "长期负债", value: 400 }, { label: "其他流动负债", value: 170 }] })) });
      }
      return json({ detail: "未定义的测试接口" }, 503);
    });
    const row = (symbol) => page.locator(`#marked-detail > article[data-symbol="${symbol}"]`);
    const select = async (symbol) => {
      if (await row(symbol).count()) return;
      if (await page.locator('#marked-directory').evaluate((element) => !element.open))
        await page.locator('#marked-directory > summary').click();
      await page.locator('#marked-list > [data-symbol="' + symbol + '"]').click();
      await row(symbol).waitFor();
    };
    const edit = async (symbol, value) => {
      await select(symbol);
      const editButton = row(symbol).locator('.document-edit-btn');
      if (await editButton.isVisible()) await editButton.click();
      await row(symbol).locator("textarea").fill(value);
    };

    await page.goto(`${baseUrl}/marked-stocks`);
    await page.waitForFunction(() => document.getElementById("marked-count").textContent === "3");
    await check("Markdown rendering, legacy text, and XSS filtering", async () => {
      await select("600036");
      assert.equal(await page.locator(".site-brand").count(), 1);
      for (const selector of ["h1", "strong", "ul", "a[href^='https:']", "blockquote", "pre code", "table"])
        assert.ok(await row("600036").locator(`.note-preview ${selector}`).count(), selector);
      assert.equal(await row("600036").locator(".note-preview script, .note-preview img, .note-preview svg, .note-preview [onerror], .note-preview a[href^='javascript:']").count(), 0);
      assert.equal(await page.evaluate(() => window.__noteXss), undefined);
      await select("000001");
      assert.match(await row("000001").locator(".note-preview").textContent(), /旧的纯文本备注/);
    });

    await check("Preview, saving, filtering and refresh preserve other drafts", async () => {
      await edit("000001", "另一只股票的未保存草稿");
      const note = "    indented code\n\n## 更新判断\n\n首行  \n次行\n";
      await edit("600036", note);
      await row("600036").getByRole("button", { name: "预览", exact: true }).click();
      assert.equal(await row("600036").locator(".note-save-btn").evaluate((element) => element === document.activeElement), true);
      assert.equal(await row("600036").locator(".note-preview h2").textContent(), "更新判断");
      await row("600036").locator(".note-save-btn").click();
      await page.waitForFunction(() => document.getElementById("marked-feedback").textContent.includes("600036 的备注已保存"));
      assert.equal(items.find((item) => item.symbol === "600036").mark_reason, note);
      await select("000001");
      assert.equal(await row("000001").locator("textarea").inputValue(), "另一只股票的未保存草稿");
      await page.locator("#marked-sort").selectOption("symbol");
      await page.locator("#marked-search").fill("未保存草稿");
      assert.equal(await page.locator("#marked-list > button").count(), 1);
      await page.locator("#marked-search").fill("");
      await page.locator("#marked-search").fill("没有这个公司xyz");
      assert.equal(await page.locator("#marked-list > button").count(), 0);
      assert.match(await page.locator("#marked-detail").textContent(), /没有匹配的公司/);
      await page.locator("#marked-search").fill("");
      await page.locator("#marked-refresh-btn").click();
      await page.waitForFunction(() => !document.getElementById("marked-refresh-btn").disabled);
      await select("000001");
      assert.equal(await row("000001").locator("textarea").inputValue(), "另一只股票的未保存草稿");
    });

    await check("Typing during a save keeps newer changes", async () => {
      await edit("600036", "已提交的版本");
      saveGate = new Promise((resolve) => { releaseSave = resolve; });
      await row("600036").locator(".note-save-btn").click();
      await row("600036").locator("textarea").fill("请求期间继续编辑的版本");
      releaseSave();
      saveGate = undefined;
      await page.waitForFunction(() => document.querySelector('article[data-symbol="600036"] .note-save-btn').textContent === "保存修改");
      assert.equal(await row("600036").locator("textarea").inputValue(), "请求期间继续编辑的版本");
      assert.equal(items.find((item) => item.symbol === "600036").mark_reason, "已提交的版本");
    });

    await check("Save and delete failures keep the note and draft", async () => {
      failSave = true;
      await select("600036");
      await row("600036").locator(".note-save-btn").click();
      await page.waitForFunction(() => document.getElementById("marked-feedback").textContent === "模拟保存失败");
      assert.equal(await row("600036").locator("textarea").inputValue(), "请求期间继续编辑的版本");
      failSave = false;
      failDelete = true;
      page.once("dialog", (dialog) => dialog.accept());
      await row("600036").locator(".note-delete-btn").click();
      await page.waitForFunction(() => document.getElementById("marked-feedback").textContent === "模拟删除失败");
      assert.equal(await row("600036").locator("textarea").inputValue(), "请求期间继续编辑的版本");
      failDelete = false;
    });

    await check("Delete cancel, success, count, and other drafts", async () => {
      page.once("dialog", (dialog) => dialog.dismiss());
      await row("600036").locator(".note-delete-btn").click();
      assert.equal(await row("600036").count(), 1);
      page.once("dialog", (dialog) => dialog.accept());
      await row("600036").locator(".note-delete-btn").click();
      await row("600036").waitFor({ state: "detached" });
      assert.equal(await page.locator("#marked-count").textContent(), "2");
      await select("000001");
      assert.equal(await row("000001").locator("textarea").inputValue(), "另一只股票的未保存草稿");
    });

    await check("Long Markdown can be added and displayed", async () => {
      const note = "## 重新关注\n\n" + "长篇研究记录。\n".repeat(200);
      await page.locator("#marked-new-btn").click();
      assert.equal(await page.locator("#marked-symbol").evaluate((element) => element === document.activeElement), true);
      await page.locator("#marked-symbol").fill("000001");
      await page.locator("#marked-reason").fill("不能覆盖目录中的未保存修改");
      await page.locator("#marked-save-btn").click();
      assert.match(await page.locator("#marked-dialog-feedback").textContent(), /有未保存修改/);
      await page.locator("#marked-dialog-close").click();
      await page.locator("#marked-new-btn").click();
      assert.equal(await page.locator("#marked-reason").inputValue(), "不能覆盖目录中的未保存修改");
      await page.locator("#marked-symbol").fill("600036");
      await page.locator("#marked-reason").fill(note);
      failSave = true;
      await page.locator("#marked-save-btn").click();
      await page.waitForFunction(() => document.getElementById("marked-dialog-feedback").textContent === "模拟保存失败");
      assert.equal(await page.locator("#marked-add-dialog").isVisible(), true);
      assert.equal(await page.locator("#marked-reason").inputValue(), note);
      failSave = false;
      await page.locator("#marked-note-editor .note-preview-toggle").click();
      assert.equal(await page.locator("#marked-note-editor .note-preview h2").textContent(), "重新关注");
      await page.locator("#marked-save-btn").click();
      await row("600036").waitFor();
      assert.equal(await row("600036").locator(".document-edit-btn").evaluate((element) => element === document.activeElement), true);
      assert.equal(items.find((item) => item.symbol === "600036").mark_reason, note);
      assert.equal(await page.locator("#marked-reason").inputValue(), "");
      assert.equal(await row("600036").locator(".note-preview h2").textContent(), "重新关注");
    });

    await check("Mobile Markdown tables retain columns without page overflow", async () => {
      const headers = Array.from({ length: 15 }, (_, index) => `年份${index + 1}`);
      const wideTable = `\n\n| ${headers.join(" | ")} |\n| ${headers.map(() => "---").join(" | ")} |\n| ${headers.map(() => "100").join(" | ")} |`;
      await edit("300750", sample + wideTable);
      await row("300750").locator(".note-preview-toggle").click();
      await page.setViewportSize({ width: 390, height: 844 });
      assert.equal(await row("300750").locator(".note-preview td").first().evaluate((cell) => getComputedStyle(cell).display), "table-cell");
      assert.equal(await row("300750").locator(".note-preview thead").first().evaluate((head) => getComputedStyle(head).display), "table-header-group");
      assert.equal(await row("300750").locator(".note-preview thead").first().isVisible(), true);
      const scroll = row("300750").locator(".markdown-table-scroll").last();
      assert.ok(await scroll.evaluate((element) => { element.scrollLeft = element.scrollWidth; return element.scrollLeft > 0 && getComputedStyle(element).overflowX === "auto"; }));
      assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
      if (process.env.UI_OUTPUT) {
        await fs.mkdir(process.env.UI_OUTPUT, { recursive: true });
        await page.screenshot({ path: path.join(process.env.UI_OUTPUT, "marked-stocks-mobile.png"), fullPage: true });
      }
    });

    // Clear drafts before navigation so the test does not suppress the unload warning.
    for (const symbol of ["000001", "300750"]) {
      await select(symbol);
      await row(symbol).locator(".note-save-btn").click();
      await page.waitForFunction((code) => document.querySelector(`article[data-symbol="${code}"] .note-save-btn`).textContent === "已保存", symbol);
    }
    await check("Deeply nested Markdown falls back to editable, deletable source", async () => {
      const nested = "> ".repeat(5000) + "note";
      items.push({ symbol: "600519", name: "嵌套测试", mark_reason: nested });
      await page.goto(`${baseUrl}/marked-stocks`);
      await page.waitForFunction(() => document.getElementById("marked-count").textContent === "4");
      await select("600519");
      assert.equal(await row("600519").locator(".note-preview").textContent(), nested);
      await row("600519").locator(".document-edit-btn").click();
      assert.equal(await row("600519").locator("textarea").inputValue(), nested);
      page.once("dialog", (dialog) => dialog.accept());
      await row("600519").locator(".note-delete-btn").click();
      await row("600519").waitFor({ state: "detached" });
    });
    await check("Home entry shares Markdown source, preview, saved display and long note support", async () => {
      await page.setViewportSize({ width: 1440, height: 1000 });
      await page.goto(baseUrl);
      await page.locator(".home-note-disclosure > summary").click();
      const homeHeading = page.locator(".note-controls h2");
      assert.ok((await homeHeading.boundingBox()).y < (await page.locator("#stock-note-editor").boundingBox()).y);
      await page.locator("#stock-symbol").fill("600036");
      const note = "    indented code\n\n# 首页备注\n\n" + "长期判断。  \n".repeat(130);
      await page.locator("#stock-mark-reason").fill(note);
      await page.locator("#stock-note-editor .note-preview-toggle").click();
      assert.equal(await page.locator("#stock-note-editor h1").textContent(), "首页备注");
      await page.locator("#mark-btn").click();
      await page.waitForFunction(() => document.getElementById("mark-feedback").textContent.includes("备注已保存"));
      assert.equal(items.find((item) => item.symbol === "600036").mark_reason, note);
      assert.equal(await page.locator("#stock-mark-reason").inputValue(), note);
      assert.equal(await page.locator("#stock-mark-reason").isVisible(), false);
      if (process.env.UI_OUTPUT) await page.screenshot({ path: path.join(process.env.UI_OUTPUT, "stock-note-home.png"), fullPage: true });
      await page.setViewportSize({ width: 390, height: 844 });
      await page.waitForFunction(() => document.documentElement.scrollWidth <= innerWidth);
    });
    await check("All four pages share the paper theme, work at desktop and mobile, and render charts", async () => {
      for (const width of [1440, 390, 320]) {
        await page.setViewportSize({ width, height: 1000 });
        for (const route of ["/marked-stocks", "/", "/balance-sheet", "/valuation"]) {
          await page.goto(baseUrl + route);
          await page.locator(".site-brand").waitFor();
          if (route === "/") await page.waitForFunction(() => document.getElementById("latest-pe").textContent !== "-");
          if (route === "/balance-sheet") {
            await page.waitForFunction(() => document.getElementById("balance-report-date").textContent === "2026-06-30");
            await page.locator("#prev-period-btn").click();
            assert.equal(await page.locator("#balance-report-date").textContent(), "2026-03-31");
          }
          if (route === "/valuation") assert.match(await page.locator("#ddm-result").textContent(), /52.00/);
          assert.equal(await page.evaluate(() => getComputedStyle(document.body).backgroundColor), "rgb(245, 243, 238)");
          assert.equal(await page.locator(".nav-link[aria-current='page']").count(), 1);
          assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), `${route} overflows at ${width}px`);
          if (process.env.ECHARTS_TEST_SCRIPT && ["/", "/balance-sheet"].includes(route)) {
            const palette = await page.evaluate((id) => { const option = echarts.getInstanceByDom(document.getElementById(id)).getOption(); return { title: option.title[0].textStyle.color, tooltip: option.tooltip[0].backgroundColor }; }, route === "/" ? "pe-chart" : "balance-chart");
            assert.deepEqual(palette, { title: "#343b35", tooltip: "#fffefa" });
            if (route === "/" && width === 1440) {
              await page.evaluate(() => ["pe-chart", "market-performance-chart"].forEach((id) => {
                const chart = echarts.getInstanceByDom(document.getElementById(id));
                chart.dispatchAction({ type: "legendUnSelect", name: chart.getOption().series[0].name });
              }));
              for (const nextWidth of [390, 1440]) {
                await page.setViewportSize({ width: nextWidth, height: 1000 });
                await page.waitForFunction((small) => (echarts.getInstanceByDom(document.getElementById("pe-chart")).getWidth() < 720) === small, nextWidth === 390);
                const retained = await page.evaluate(() => ["pe-chart", "market-performance-chart"].every((id) => {
                  const option = echarts.getInstanceByDom(document.getElementById(id)).getOption();
                  return option.legend[0].selected[option.series[0].name] === false;
                }));
                assert.equal(retained, true, "Resizing must retain hidden chart series");
              }
              await page.evaluate(() => ["pe-chart", "market-performance-chart"].forEach((id) => {
                const chart = echarts.getInstanceByDom(document.getElementById(id));
                chart.dispatchAction({ type: "legendSelect", name: chart.getOption().series[0].name });
              }));
            }
          }
          if (process.env.UI_OUTPUT && width !== 320) {
            const title = route === "/" ? "home" : route.slice(1);
            await page.screenshot({ path: path.join(process.env.UI_OUTPUT, `${title}-${width}.png`), fullPage: true });
          }
        }
      }
    });
    if (process.env.ECHARTS_TEST_SCRIPT) await check("Market chart hover follows the cursor directly and otherwise stays native", async () => {
      await page.setViewportSize({ width: 1440, height: 1000 });
      await page.goto(baseUrl);
      await page.waitForFunction(() => document.getElementById("latest-pe").textContent !== "-");
      const comparison = await page.evaluate(() => {
        const current = echarts.getInstanceByDom(document.getElementById("market-performance-chart"));
        const element = document.createElement("div");
        element.style.cssText = "width:800px;height:390px;position:absolute;left:-10000px";
        document.body.append(element);
        const baseline = echarts.init(element);
        try {
          baseline.setOption({ tooltip: { trigger: "axis", axisPointer: { type: "shadow" } }, xAxis: { type: "category", data: ["a", "b"] }, yAxis: {}, series: [{ type: "line", data: [1, 2] }, { type: "bar", data: [2, 3] }] });
          function interaction(chart) {
            const option = chart.getOption();
            const tooltip = option.tooltip[0];
            return {
              // The hover motion (tooltip glide, shadow-band slide) is deliberately off; the motion check below covers it.
              tooltip: Object.fromEntries(["trigger", "triggerOn", "showDelay", "hideDelay", "confine", "extraCssText", "enterable"].map((key) => [key, tooltip[key]])),
              axisPointer: Object.fromEntries(["animation", "animationDurationUpdate", "animationEasingUpdate", "triggerOn"].map((key) => [key, option.axisPointer[0][key]])),
              stateAnimation: option.stateAnimation,
              emphasis: ["line", "bar"].map((type) => option.series.find((series) => series.type === type).emphasis),
              dirtyRect: Boolean(chart.getZr().painter._opts.useDirtyRect),
            };
          }
          return { current: interaction(current), baseline: interaction(baseline), customController: typeof window.PaperCharts };
        } finally {
          baseline.dispose();
          element.remove();
        }
      });
      assert.deepEqual(comparison.current, comparison.baseline, "Apart from the hover motion, chart interaction must stay native");
      assert.equal(comparison.customController, "undefined");
      await page.locator("#market-performance-chart").scrollIntoViewIfNeeded();
      const points = await page.evaluate(() => {
        const element = document.getElementById("market-performance-chart");
        const chart = echarts.getInstanceByDom(element);
        const rect = element.getBoundingClientRect();
        return [2, 26, 5, 29, 11].map((index) => {
          const center = chart.convertToPixel({ xAxisIndex: 0 }, index);
          return { x: rect.left + center, y: rect.top + 260, center, date: chart.getOption().xAxis[0].data[index] };
        });
      });
      for (const point of points) {
        await page.mouse.move(point.x, point.y);
        await page.waitForFunction((date) => {
          const tooltip = Array.from(document.getElementById("market-performance-chart").querySelectorAll("div")).find((node) => node.style.zIndex === "9999999");
          return tooltip?.textContent.includes(date);
        }, point.date);
      }
      const tooltip = page.locator('#market-performance-chart div[style*="z-index: 9999999"]');
      const text = await tooltip.textContent();
      for (const value of ["2020-12-31", "总市值: 1450.00 亿", "营收(TTM): 155.00 亿", "净利润(TTM): 41.00 亿", "审计意见: 标准无保留意见"]) assert.ok(text.includes(value), value);
      // Each jump crosses 15+ categories, so a glide would leave the tooltip or shadow band visibly short on the next frame.
      const hoverState = () => page.evaluate(async () => {
        await new Promise(requestAnimationFrame);
        const element = document.getElementById("market-performance-chart");
        const tip = Array.from(element.querySelectorAll("div")).find((node) => node.style.zIndex === "9999999");
        const band = echarts.getInstanceByDom(element).getZr().storage.getDisplayList(true).find((node) => node.style?.fill === "rgba(210,219,238,0.2)");
        const { x, y } = tip.getBoundingClientRect();
        return { tooltip: [x, y], bandCenter: band ? band.shape.x + band.shape.width / 2 : null };
      });
      for (const point of [points[1], points[2]]) {
        await page.waitForTimeout(600);
        await page.mouse.move(point.x, point.y);
        const next = await hoverState();
        await page.waitForTimeout(600);
        const settled = await hoverState();
        const tooltipGap = Math.hypot(next.tooltip[0] - settled.tooltip[0], next.tooltip[1] - settled.tooltip[1]);
        assert.ok(tooltipGap < 1, `Tooltip must reach ${point.date} on the next frame, was ${tooltipGap.toFixed(1)}px short`);
        assert.ok(next.bandCenter !== null && Math.abs(next.bandCenter - point.center) < 1, `Shadow band must cover ${point.date} on the next frame, was ${Math.abs(next.bandCenter - point.center).toFixed(1)}px away`);
      }
      const drift = await tooltip.evaluate(async (element) => {
        const positions = [];
        for (let frame = 0; frame < 20; frame += 1) {
          await new Promise(requestAnimationFrame);
          const rect = element.getBoundingClientRect();
          positions.push([rect.x, rect.y]);
        }
        return Math.max(...[0, 1].map((axis) => Math.max(...positions.map((point) => point[axis])) - Math.min(...positions.map((point) => point[axis]))));
      });
      assert.ok(drift < 0.5, `Tooltip must not keep drifting at rest: ${drift}px`);
      await page.evaluate(() => {
        const chart = echarts.getInstanceByDom(document.getElementById("market-performance-chart"));
        const original = chart.setOption;
        window.__resizeRebuildCount = 0;
        window.__restoreSetOption = () => { chart.setOption = original; };
        chart.setOption = function (...args) { window.__resizeRebuildCount += 1; return original.apply(this, args); };
      });
      await page.setViewportSize({ width: 1439, height: 960 });
      await page.waitForFunction(() => echarts.getInstanceByDom(document.getElementById("market-performance-chart")).getWidth() === document.getElementById("market-performance-chart").clientWidth);
      const rebuilds = await page.evaluate(() => { const count = window.__resizeRebuildCount; window.__restoreSetOption(); delete window.__restoreSetOption; delete window.__resizeRebuildCount; return count; });
      assert.equal(rebuilds, 0, "Small viewport changes must not rebuild chart series");
    });
    assert.deepEqual(errors, []);
    console.log(`${checks} browser checks passed`);
  } finally {
    await browser.close();
  }
})().catch((error) => { console.error(error); process.exitCode = 1; }).finally(() => server.close());
