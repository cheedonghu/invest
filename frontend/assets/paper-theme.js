// Shared chart surfaces and typography for the paper notebook interface.
(() => {
  const css = getComputedStyle(document.documentElement);
  const color = (name) => css.getPropertyValue(name).trim();
  const ink = color("--text-main");
  const muted = color("--text-soft");
  const border = color("--panel-border");
  const axis = {
    axisLine: { lineStyle: { color: border } },
    axisTick: { show: false },
    axisLabel: { color: muted },
    nameTextStyle: { color: muted },
    splitLine: { lineStyle: { color: border } },
  };
  echarts.registerTheme("paper-notebook", {
    color: [color("--accent"), color("--chart-blue"), color("--chart-gold"), color("--chart-clay")],
    textStyle: { color: ink, fontFamily: '"Segoe UI", "Microsoft YaHei", sans-serif' },
    title: { textStyle: { color: ink }, subtextStyle: { color: muted } },
    legend: { textStyle: { color: muted } },
    // Keep interaction and motion at ECharts defaults; this theme is visual only.
    tooltip: {
      backgroundColor: color("--paper-sheet"), borderColor: border,
      textStyle: { color: ink }, borderRadius: 5,
    },
    categoryAxis: axis, valueAxis: axis,
  });

})();
