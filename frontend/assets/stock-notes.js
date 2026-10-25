/* Shared Markdown editor for stock notes. Dependencies are bundled in assets/vendor. */
(() => {
  const MAX_LENGTH = 100000;
  const allowedTags = ["p", "br", "h1", "h2", "h3", "h4", "h5", "h6", "strong", "em", "del",
    "ul", "ol", "li", "a", "blockquote", "pre", "code", "table", "thead", "tbody", "tr", "th", "td", "hr"];

  function validate(value) {
    if (!value.trim()) return "请输入备注。";
    if (Array.from(value).length > MAX_LENGTH) return "备注最多支持 100000 个字符。";
    return "";
  }

  function render(target, value) {
    target.classList.remove("note-plain");
    try {
      if (!window.marked || !window.DOMPurify) throw new Error("Markdown dependency unavailable");
      const html = window.marked.parse(value || "", { gfm: true, breaks: true, async: false });
      target.replaceChildren(window.DOMPurify.sanitize(html, {
        ALLOWED_TAGS: allowedTags,
        ALLOWED_ATTR: ["href", "title", "start", "align"],
        ALLOW_DATA_ATTR: false,
        ALLOW_ARIA_ATTR: false,
        RETURN_DOM_FRAGMENT: true,
      }));
      target.querySelectorAll("table").forEach((table) => {
        const wrapper = document.createElement("div");
        wrapper.className = "markdown-table-scroll";
        wrapper.setAttribute("role", "region");
        wrapper.setAttribute("aria-label", "备注表格，可横向滚动查看");
        wrapper.tabIndex = 0;
        table.replaceWith(wrapper);
        wrapper.append(table);
      });
      if (!target.textContent.trim()) target.textContent = "暂无可预览内容";
    } catch {
      // Unusual Markdown or unavailable dependencies must not block editing/deletion.
      target.textContent = value || "暂无备注";
      target.classList.add("note-plain");
    }
  }

  function mount(root, { id, label = "备注", value = "", viewing = false, onInput, onModeChange, onPreview } = {}) {
    root.classList.add("note-editor");
    root.innerHTML = `<div class="note-editor-toolbar" role="group" aria-label="备注编辑模式">
      <button type="button" class="secondary-btn note-edit-toggle">编辑</button>
      <button type="button" class="secondary-btn note-preview-toggle">预览</button>
      <small>支持 Markdown · 最多 10 万字</small>
    </div>
    <textarea class="note-source table-reason-input" rows="5" aria-label="备注 Markdown 原文" placeholder="支持 Markdown，例如：## 投资判断"></textarea>
    <div class="note-preview markdown-body" role="region" aria-label="备注预览"></div>`;
    const input = root.querySelector("textarea");
    const preview = root.querySelector(".note-preview");
    const editButton = root.querySelector(".note-edit-toggle");
    const previewButton = root.querySelector(".note-preview-toggle");
    preview.tabIndex = -1;
    if (id) input.id = id;
    input.setAttribute("aria-label", `${label} Markdown 原文`);
    preview.setAttribute("aria-label", `${label}预览`);
    input.value = value;

    function setMode(previewing, focus = false) {
      viewing = previewing;
      input.hidden = viewing;
      preview.hidden = !viewing;
      editButton.setAttribute("aria-pressed", String(!viewing));
      previewButton.setAttribute("aria-pressed", String(viewing));
      if (viewing) render(preview, input.value);
      else if (focus) input.focus();
      if (onModeChange) onModeChange(viewing);
    }
    editButton.addEventListener("click", () => setMode(false, true));
    previewButton.addEventListener("click", () => {
      setMode(true);
      if (onPreview) onPreview();
      else preview.focus({ preventScroll: true });
    });
    input.addEventListener("input", () => { if (onInput) onInput(input.value); });
    setMode(viewing);
    return {
      input,
      get value() { return input.value; },
      setValue(nextValue, previewing = viewing) { input.value = nextValue; setMode(previewing); },
      showPreview() { setMode(true); },
      showEdit() { setMode(false, true); },
    };
  }

  window.StockNotes = { MAX_LENGTH, validate, render, mount };
})();
