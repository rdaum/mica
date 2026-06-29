import { bootstrapServerRenderedSync } from "/sync-client.js?surface=agent";
window.micaAgent = bootstrapServerRenderedSync(document.getElementById("mount"));

const RIGHT_COLUMN_WIDTH_KEY = "micaAgentRightColumnWidth";
const MIN_RIGHT_COLUMN_WIDTH = 280;
const MIN_LEFT_COLUMN_WIDTH = 420;

function cssEscape(value) {
  return window.CSS?.escape ? CSS.escape(value) : String(value).replaceAll('"', '\\"');
}

function applySavedColumnWidth() {
  const saved = window.localStorage?.getItem(RIGHT_COLUMN_WIDTH_KEY);
  if (saved) {
    document.documentElement.style.setProperty("--agent-right-width", saved);
  }
}

function installColumnSplitter(mount) {
  let drag = null;

  mount.addEventListener("pointerdown", (event) => {
    const splitter = event.target?.closest?.(".column-splitter");
    if (!splitter || !mount.contains(splitter)) {
      return;
    }

    const columns = splitter.closest(".agent-columns");
    if (!columns) {
      return;
    }

    const right = columns.querySelector(".agent-right-column");
    if (!right) {
      return;
    }

    drag = {
      splitter,
      columns,
      pointerId: event.pointerId,
    };
    splitter.classList.add("is-dragging");
    splitter.setPointerCapture?.(event.pointerId);
    event.preventDefault();
  });

  mount.addEventListener("pointermove", (event) => {
    if (!drag || drag.pointerId !== event.pointerId) {
      return;
    }

    const bounds = drag.columns.getBoundingClientRect();
    const maxRightWidth = Math.max(MIN_RIGHT_COLUMN_WIDTH, bounds.width - MIN_LEFT_COLUMN_WIDTH);
    const nextWidth = Math.min(Math.max(bounds.right - event.clientX, MIN_RIGHT_COLUMN_WIDTH), maxRightWidth);
    const value = `${Math.round(nextWidth)}px`;
    document.documentElement.style.setProperty("--agent-right-width", value);
    window.localStorage?.setItem(RIGHT_COLUMN_WIDTH_KEY, value);
  });

  function finishDrag(event) {
    if (!drag || drag.pointerId !== event.pointerId) {
      return;
    }
    drag.splitter.classList.remove("is-dragging");
    drag.splitter.releasePointerCapture?.(event.pointerId);
    drag = null;
  }

  mount.addEventListener("pointerup", finishDrag);
  mount.addEventListener("pointercancel", finishDrag);
}

applySavedColumnWidth();
installColumnSplitter(document.getElementById("mount"));
installQueueModeToggle(document.getElementById("mount"));

// Before the sync-client's submit handler runs, set the hidden "mode"
// field based on whether shift was held. Shift+Enter during streaming
// queues the message as a follow-up instead of steering.
function installQueueModeToggle(mount) {
  mount.addEventListener("submit", (event) => {
    const form = event.target?.closest?.("form");
    if (!form || !mount.contains(form)) {
      return;
    }
    if (form.dataset.syncAction !== "agent_command") {
      return;
    }
    let modeInput = form.querySelector('input[name="mode"]');
    if (!modeInput) {
      modeInput = document.createElement("input");
      modeInput.type = "hidden";
      modeInput.name = "mode";
      form.appendChild(modeInput);
    }
    modeInput.value = event.shiftKey ? "follow_up" : "";
  }, true);
}

function closeDetails(details) {
  if (details) {
    details.open = false;
  }
}

function installToolWindows(mount) {
  mount.addEventListener("click", (event) => {
    const close = event.target?.closest?.("[data-close-details]");
    if (close && mount.contains(close)) {
      closeDetails(close.closest("details"));
      event.preventDefault();
    }
  });

  window.addEventListener("keydown", (event) => {
    if (event.key !== "Escape") {
      return;
    }
    let closed = false;
    for (const details of mount.querySelectorAll("details.tool-sheet[open]")) {
      closeDetails(details);
      closed = true;
    }
    if (closed) {
      event.preventDefault();
    }
  });
}

installToolWindows(document.getElementById("mount"));
installAtCompletion(document.getElementById("mount"));

function installAtCompletion(mount) {
  let dropdown = null;
  let activeInput = null;
  let items = [];
  let selectedIndex = 0;
  let fetchController = null;

  function createDropdown() {
    const el = document.createElement("div");
    el.className = "at-completion-dropdown";
    el.setAttribute("role", "listbox");
    el.style.display = "none";
    mount.appendChild(el);
    return el;
  }

  function destroyDropdown() {
    if (dropdown) {
      dropdown.remove();
      dropdown = null;
    }
    if (fetchController) {
      fetchController.abort();
      fetchController = null;
    }
    items = [];
    selectedIndex = 0;
    activeInput = null;
  }

  function renderItems() {
    if (!dropdown) return;
    if (items.length === 0) {
      dropdown.innerHTML = '<div class="at-completion-empty">No files found</div>';
      return;
    }
    dropdown.innerHTML = items
      .map((item, i) => {
        const cls = i === selectedIndex ? "at-completion-item selected" : "at-completion-item";
        const lang = item.language ? `<span class="at-completion-lang">${item.language}</span>` : "";
        return `<div class="${cls}" role="option" data-index="${i}">` +
          `<span class="at-completion-path">${item.path}</span>${lang}</div>`;
      })
      .join("");
  }

  function positionDropdown(input) {
    if (!dropdown) return;
    const rect = input.getBoundingClientRect();
    dropdown.style.left = `${rect.left}px`;
    dropdown.style.bottom = `${window.innerHeight - rect.top + 2}px`;
    dropdown.style.minWidth = `${Math.max(rect.width, 300)}px`;
  }

  async function fetchCompletions(query) {
    if (fetchController) {
      fetchController.abort();
    }
    fetchController = new AbortController();
    try {
      const res = await fetch(`/agent/api/completions?q=${encodeURIComponent(query)}`, {
        signal: fetchController.signal,
      });
      const data = await res.json();
      items = data.items || [];
      selectedIndex = 0;
      renderItems();
      if (items.length > 0) {
        dropdown.style.display = "block";
        positionDropdown(activeInput);
      } else {
        dropdown.style.display = "none";
      }
    } catch {
      // aborted or network error
    }
  }

  function findAtPrefix(text, cursorPos) {
    const before = text.slice(0, cursorPos);
    for (let i = before.length - 1; i >= 0; i--) {
      const ch = before[i];
      if (ch === "@") {
        if (i === 0 || before[i - 1] === " " || before[i - 1] === "\t" || before[i - 1] === "\n") {
          return before.slice(i + 1);
        }
        return null;
      }
      if (ch === " " || ch === "\t" || ch === "\n") {
        return null;
      }
    }
    return null;
  }

  function applyCompletion(item, prefixLength) {
    const input = activeInput;
    if (!input) return;
    const before = input.value.slice(0, input.selectionStart - prefixLength);
    const after = input.value.slice(input.selectionEnd);
    const rawPath = item.path;
    const path = rawPath.startsWith("/") || rawPath.startsWith("./") ? rawPath : `./${rawPath}`;
    const insert = `${path} `;
    input.value = before + insert + after;
    const cursorPos = before.length + insert.length;
    input.setSelectionRange(cursorPos, cursorPos);
    input.dispatchEvent(new Event("input", { bubbles: true }));
    input.focus();
  }

  mount.addEventListener("input", (event) => {
    const input = event.target;
    if (!input || input.name !== "text" || !input.closest('[data-sync-action="agent_command"]')) {
      return;
    }
    const prefix = findAtPrefix(input.value, input.selectionStart);
    if (prefix === null) {
      if (dropdown) {
        destroyDropdown();
      }
      return;
    }
    if (!dropdown) {
      dropdown = createDropdown();
      activeInput = input;
    }
    activeInput = input;
    fetchCompletions(prefix);
  });

  mount.addEventListener("click", (event) => {
    const item = event.target?.closest?.(".at-completion-item");
    if (item && dropdown && dropdown.contains(item)) {
      const idx = parseInt(item.dataset.index, 10);
      const selected = items[idx];
      if (selected) {
        const text = activeInput.value;
        const cursor = activeInput.selectionStart;
        const before = text.slice(0, cursor);
        const atPos = before.lastIndexOf("@");
        if (atPos >= 0) {
          const prefixLen = cursor - atPos - 1;
          applyCompletion(selected, prefixLen);
        }
      }
      destroyDropdown();
      event.preventDefault();
      return;
    }
    if (dropdown && activeInput && !activeInput.contains(event.target) && !dropdown.contains(event.target)) {
      destroyDropdown();
    }
  });

  window.addEventListener("keydown", (event) => {
    if (!dropdown || dropdown.style.display === "none" || !activeInput) {
      return;
    }
    if (event.key === "Escape") {
      destroyDropdown();
      event.preventDefault();
      return;
    }
    if (event.key === "ArrowDown") {
      event.preventDefault();
      selectedIndex = Math.min(selectedIndex + 1, items.length - 1);
      renderItems();
      return;
    }
    if (event.key === "ArrowUp") {
      event.preventDefault();
      selectedIndex = Math.max(selectedIndex - 1, 0);
      renderItems();
      return;
    }
    if (event.key === "Enter" || event.key === "Tab") {
      if (items.length === 0) return;
      event.preventDefault();
      const selected = items[selectedIndex];
      const text = activeInput.value;
      const cursor = activeInput.selectionStart;
      const before = text.slice(0, cursor);
      const atPos = before.lastIndexOf("@");
      if (atPos >= 0) {
        const prefixLen = cursor - atPos - 1;
        applyCompletion(selected, prefixLen);
      }
      destroyDropdown();
      return;
    }
  });

  window.addEventListener("scroll", () => {
    if (dropdown && activeInput) {
      positionDropdown(activeInput);
    }
  }, true);
}