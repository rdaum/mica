import { bootstrapServerRenderedSync } from "/sync-client.js?surface=mud";
window.micaMud = bootstrapServerRenderedSync(document.getElementById("mount"));

const HOLD_TO_EXAMINE_MS = 520;
const RIGHT_COLUMN_WIDTH_KEY = "micaMudRightColumnWidth";
const TOOL_WINDOW_STATE_PREFIX = "micaMudToolWindow:";
const MIN_RIGHT_COLUMN_WIDTH = 280;
const MIN_LEFT_COLUMN_WIDTH = 420;
const MIN_WINDOW_WIDTH = 320;
const MIN_WINDOW_HEIGHT = 240;

function cssEscape(value) {
  return window.CSS?.escape ? CSS.escape(value) : String(value).replaceAll('"', '\\"');
}

function createIcon(name) {
  const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  svg.classList.add("mud-icon");
  svg.setAttribute("viewBox", "0 0 24 24");
  svg.setAttribute("fill", "none");
  svg.setAttribute("stroke", "currentColor");
  svg.setAttribute("stroke-width", "2");
  svg.setAttribute("stroke-linecap", "round");
  svg.setAttribute("stroke-linejoin", "round");
  svg.setAttribute("aria-hidden", "true");

  function addPath(d) {
    const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
    path.setAttribute("d", d);
    svg.append(path);
  }

  if (name === "x") {
    addPath("M18 6 6 18");
    addPath("m6 6 12 12");
    return svg;
  }

  const fallback = document.createElement("span");
  fallback.className = "mud-icon-fallback";
  return fallback;
}

function createIconLabel(icon, label) {
  const wrapper = document.createElement("span");
  wrapper.className = "mud-icon-label";
  wrapper.append(createIcon(icon));

  const text = document.createElement("span");
  text.textContent = label;
  wrapper.append(text);
  return wrapper;
}

function applySavedColumnWidth() {
  const saved = window.localStorage?.getItem(RIGHT_COLUMN_WIDTH_KEY);
  if (saved) {
    document.documentElement.style.setProperty("--mud-right-width", saved);
  }
}

function submitInspect(mount, entity) {
  const form = document.createElement("form");
  form.hidden = true;
  form.dataset.syncEvent = "submit";
  form.dataset.syncAction = "mud_inspect";

  const input = document.createElement("input");
  input.type = "hidden";
  input.name = "entity";
  input.value = entity;
  form.append(input);

  const button = document.createElement("button");
  button.type = "submit";
  form.append(button);

  mount.append(form);
  form.requestSubmit(button);
  setTimeout(() => form.remove(), 0);
}

function installHoldToExamine(mount) {
  let hold = null;
  let suppressClickButton = null;

  function clearHold() {
    if (hold?.timer) {
      clearTimeout(hold.timer);
    }
    hold = null;
  }

  mount.addEventListener("pointerdown", (event) => {
    const button = event.target?.closest?.("button[data-hold-entity]");
    if (!button || !mount.contains(button)) {
      return;
    }

    const entity = button.dataset.holdEntity;
    if (!entity) {
      return;
    }

    clearHold();
    hold = {
      button,
      pointerId: event.pointerId,
      fired: false,
      timer: setTimeout(() => {
        hold.fired = true;
        button.classList.add("hold-inspect-fired");
        submitInspect(mount, entity);
      }, HOLD_TO_EXAMINE_MS),
    };
  });

  mount.addEventListener("pointerup", (event) => {
    if (!hold || hold.pointerId !== event.pointerId) {
      return;
    }
    const fired = hold.fired;
    const button = hold.button;
    if (fired) {
      suppressClickButton = button;
      setTimeout(() => {
        if (suppressClickButton === button) {
          suppressClickButton = null;
        }
        button.classList.remove("hold-inspect-fired");
      }, 0);
    } else {
      button.classList.remove("hold-inspect-fired");
    }
    clearHold();
    if (fired) {
      event.preventDefault();
      event.stopPropagation();
    }
  });

  mount.addEventListener("pointercancel", clearHold);
  mount.addEventListener("pointerleave", clearHold);
  mount.addEventListener(
    "click",
    (event) => {
      const button = event.target?.closest?.("button[data-hold-entity]");
      if (button && (button === suppressClickButton || button.classList.contains("hold-inspect-fired"))) {
        event.preventDefault();
        event.stopPropagation();
        button.classList.remove("hold-inspect-fired");
        if (button === suppressClickButton) {
          suppressClickButton = null;
        }
      }
    },
    true,
  );
}

installHoldToExamine(document.getElementById("mount"));

function installColumnSplitter(mount) {
  let drag = null;

  mount.addEventListener("pointerdown", (event) => {
    const splitter = event.target?.closest?.(".column-splitter");
    if (!splitter || !mount.contains(splitter)) {
      return;
    }

    const columns = splitter.closest(".mud-columns");
    if (!columns) {
      return;
    }

    const right = columns.querySelector(".mud-right-column");
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
    document.documentElement.style.setProperty("--mud-right-width", value);
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

function closeDetails(details) {
  if (details) {
    details.open = false;
  }
}

function windowState(windowEl) {
  const rect = windowEl.getBoundingClientRect();
  return {
    left: Math.round(rect.left),
    top: Math.round(rect.top),
    width: Math.round(rect.width),
    height: Math.round(rect.height),
  };
}

function toolWindowStateKey(windowEl) {
  return `${TOOL_WINDOW_STATE_PREFIX}${windowEl.dataset.windowKey || "window"}`;
}

function saveToolWindowState(windowEl) {
  window.localStorage?.setItem(toolWindowStateKey(windowEl), JSON.stringify(windowState(windowEl)));
}

function applyToolWindowState(windowEl, state) {
  if (!state) {
    return;
  }
  const maxLeft = Math.max(12, window.innerWidth - MIN_WINDOW_WIDTH - 12);
  const maxTop = Math.max(12, window.innerHeight - MIN_WINDOW_HEIGHT - 12);
  const left = Math.min(Math.max(Number(state.left) || 18, 12), maxLeft);
  const top = Math.min(Math.max(Number(state.top) || 82, 12), maxTop);
  const width = Math.min(Math.max(Number(state.width) || 520, MIN_WINDOW_WIDTH), window.innerWidth - 24);
  const height = Math.min(Math.max(Number(state.height) || 360, MIN_WINDOW_HEIGHT), window.innerHeight - 24);
  windowEl.style.left = `${left}px`;
  windowEl.style.right = "auto";
  windowEl.style.top = `${top}px`;
  windowEl.style.width = `${width}px`;
  windowEl.style.height = `${height}px`;
}

function restoreToolWindowState(windowEl) {
  try {
    applyToolWindowState(windowEl, JSON.parse(window.localStorage?.getItem(toolWindowStateKey(windowEl)) ?? "null"));
  } catch {
    window.localStorage?.removeItem(toolWindowStateKey(windowEl));
  }
}

function createSourceWindow(mount, key, title, source) {
  const existing = mount.querySelector(`.source-window[data-window-key="${cssEscape(key)}"]`);
  if (existing) {
    existing.removeAttribute("hidden");
    existing.style.zIndex = "20";
    return existing;
  }

  const windowEl = document.createElement("div");
  windowEl.className = "tool-window source-window";
  windowEl.dataset.windowKey = key;

  const titlebar = document.createElement("div");
  titlebar.className = "window-titlebar";
  titlebar.dataset.windowDrag = key;

  const titleEl = document.createElement("strong");
  titleEl.textContent = title;
  titlebar.append(titleEl);

  const close = document.createElement("button");
  close.type = "button";
  close.className = "window-close";
  close.dataset.closeWindow = "true";
  close.setAttribute("aria-label", "Close source window");
  close.append(createIconLabel("x", "Close"));
  titlebar.append(close);

  const body = document.createElement("div");
  body.className = "window-body source-window-body";
  const pre = document.createElement("pre");
  pre.className = "mica-source mica-source-popout";
  pre.textContent = source;
  body.append(pre);

  const resize = document.createElement("div");
  resize.className = "window-resize-handle";
  resize.dataset.windowResize = key;
  resize.setAttribute("aria-hidden", "true");

  windowEl.append(titlebar, body, resize);
  mount.append(windowEl);
  restoreToolWindowState(windowEl);
  if (!windowEl.style.left) {
    windowEl.style.left = `${Math.max(16, window.innerWidth - 620)}px`;
    windowEl.style.top = "96px";
    windowEl.style.width = "600px";
    windowEl.style.height = "520px";
  }
  return windowEl;
}

function installToolWindows(mount) {
  let drag = null;

  mount.addEventListener("toggle", (event) => {
    const details = event.target?.closest?.("details.tool-sheet");
    if (!details || event.target !== details || !details.open) {
      return;
    }
    const windowEl = details.querySelector(".tool-window");
    if (windowEl) {
      restoreToolWindowState(windowEl);
    }
  }, true);

  mount.addEventListener("click", (event) => {
    const sourcePopout = event.target?.closest?.(".source-popout");
    if (sourcePopout && mount.contains(sourcePopout)) {
      const sourceHost = sourcePopout.closest(".source-host");
      const source = sourceHost?.querySelector(`.mica-source-full[data-source-key="${cssEscape(sourcePopout.dataset.sourceKey)}"]`);
      if (source) {
        createSourceWindow(mount, `source:${sourcePopout.dataset.sourceKey}`, sourcePopout.dataset.sourceTitle || "Method source", source.textContent || "");
      }
      event.preventDefault();
      event.stopPropagation();
      return;
    }

    const closeWindow = event.target?.closest?.("[data-close-window]");
    if (closeWindow && mount.contains(closeWindow)) {
      closeWindow.closest(".tool-window")?.remove();
      event.preventDefault();
      return;
    }

    const close = event.target?.closest?.("[data-close-details]");
    if (close && mount.contains(close)) {
      closeDetails(close.closest("details"));
      event.preventDefault();
    }
  });

  mount.addEventListener("pointerdown", (event) => {
    if (event.target?.closest?.("[data-close-details], [data-close-window]")) {
      return;
    }

    const dragHandle = event.target?.closest?.("[data-window-drag]");
    const resizeHandle = event.target?.closest?.("[data-window-resize]");
    const handle = resizeHandle || dragHandle;
    if (!handle || !mount.contains(handle)) {
      return;
    }

    const windowEl = handle.closest(".tool-window");
    if (!windowEl) {
      return;
    }

    const rect = windowEl.getBoundingClientRect();
    drag = {
      mode: resizeHandle ? "resize" : "move",
      windowEl,
      pointerId: event.pointerId,
      startX: event.clientX,
      startY: event.clientY,
      left: rect.left,
      top: rect.top,
      width: rect.width,
      height: rect.height,
    };
    windowEl.classList.add(resizeHandle ? "is-resizing" : "is-dragging");
    handle.setPointerCapture?.(event.pointerId);
    event.preventDefault();
  });

  mount.addEventListener("pointermove", (event) => {
    if (!drag || drag.pointerId !== event.pointerId) {
      return;
    }

    if (drag.mode === "move") {
      const width = drag.windowEl.offsetWidth;
      const height = drag.windowEl.offsetHeight;
      const left = Math.min(Math.max(drag.left + event.clientX - drag.startX, 8), window.innerWidth - width - 8);
      const top = Math.min(Math.max(drag.top + event.clientY - drag.startY, 8), window.innerHeight - height - 8);
      drag.windowEl.style.left = `${Math.round(left)}px`;
      drag.windowEl.style.right = "auto";
      drag.windowEl.style.top = `${Math.round(top)}px`;
    } else {
      const width = Math.min(Math.max(drag.width + event.clientX - drag.startX, MIN_WINDOW_WIDTH), window.innerWidth - drag.left - 8);
      const height = Math.min(Math.max(drag.height + event.clientY - drag.startY, MIN_WINDOW_HEIGHT), window.innerHeight - drag.top - 8);
      drag.windowEl.style.width = `${Math.round(width)}px`;
      drag.windowEl.style.height = `${Math.round(height)}px`;
    }
  });

  function finishWindowDrag(event) {
    if (!drag || drag.pointerId !== event.pointerId) {
      return;
    }
    drag.windowEl.classList.remove("is-dragging", "is-resizing");
    saveToolWindowState(drag.windowEl);
    event.target?.releasePointerCapture?.(event.pointerId);
    drag = null;
  }

  mount.addEventListener("pointerup", finishWindowDrag);
  mount.addEventListener("pointercancel", finishWindowDrag);

  window.addEventListener("keydown", (event) => {
    if (event.key !== "Escape") {
      return;
    }
    let closed = false;
    for (const windowEl of mount.querySelectorAll(".source-window")) {
      windowEl.remove();
      closed = true;
    }
    for (const details of mount.querySelectorAll("details.tool-sheet[open]")) {
      closeDetails(details);
      closed = true;
    }
    if (closed) {
      event.preventDefault();
    }
  });
}

function installCommandSuggestions(mount) {
  function commandInput() {
    return mount.querySelector("#command");
  }

  function suggestions() {
    return Array.from(mount.querySelectorAll("[data-command-suggestion]"));
  }

  function acceptSuggestion(button) {
    const input = commandInput();
    const command = button?.dataset?.commandSuggestion;
    if (!input || !command) {
      return false;
    }

    input.value = command;
    input.focus();
    input.setSelectionRange(command.length, command.length);
    input.dispatchEvent(
      new InputEvent("input", {
        bubbles: true,
        inputType: "insertReplacementText",
        data: command,
      }),
    );
    return true;
  }

  mount.addEventListener("keydown", (event) => {
    if (event.target !== commandInput()) {
      return;
    }

    const items = suggestions();
    if (items.length === 0) {
      return;
    }

    if (event.key === "Tab") {
      const accepted = acceptSuggestion(items[0]);
      if (accepted) {
        event.preventDefault();
      }
    }
  });
}

installToolWindows(document.getElementById("mount"));
installCommandSuggestions(document.getElementById("mount"));
