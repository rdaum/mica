import { bootstrapServerRenderedSync } from "/sync-client.js?surface=mud";
window.micaMud = bootstrapServerRenderedSync(document.getElementById("mount"));

const HOLD_TO_EXAMINE_MS = 520;
const RIGHT_COLUMN_WIDTH_KEY = "micaMudRightColumnWidth";
const BUILD_WINDOW_STATE_KEY = "micaMudBuildWindow";
const MIN_RIGHT_COLUMN_WIDTH = 280;
const MIN_LEFT_COLUMN_WIDTH = 420;
const MIN_WINDOW_WIDTH = 320;
const MIN_WINDOW_HEIGHT = 240;

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

function saveBuildWindowState(windowEl) {
  window.localStorage?.setItem(BUILD_WINDOW_STATE_KEY, JSON.stringify(windowState(windowEl)));
}

function applyBuildWindowState(windowEl, state) {
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

function restoreBuildWindowState(windowEl) {
  try {
    applyBuildWindowState(windowEl, JSON.parse(window.localStorage?.getItem(BUILD_WINDOW_STATE_KEY) ?? "null"));
  } catch {
    window.localStorage?.removeItem(BUILD_WINDOW_STATE_KEY);
  }
}

function installToolWindows(mount) {
  let drag = null;

  mount.addEventListener("toggle", (event) => {
    const details = event.target?.closest?.("details.tool-sheet");
    if (!details || event.target !== details || !details.open) {
      return;
    }
    const windowEl = details.querySelector(".builder-window");
    if (windowEl) {
      restoreBuildWindowState(windowEl);
    }
  }, true);

  mount.addEventListener("click", (event) => {
    const close = event.target?.closest?.("[data-close-details]");
    if (close && mount.contains(close)) {
      closeDetails(close.closest("details"));
      event.preventDefault();
    }
  });

  mount.addEventListener("pointerdown", (event) => {
    if (event.target?.closest?.("[data-close-details]")) {
      return;
    }

    const dragHandle = event.target?.closest?.("[data-window-drag]");
    const resizeHandle = event.target?.closest?.("[data-window-resize]");
    const handle = resizeHandle || dragHandle;
    if (!handle || !mount.contains(handle)) {
      return;
    }

    const windowEl = handle.closest(".builder-window");
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
    saveBuildWindowState(drag.windowEl);
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
