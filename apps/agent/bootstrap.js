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