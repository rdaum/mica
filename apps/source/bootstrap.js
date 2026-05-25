import { bootstrapServerRenderedSync } from "/sync-client.js?surface=source";

const SPLITTER_STORAGE = {
  "--source-sidebar-width": "mica.source.sidebarWidth",
  "--source-inspector-width": "mica.source.inspectorWidth",
  "--source-symbol-height": "mica.source.symbolHeight",
  "--source-outline-height": "mica.source.outlineHeight",
  "--source-agent-height": "mica.source.agentHeight",
};

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function setStoredLayoutValue(name, value) {
  document.documentElement.style.setProperty(name, value);
  localStorage.setItem(SPLITTER_STORAGE[name], value);
}

function expandAgentPanel() {
  const height = clamp(window.innerHeight * 0.48, 340, 680);
  setStoredLayoutValue("--source-agent-height", `${Math.round(height)}px`);
}

function restoreStoredLayout() {
  for (const [name, key] of Object.entries(SPLITTER_STORAGE)) {
    const value = localStorage.getItem(key);
    if (value) {
      document.documentElement.style.setProperty(name, value);
    }
  }
}

function splitterKind(splitter) {
  if (splitter.classList.contains("source-splitter-sidebar")) {
    return "sidebar";
  }
  if (splitter.classList.contains("source-splitter-workbench")) {
    return "workbench";
  }
  if (splitter.classList.contains("source-splitter-code-symbol")) {
    return "code-symbol";
  }
  if (splitter.classList.contains("source-splitter-outline-annotations")) {
    return "outline-annotations";
  }
  if (splitter.classList.contains("source-splitter-agent")) {
    return "agent";
  }
  return null;
}

function dragValue(kind, splitter, event) {
  if (kind === "sidebar") {
    const shell = splitter.closest(".source-shell");
    const rect = shell?.getBoundingClientRect();
    if (!rect) {
      return null;
    }
    const width = clamp(event.clientX - rect.left, 190, Math.min(460, rect.width - 520));
    return ["--source-sidebar-width", `${Math.round(width)}px`];
  }
  if (kind === "workbench") {
    const workbench = splitter.closest(".source-workbench");
    const rect = workbench?.getBoundingClientRect();
    if (!rect) {
      return null;
    }
    const width = clamp(rect.right - event.clientX, 260, Math.min(620, rect.width - 420));
    return ["--source-inspector-width", `${Math.round(width)}px`];
  }
  if (kind === "code-symbol") {
    const stack = splitter.closest(".source-code-stack");
    const rect = stack?.getBoundingClientRect();
    if (!rect) {
      return null;
    }
    const height = clamp(rect.bottom - event.clientY, 130, Math.min(520, rect.height - 180));
    return ["--source-symbol-height", `${Math.round(height)}px`];
  }
  if (kind === "outline-annotations") {
    const rail = splitter.closest(".source-inspector-rail");
    const rect = rail?.getBoundingClientRect();
    if (!rect) {
      return null;
    }
    const height = clamp(event.clientY - rect.top, 90, Math.min(560, rect.height - 120));
    return ["--source-outline-height", `${Math.round(height)}px`];
  }
  if (kind === "agent") {
    const panel = splitter.closest(".source-code-panel-body");
    const rect = panel?.getBoundingClientRect();
    if (!rect) {
      return null;
    }
    const height = clamp(rect.bottom - event.clientY, 170, Math.min(680, rect.height - 260));
    return ["--source-agent-height", `${Math.round(height)}px`];
  }
  return null;
}

function installSplitters() {
  document.addEventListener("pointerdown", (event) => {
    const splitter = event.target?.closest?.(".source-splitter");
    if (!splitter || event.button !== 0) {
      return;
    }

    const kind = splitterKind(splitter);
    if (!kind) {
      return;
    }

    event.preventDefault();
    splitter.setPointerCapture?.(event.pointerId);
    const horizontal = splitter.classList.contains("source-splitter-horizontal");
    document.body.classList.add(
      "source-splitter-dragging",
      horizontal ? "source-splitter-dragging-horizontal" : "source-splitter-dragging-vertical",
    );

    const onMove = (moveEvent) => {
      const value = dragValue(kind, splitter, moveEvent);
      if (value) {
        setStoredLayoutValue(value[0], value[1]);
      }
    };
    const onUp = () => {
      document.body.classList.remove(
        "source-splitter-dragging",
        "source-splitter-dragging-horizontal",
        "source-splitter-dragging-vertical",
      );
      document.removeEventListener("pointermove", onMove);
      document.removeEventListener("pointerup", onUp);
      document.removeEventListener("pointercancel", onUp);
    };

    onMove(event);
    document.addEventListener("pointermove", onMove);
    document.addEventListener("pointerup", onUp);
    document.addEventListener("pointercancel", onUp);
  });
}

function sourceCodeLineHeight(frame) {
  const row = frame.querySelector(".source-code-line");
  const height = row?.getBoundingClientRect?.().height;
  if (height && Number.isFinite(height)) {
    return height;
  }
  const cssValue = getComputedStyle(document.documentElement).getPropertyValue(
    "--source-code-row-height",
  );
  const parsed = Number.parseFloat(cssValue);
  return Number.isFinite(parsed) && parsed > 0 ? parsed * 16 : 22;
}

function updateSourceSpacers(frame) {
  const lineHeight = sourceCodeLineHeight(frame);
  for (const spacer of frame.querySelectorAll(".source-code-spacer")) {
    const lines = Number.parseInt(spacer.dataset.sourceSpacerLines ?? "0", 10);
    spacer.style.height = `${Math.max(0, lines) * lineHeight}px`;
  }
}

function installSourceViewport() {
  const mount = document.getElementById("mount");
  if (!mount) {
    return;
  }

  const preloadMargin = 160;
  const viewportOverscan = 80;
  let timer = null;
  let lastSubmitted = "";
  let pendingScrollTop = null;
  let queuedWindow = null;
  let syncBusy = false;
  let lastWindowRequest = null;
  let suppressScrollUntil = 0;
  let sourceWindowInFlight = false;
  let sourceWindowStallTimer = null;
  let sourceWindowTimeoutTimer = null;
  let selectedFocusKey = "";

  const resetManualWindowState = () => {
    window.clearTimeout(timer);
    timer = null;
    pendingScrollTop = null;
    queuedWindow = null;
    lastSubmitted = "";
    suppressScrollUntil = performance.now() + 300;
  };

  const updateSourceWindowLoadingText = (frame, message) => {
    const text = frame?.querySelector?.(".source-code-loading-text");
    if (text) {
      text.textContent = message;
    }
  };

  const clearSourceWindowTimers = () => {
    window.clearTimeout(sourceWindowStallTimer);
    window.clearTimeout(sourceWindowTimeoutTimer);
    sourceWindowStallTimer = null;
    sourceWindowTimeoutTimer = null;
  };

  const setSourceWindowLoading = (frame, loading, message = "Loading source window...") => {
    if (!frame) {
      return;
    }
    frame.classList.toggle("source-window-loading", loading);
    if (!loading) {
      frame.classList.remove("source-window-stalled");
      clearSourceWindowTimers();
      return;
    }

    frame.classList.remove("source-window-stalled");
    updateSourceWindowLoadingText(frame, message);
    clearSourceWindowTimers();
    sourceWindowStallTimer = window.setTimeout(() => {
      frame.classList.add("source-window-stalled");
      updateSourceWindowLoadingText(frame, "Still rendering this source window...");
    }, 2500);
    sourceWindowTimeoutTimer = window.setTimeout(() => {
      frame.classList.add("source-window-stalled");
      updateSourceWindowLoadingText(frame, "Source window is taking too long.");
    }, 12000);
  };

  const submitWindowRequest = (frame, windowStart, scrollTop) => {
    updateSourceSpacers(frame);
    const form = frame.querySelector("#source-window-form");
    const input = form?.querySelector("input[name='window_start']");
    if (!form || !input) {
      return;
    }

    const key = `${frame.dataset.sourcePath ?? ""}:${windowStart}`;
    lastSubmitted = key;
    lastWindowRequest = {
      path: frame.dataset.sourcePath ?? "",
      scrollTop,
      windowStart,
    };
    pendingScrollTop = scrollTop;
    setSourceWindowLoading(frame, true);
    input.value = String(windowStart);
    form.requestSubmit();
  };

  const flushQueuedWindow = () => {
    if (syncBusy || !queuedWindow) {
      return;
    }

    const frame = mount.querySelector(".source-code-frame");
    if (!frame || frame.dataset.sourcePath !== queuedWindow.path) {
      queuedWindow = null;
      return;
    }

    const request = queuedWindow;
    queuedWindow = null;
    submitWindowRequest(frame, request.windowStart, request.scrollTop);
  };

  const requestWindowStart = (frame, windowStart, force = false) => {
    const key = `${frame.dataset.sourcePath ?? ""}:${windowStart}`;
    if (!force && key === lastSubmitted) {
      return;
    }
    if (
      !force &&
      queuedWindow &&
      queuedWindow.path === (frame.dataset.sourcePath ?? "") &&
      queuedWindow.windowStart === windowStart
    ) {
      return;
    }

    queuedWindow = {
      path: frame.dataset.sourcePath ?? "",
      scrollTop: frame.scrollTop,
      windowStart,
    };
    setSourceWindowLoading(frame, true, syncBusy ? "Waiting for the current action..." : "Loading source window...");
    flushQueuedWindow();
  };

  const submitWindowStart = (frame) => {
    updateSourceSpacers(frame);
    const lineHeight = sourceCodeLineHeight(frame);
    const lineCount = Number.parseInt(frame.dataset.sourceLineCount ?? "1", 10);
    const windowSize = Number.parseInt(frame.dataset.sourceWindowSize ?? "240", 10);
    const currentStart = Number.parseInt(frame.dataset.sourceWindowStart ?? "1", 10);
    const currentEnd = currentStart + windowSize - 1;
    const firstVisible = Math.floor(frame.scrollTop / lineHeight) + 1;
    const lastVisible = Math.ceil((frame.scrollTop + frame.clientHeight) / lineHeight);
    const maxStart = Math.max(1, lineCount - windowSize + 1);
    let nextStart = currentStart;
    if (currentEnd < lineCount && lastVisible >= currentEnd - preloadMargin) {
      nextStart = clamp(firstVisible - viewportOverscan, 1, maxStart);
    } else if (currentStart > 1 && firstVisible <= currentStart + preloadMargin) {
      nextStart = clamp(lastVisible - windowSize + viewportOverscan, 1, maxStart);
    }
    if (nextStart === currentStart) {
      return;
    }

    requestWindowStart(frame, nextStart);
  };

  const focusSelectedLine = (frame) => {
    const selected = frame.querySelector(".source-code-line.selected");
    const nextKey = selected
      ? `${selected.getAttribute("data-sync-key") ?? ""}:${frame.dataset.sourcePath ?? ""}:${frame.dataset.sourceWindowStart ?? ""}`
      : "";
    if (!selected || nextKey === selectedFocusKey) {
      return;
    }

    selectedFocusKey = nextKey;
    suppressScrollUntil = performance.now() + 250;
    const targetTop = Math.max(0, selected.offsetTop - frame.clientHeight * 0.18);
    frame.scrollTop = targetTop;
  };

  const refreshFrame = () => {
    const frame = mount.querySelector(".source-code-frame");
    if (!frame) {
      return;
    }
    updateSourceSpacers(frame);
    if (pendingScrollTop !== null) {
      suppressScrollUntil = performance.now() + 250;
      frame.scrollTop = pendingScrollTop;
      pendingScrollTop = null;
    } else {
      focusSelectedLine(frame);
    }
    if (!syncBusy && !queuedWindow) {
      lastSubmitted = `${frame.dataset.sourcePath ?? ""}:${frame.dataset.sourceWindowStart ?? ""}`;
      setSourceWindowLoading(frame, false);
    }
  };

  mount.addEventListener("click", (event) => {
    const retry = event.target?.closest?.(".source-code-loading-retry");
    if (!retry || !mount.contains(retry) || !lastWindowRequest) {
      return;
    }
    const frame = retry.closest(".source-code-frame");
    if (!frame || frame.dataset.sourcePath !== lastWindowRequest.path) {
      return;
    }
    event.preventDefault();
    lastSubmitted = "";
    requestWindowStart(frame, lastWindowRequest.windowStart, true);
  });

  mount.addEventListener(
    "scroll",
    (event) => {
      const frame = event.target?.closest?.(".source-code-frame");
      if (!frame || !mount.contains(frame)) {
        return;
      }
      if (performance.now() < suppressScrollUntil) {
        return;
      }
      window.clearTimeout(timer);
      timer = window.setTimeout(() => submitWindowStart(frame), 80);
    },
    true,
  );

  window.addEventListener("mica:sync-loading-start", (event) => {
    syncBusy = true;
    const action = event.detail?.action;
    if (action === "source_set_window") {
      sourceWindowInFlight = true;
      setSourceWindowLoading(mount.querySelector(".source-code-frame"), true);
    } else if (action === "source_agent_prompt") {
      expandAgentPanel();
      document.body.classList.add("source-agent-working");
    } else if (
      action === "source_open_file" ||
      action === "source_jump_to_line" ||
      action === "source_select_symbol"
    ) {
      resetManualWindowState();
      setSourceWindowLoading(mount.querySelector(".source-code-frame"), false);
    }
  });
  window.addEventListener("mica:sync-loading-stop", (event) => {
    syncBusy = false;
    if (event.detail?.action === "source_set_window") {
      sourceWindowInFlight = false;
    } else if (event.detail?.action === "source_agent_prompt") {
      expandAgentPanel();
      document.body.classList.remove("source-agent-working");
    }
    window.setTimeout(() => {
      flushQueuedWindow();
      if (!queuedWindow && !sourceWindowInFlight) {
        setSourceWindowLoading(mount.querySelector(".source-code-frame"), false);
      }
      refreshFrame();
    }, 0);
  });

  refreshFrame();
  new MutationObserver(refreshFrame).observe(mount, {
    childList: true,
    subtree: true,
  });
}

restoreStoredLayout();
installSplitters();
installSourceViewport();

window.micaSource = bootstrapServerRenderedSync(
  document.getElementById("mount"),
  document.getElementById("status"),
);
