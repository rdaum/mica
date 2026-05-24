import { bootstrapServerRenderedSync } from "/sync-client.js?surface=source";

const SPLITTER_STORAGE = {
  "--source-sidebar-width": "mica.source.sidebarWidth",
  "--source-inspector-width": "mica.source.inspectorWidth",
  "--source-symbol-height": "mica.source.symbolHeight",
  "--source-outline-height": "mica.source.outlineHeight",
};

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function setStoredLayoutValue(name, value) {
  document.documentElement.style.setProperty(name, value);
  localStorage.setItem(SPLITTER_STORAGE[name], value);
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

function installSelectedLineFocus() {
  const mount = document.getElementById("mount");
  if (!mount) {
    return;
  }

  let selectedKey = "";
  const focusSelectedLine = () => {
    const selected = mount.querySelector(".source-code-line.selected");
    const nextKey = selected?.getAttribute("data-sync-key") ?? "";
    if (!selected || nextKey === selectedKey) {
      return;
    }
    selectedKey = nextKey;
    selected.scrollIntoView({ block: "center", inline: "nearest" });
  };

  focusSelectedLine();
  new MutationObserver(focusSelectedLine).observe(mount, {
    childList: true,
    subtree: true,
  });
}

restoreStoredLayout();
installSplitters();
installSelectedLineFocus();

window.micaSource = bootstrapServerRenderedSync(
  document.getElementById("mount"),
  document.getElementById("status"),
);
