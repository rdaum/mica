import { bootstrapServerRenderedSync } from "/sync-client.js?surface=agent";
import DOMPurify from "https://esm.sh/dompurify@3.2.6";
import { marked } from "https://esm.sh/marked@13";

window.micaAgent = bootstrapServerRenderedSync(document.getElementById("mount"));

const RIGHT_COLUMN_WIDTH_KEY = "micaAgentRightColumnWidth";
const MIN_RIGHT_COLUMN_WIDTH = 280;
const MIN_LEFT_COLUMN_WIDTH = 420;
const MIN_WINDOW_WIDTH = 320;
const MIN_WINDOW_HEIGHT = 240;

function cssEscape(value) {
    return window.CSS?.escape ? CSS.escape(value) : String(value).replaceAll("\"", "\\\"");
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
        let modeInput = form.querySelector("input[name=\"mode\"]");
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
    let drag = null;

    mount.addEventListener("toggle", (event) => {
        const details = event.target?.closest?.("details.tool-sheet");
        if (!details || event.target !== details || !details.open) {
            return;
        }
        const windowEl = details.querySelector(".tool-window");
        if (windowEl) {
            const saved = window.localStorage?.getItem(toolWindowStateKey(windowEl));
            if (saved) {
                restoreToolWindowState(windowEl);
            } else {
                const summary = details.querySelector("summary");
                if (summary) {
                    const rect = summary.getBoundingClientRect();
                    const width = 520;
                    let left = rect.right - width;
                    if (left < 8) left = 8;
                    let top = rect.bottom + 4;
                    const height = 360;
                    if (top + height > window.innerHeight - 8) {
                        top = Math.max(8, rect.top - height - 4);
                    }
                    windowEl.style.left = `${Math.round(left)}px`;
                    windowEl.style.right = "auto";
                    windowEl.style.top = `${Math.round(top)}px`;
                }
            }
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
            const width = Math.min(
                Math.max(drag.width + event.clientX - drag.startX, MIN_WINDOW_WIDTH),
                window.innerWidth - drag.left - 8,
            );
            const height = Math.min(
                Math.max(drag.height + event.clientY - drag.startY, MIN_WINDOW_HEIGHT),
                window.innerHeight - drag.top - 8,
            );
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
        for (const details of mount.querySelectorAll("details.tool-sheet[open]")) {
            closeDetails(details);
            closed = true;
        }
        if (closed) {
            event.preventDefault();
        }
    });
}

function toolWindowStateKey(windowEl) {
    const key = windowEl.dataset.windowKey || windowEl.id || "default";
    return `micaAgentWindow:${key}`;
}

function windowState(windowEl) {
    const style = windowEl.style;
    return {
        left: style.left || "",
        top: style.top || "",
        width: style.width || "",
        height: style.height || "",
    };
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
        applyToolWindowState(
            windowEl,
            JSON.parse(window.localStorage?.getItem(toolWindowStateKey(windowEl)) ?? "null"),
        );
    } catch {
        window.localStorage?.removeItem(toolWindowStateKey(windowEl));
    }
}

installToolWindows(document.getElementById("mount"));
installAtCompletion(document.getElementById("mount"));
installMarkdownRendering(document.getElementById("mount"));
installAutoScroll(document.getElementById("mount"));

function linkifyMentions(html) {
    return html.replace(/(^|[\s(>])@(\.\/[^\s`"'*<>\[\]\(\)\)]+)/g, (_, pre, path) => {
        const entity = `#target/file<"${path}">`;
        return `${pre}<button type="button" class="mention-link" data-entity='${entity}'>@${path}</button>`;
    });
}

function renderMessageContent(el) {
    const raw = el.dataset.rawContent;
    if (raw === undefined) return;
    const html = marked.parse(raw);
    const linked = linkifyMentions(html);
    const safe = DOMPurify.sanitize(linked, {
        USE_PROFILES: { html: true },
        ADD_TAGS: ["button"],
        ADD_ATTR: ["data-entity", "class", "type"],
    });
    const shadow = el.shadowRoot || el.attachShadow({ mode: "open" });
    shadow.innerHTML = `<style>
    :host { all: inherit; }
    .markdown-body { line-height: 1.5; }
    .markdown-body > *:first-child { margin-top: 0; }
    .markdown-body > *:last-child { margin-bottom: 0; }
    .markdown-body h1, .markdown-body h2, .markdown-body h3, .markdown-body h4 { margin: 0.6em 0 0.3em; line-height: 1.3; }
    .markdown-body h1 { font-size: 1.3em; }
    .markdown-body h2 { font-size: 1.15em; }
    .markdown-body h3 { font-size: 1.05em; }
    .markdown-body p { margin: 0.4em 0; }
    .markdown-body ul, .markdown-body ol { margin: 0.4em 0; padding-left: 1.5em; }
    .markdown-body li { margin: 0.15em 0; }
    .markdown-body code { font-family: var(--mono-font, monospace); font-size: 0.88em; background: #1a2422; padding: 1px 4px; border-radius: 3px; }
    .markdown-body pre { margin: 0.6em 0; padding: 10px 14px; background: #0d1411; border: 1px solid #2a3a36; border-radius: 6px; overflow-x: auto; }
    .markdown-body pre code { background: none; padding: 0; font-size: 0.85em; border-radius: 0; }
    .markdown-body blockquote { margin: 0.4em 0; padding: 2px 12px; border-left: 3px solid #3a4540; color: #9ab0a8; }
    .markdown-body table { border-collapse: collapse; margin: 0.6em 0; font-size: 0.9em; }
    .markdown-body th, .markdown-body td { border: 1px solid #2a3a36; padding: 4px 8px; }
    .markdown-body th { background: #1a2422; }
    .markdown-body hr { border: none; border-top: 1px solid #2a3a36; margin: 0.8em 0; }
    .markdown-body a { color: #91d8c7; }
    .markdown-body strong { font-weight: 600; }
    .mention-link { display: inline; background: none; border: none; padding: 0; font-family: var(--mono-font, monospace); font-size: inherit; color: #91d8c7; cursor: pointer; text-decoration: none; }
    .mention-link:hover { text-decoration: underline; }
  </style><div class="markdown-body">${safe}</div>`;
    if (shadow._micaMentionHandlerInstalled) return;
    shadow._micaMentionHandlerInstalled = true;
    shadow.addEventListener("click", (event) => {
        const btn = event.target?.closest?.("button.mention-link");
        if (!btn) return;
        event.preventDefault();
        const form = document.createElement("form");
        form.dataset.syncEvent = "submit";
        form.dataset.syncAction = "agent_inspect";
        const hidden = document.createElement("input");
        hidden.type = "hidden";
        hidden.name = "entity";
        hidden.value = btn.dataset.entity;
        form.appendChild(hidden);
        mount.appendChild(form);
        form.requestSubmit();
        form.remove();
    });
}

function installMarkdownRendering(mount) {
    const pending = new Set();
    let frame = null;
    function scheduleRender(el) {
        pending.add(el);
        if (frame !== null) return;
        frame = requestAnimationFrame(() => {
            frame = null;
            for (const item of pending) {
                renderMessageContent(item);
            }
            pending.clear();
        });
    }
    for (const el of mount.querySelectorAll("[data-raw-content]")) {
        renderMessageContent(el);
    }
    const observer = new MutationObserver((mutations) => {
        for (const mutation of mutations) {
            if (
                mutation.type === "attributes"
                && mutation.target.dataset?.rawContent !== undefined
            ) {
                scheduleRender(mutation.target);
            }
            for (const node of mutation.addedNodes) {
                if (node.nodeType !== Node.ELEMENT_NODE) continue;
                if (node.dataset?.rawContent !== undefined) {
                    scheduleRender(node);
                }
                for (const el of node.querySelectorAll?.("[data-raw-content]") || []) {
                    scheduleRender(el);
                }
            }
        }
    });
    observer.observe(mount, {
        childList: true,
        subtree: true,
        attributes: true,
        attributeFilter: ["data-raw-content"],
    });
}

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
            dropdown.innerHTML = "<div class=\"at-completion-empty\">No files found</div>";
            return;
        }
        dropdown.innerHTML = items
            .map((item, i) => {
                const cls = i === selectedIndex ? "at-completion-item selected" : "at-completion-item";
                const icon = item.kind === "directory" ? "📁" : "";
                const lang = item.language ? `<span class="at-completion-lang">${item.language}</span>` : "";
                return `<div class="${cls}" role="option" data-index="${i}">`
                    + `<span class="at-completion-path">${icon}${item.path}</span>${lang}</div>`;
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
        const suffix = item.kind === "directory" ? "/" : " ";
        const insert = `${path}${suffix}`;
        input.value = before + insert + after;
        const cursorPos = before.length + insert.length;
        input.setSelectionRange(cursorPos, cursorPos);
        input.dispatchEvent(new Event("input", { bubbles: true }));
        input.focus();
    }

    mount.addEventListener("input", (event) => {
        const input = event.target;
        if (!input || input.name !== "text" || !input.closest("[data-sync-action=\"agent_command\"]")) {
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

function installAutoScroll(mount) {
    const tailKeys = new WeakMap();

    function scrollToBottom(el) {
        requestAnimationFrame(() => {
            el.scrollTop = el.scrollHeight;
            setTimeout(() => {
                el.scrollTop = el.scrollHeight;
            }, 0);
        });
    }

    function refreshTranscriptScroll() {
        for (const el of mount.querySelectorAll("[data-sync-follow='bottom']")) {
            const lastChild = el.lastElementChild;
            const rawContent = lastChild?.matches?.("[data-raw-content]")
                ? lastChild
                : lastChild?.querySelector?.("[data-raw-content]");
            const tailKey = lastChild
                ? `${lastChild.getAttribute("data-sync-key") ?? ""}:${rawContent?.dataset?.rawContent?.length ?? lastChild.textContent?.length ?? 0}`
                : "";
            const prevKey = tailKeys.get(el) ?? "";
            if (tailKey && tailKey !== prevKey) {
                const wasAtBottom = el.scrollHeight <= el.clientHeight
                    || el.scrollTop + el.clientHeight >= el.scrollHeight - 24;
                if (wasAtBottom) {
                    scrollToBottom(el);
                }
            }
            tailKeys.set(el, tailKey);
        }
    }

    refreshTranscriptScroll();
    new MutationObserver(() => {
        refreshTranscriptScroll();
    }).observe(mount, {
        childList: true,
        subtree: true,
        attributes: true,
        attributeFilter: ["data-raw-content"],
    });
    window.addEventListener("mica:sync-applied", () => {
        requestAnimationFrame(refreshTranscriptScroll);
    });
}
