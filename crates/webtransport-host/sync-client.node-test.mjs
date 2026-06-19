import assert from "node:assert/strict";
import {
  SyncKind,
  MicaSseSyncClient,
  MicaWebTransportSyncClient,
  decodeChunkedSyncEnvelope,
  decodeSyncEnvelope,
  encodeSyncEnvelope,
  applyDelta,
  applySnapshot,
  beginEventLoading,
  beginSubmitLoading,
  boundEventFields,
  endEventLoading,
  endSubmitLoading,
  clearCommandInputAfterSubmit,
  focusAfterSubmit,
  submitKeyMatches,
} from "./sync-client.js";

const encoded = encodeSyncEnvelope({
  kind: SyncKind.ViewDelta,
  session: 7n,
  view: 21n,
  clientRevision: 1n,
  clientSignature: 2n,
  serverRevision: 3n,
  serverSignature: 4n,
  payload: JSON.stringify({ type: "dom_patch", patches: [] }),
});

assert.deepEqual(decodeSyncEnvelope(encoded), {
  kind: "ViewDelta",
  session: "7",
  view: "21",
  clientRevision: "1",
  clientSignature: "2",
  serverRevision: "3",
  serverSignature: "4",
  payload: "{\"type\":\"dom_patch\",\"patches\":[]}",
});

const chunks = new Map();
const first = new Uint8Array(24 + 16);
const second = new Uint8Array(24 + encoded.length - 16);
for (const data of [first, second]) {
  data.set([0x4d, 0x53, 0x43, 0x31], 0);
}
new DataView(first.buffer).setUint32(4, 44, true);
new DataView(first.buffer).setUint32(8, 0, true);
new DataView(first.buffer).setUint32(12, 2, true);
new DataView(first.buffer).setUint32(16, encoded.length, true);
new DataView(first.buffer).setUint32(20, 16, true);
first.set(encoded.slice(0, 16), 24);
new DataView(second.buffer).setUint32(4, 44, true);
new DataView(second.buffer).setUint32(8, 1, true);
new DataView(second.buffer).setUint32(12, 2, true);
new DataView(second.buffer).setUint32(16, encoded.length, true);
new DataView(second.buffer).setUint32(20, encoded.length - 16, true);
second.set(encoded.slice(16), 24);
assert.equal(decodeChunkedSyncEnvelope(first, chunks), null);
assert.equal(decodeChunkedSyncEnvelope(second, chunks).kind, "ViewDelta");
assert.equal(chunks.size, 0);

const streamWrites = [];
class FakeWebTransport {
  constructor() {
    this.ready = Promise.resolve();
    this.closed = new Promise(() => {});
    this.datagrams = {
      incomingHighWaterMark: 1,
      outgoingHighWaterMark: 1,
      writable: {
        getWriter() {
          return {
            async write() {
              throw new Error("sync envelopes should use streams");
            },
          };
        },
      },
      readable: {
        getReader() {
          return { read: () => new Promise(() => {}) };
        },
      },
    };
    this.incomingUnidirectionalStreams = {
      getReader() {
        return { read: () => new Promise(() => {}) };
      },
    };
  }

  async createUnidirectionalStream() {
    return {
      getWriter() {
        return {
          async write(value) {
            streamWrites.push(value);
          },
          async close() {
            streamWrites.push("closed");
          },
        };
      },
    };
  }
}

globalThis.WebTransport = FakeWebTransport;
const client = new MicaWebTransportSyncClient({
  url: "https://127.0.0.1:4433/view",
  certificateHash: "",
});
await client.connect();
await client.needView({
  session: 42n,
  view: 21n,
  clientRevision: 3n,
  clientSignature: 5n,
  payload: "test-need",
});
assert.equal(streamWrites.length, 2);
assert.equal(decodeSyncEnvelope(streamWrites[0]).kind, "NeedView");
assert.equal(decodeSyncEnvelope(streamWrites[0]).payload, "test-need");
assert.equal(streamWrites[1], "closed");

const sseWrites = [];
globalThis.EventSource = class FakeEventSource {
  static CLOSED = 2;

  constructor(url) {
    this.url = url;
    this.readyState = 1;
    this.listeners = new Map();
    queueMicrotask(() => {
      this.listeners.get("open")?.({ type: "open" });
    });
  }

  addEventListener(name, handler) {
    this.listeners.set(name, handler);
  }

  close() {
    this.readyState = FakeEventSource.CLOSED;
  }
};
globalThis.fetch = async (_url, options) => {
  const body =
    options.body instanceof Uint8Array
      ? options.body
      : new Uint8Array(options.body);
  sseWrites.push(decodeSyncEnvelope(body));
  return { ok: true, status: 202, statusText: "Accepted" };
};
const sseClient = new MicaSseSyncClient({
  streamUrl: "http://127.0.0.1:8080/sync/events?session=42",
  sendUrl: "http://127.0.0.1:8080/sync/input",
});
await sseClient.connect();
await sseClient.needView({
  session: 42n,
  view: 21n,
  clientRevision: 3n,
  clientSignature: 5n,
  payload: "sse-need",
});
assert.equal(sseWrites.length, 1);
assert.equal(sseWrites[0].kind, "NeedView");
assert.equal(sseWrites[0].payload, "sse-need");

let commandFocused = false;
let localFocused = false;
let fallbackFocused = false;
globalThis.document = {
  getElementById(id) {
    return id === "command" ? { focus: () => (commandFocused = true) } : null;
  },
  querySelector() {
    return { focus: () => (fallbackFocused = true) };
  },
  createElement(tag) {
    return {
      localName: tag,
      attributes: new Map(),
      textContent: "",
      setAttribute(name, value) {
        this.attributes.set(name, String(value));
      },
      remove() {
        const parent = this.parentNode;
        const index = parent?.children?.indexOf(this) ?? -1;
        if (index >= 0) {
          parent.children.splice(index, 1);
        }
      },
    };
  },
};
focusAfterSubmit({ querySelector: () => ({ focus: () => (localFocused = true) }) });
assert.equal(commandFocused, true);
assert.equal(localFocused, false);
assert.equal(fallbackFocused, false);

commandFocused = false;
globalThis.document.getElementById = () => null;
focusAfterSubmit({ querySelector: () => ({ focus: () => (localFocused = true) }) });
assert.equal(localFocused, true);
assert.equal(fallbackFocused, false);

localFocused = false;
focusAfterSubmit({ querySelector: () => null });
assert.equal(fallbackFocused, true);

const commandInput = {
  value: "look",
  defaultValue: "look",
};
globalThis.document.getElementById = (id) => (id === "command" ? commandInput : null);
clearCommandInputAfterSubmit({ querySelector: () => null });
assert.equal(commandInput.value, "");
assert.equal(commandInput.defaultValue, "");

assert.equal(
  submitKeyMatches({ key: "Enter", ctrlKey: true }, "ctrl+enter"),
  true,
);
assert.equal(
  submitKeyMatches({ key: "Enter", metaKey: true }, "ctrl+enter"),
  false,
);
assert.equal(
  submitKeyMatches({ key: "Enter", ctrlKey: true, shiftKey: true }, "ctrl+enter"),
  false,
);
assert.equal(
  submitKeyMatches({ key: "Enter", metaKey: true }, "cmd+enter"),
  true,
);

const loadingForm = {
  className: "",
  attributes: new Map(),
  elements: [],
  setAttribute(name, value) {
    this.attributes.set(name, String(value));
  },
  removeAttribute(name) {
    this.attributes.delete(name);
  },
};
const loadingInput = {
  localName: "input",
  type: "text",
  readOnly: false,
};
const loadingButton = {
  localName: "button",
  type: "submit",
  disabled: false,
  className: "",
  textContent: "Find references",
  children: [],
  attributes: new Map([["data-sync-disable-with", "Finding..."]]),
  append(child) {
    child.parentNode = this;
    this.children.push(child);
  },
  getAttribute(name) {
    return this.attributes.get(name) ?? null;
  },
  setAttribute(name, value) {
    this.attributes.set(name, String(value));
  },
  removeAttribute(name) {
    this.attributes.delete(name);
  },
};
loadingForm.elements = [loadingInput, loadingButton];
const loadingToken = beginSubmitLoading(loadingForm, loadingButton);
assert.match(loadingForm.className, /sync-submit-loading/);
assert.match(loadingButton.className, /sync-submit-loading/);
assert.equal(loadingForm.attributes.get("aria-busy"), "true");
assert.equal(loadingButton.attributes.get("aria-busy"), "true");
assert.equal(loadingInput.readOnly, true);
assert.equal(loadingButton.disabled, true);
assert.equal(loadingButton.textContent, "Find references");
assert.equal(loadingButton.children.length, 1);
assert.equal(loadingButton.children[0].textContent, "Finding...");
endSubmitLoading(loadingToken);
assert.equal(loadingForm.className, "");
assert.equal(loadingButton.className, "");
assert.equal(loadingForm.attributes.has("aria-busy"), false);
assert.equal(loadingButton.attributes.has("aria-busy"), false);
assert.equal(loadingInput.readOnly, false);
assert.equal(loadingButton.disabled, false);
assert.equal(loadingButton.textContent, "Find references");
assert.equal(loadingButton.children.length, 0);

const clickButton = {
  localName: "button",
  type: "button",
  name: "mode",
  value: "references",
  disabled: false,
  className: "",
  textContent: "References",
  children: [],
  attributes: new Map([
    ["data-sync-disable-with", "Loading..."],
    ["data-sync-value-symbol", "RuleDefinition"],
    ["data-sync-value-source-path", "crates/relation-kernel/src/rules.rs"],
  ]),
  getAttribute(name) {
    return this.attributes.get(name) ?? null;
  },
  append(child) {
    child.parentNode = this;
    this.children.push(child);
  },
  getAttributeNames() {
    return Array.from(this.attributes.keys());
  },
  setAttribute(name, value) {
    this.attributes.set(name, String(value));
  },
  removeAttribute(name) {
    this.attributes.delete(name);
  },
};
assert.deepEqual(boundEventFields(clickButton), {
  mode: "references",
  symbol: "RuleDefinition",
  source_path: "crates/relation-kernel/src/rules.rs",
});
const clickToken = beginEventLoading(clickButton);
assert.match(clickButton.className, /sync-loading/);
assert.equal(clickButton.attributes.get("aria-busy"), "true");
assert.equal(clickButton.disabled, true);
assert.equal(clickButton.textContent, "References");
assert.equal(clickButton.children.length, 1);
assert.equal(clickButton.children[0].textContent, "Loading...");
endEventLoading(clickToken);
assert.equal(clickButton.className, "");
assert.equal(clickButton.attributes.has("aria-busy"), false);
assert.equal(clickButton.disabled, false);
assert.equal(clickButton.textContent, "References");
assert.equal(clickButton.children.length, 0);

const passiveInput = {
  className: "",
  readOnly: false,
  type: "text",
  attributes: new Map(),
  getAttribute(name) {
    return this.attributes.get(name) ?? null;
  },
  setAttribute(name, value) {
    this.attributes.set(name, String(value));
  },
  removeAttribute(name) {
    this.attributes.delete(name);
  },
};
const passiveToken = beginEventLoading(passiveInput, { passive: true });
assert.equal(passiveInput.className, "");
assert.equal(passiveInput.readOnly, false);
assert.equal(passiveInput.attributes.has("aria-busy"), false);
endEventLoading(passiveToken);
assert.equal(passiveInput.readOnly, false);

class FakeText {
  nodeType = Node.TEXT_NODE;
  parentNode = null;

  constructor(text) {
    this.nodeValue = text;
  }
}

class FakeElement {
  nodeType = Node.ELEMENT_NODE;
  parentNode = null;
  childNodes = [];
  attributes = new Map();
  id = "";
  className = "";
  value = "";
  scrollTop = 0;
  scrollHeight = 0;
  clientHeight = 0;
  ownerDocument = null;

  constructor(tag) {
    this.localName = tag;
    this.namespaceURI = null;
  }

  get firstChild() {
    return this.childNodes[0] ?? null;
  }

  append(child) {
    child.parentNode = this;
    this.childNodes.push(child);
    for (let node = this; node; node = node.parentNode) {
      node.scrollHeight += 30;
    }
  }

  insertBefore(child, before) {
    child.parentNode = this;
    const index = before ? this.childNodes.indexOf(before) : -1;
    if (index < 0) {
      this.childNodes.push(child);
    } else {
      this.childNodes.splice(index, 0, child);
    }
  }

  remove() {
    const siblings = this.parentNode?.childNodes;
    const index = siblings?.indexOf(this) ?? -1;
    if (index >= 0) {
      siblings.splice(index, 1);
    }
    this.parentNode = null;
  }

  replaceWith(next) {
    const siblings = this.parentNode?.childNodes;
    const index = siblings?.indexOf(this) ?? -1;
    if (index >= 0) {
      next.parentNode = this.parentNode;
      siblings[index] = next;
    }
    this.parentNode = null;
  }

  replaceChildren(...children) {
    this.childNodes = [];
    for (const child of children) {
      this.append(child);
    }
  }

  contains(target) {
    if (target === this) {
      return true;
    }
    return this.childNodes.some((child) => child.contains?.(target));
  }

  querySelectorAll(selector) {
    const matches = [];
    const visit = (node) => {
      if (
        selector === '[data-sync-follow="bottom"]' &&
        node.getAttribute?.("data-sync-follow") === "bottom"
      ) {
        matches.push(node);
      }
      for (const child of node.childNodes ?? []) {
        visit(child);
      }
    };
    visit(this);
    return matches;
  }

  setAttribute(name, value) {
    const text = String(value);
    this.attributes.set(name, text);
    if (name === "id") {
      this.id = text;
    } else if (name === "class") {
      this.className = text;
    }
  }

  getAttribute(name) {
    return this.attributes.get(name) ?? null;
  }

  hasAttribute(name) {
    return this.attributes.has(name);
  }

  removeAttribute(name) {
    this.attributes.delete(name);
    if (name === "id") {
      this.id = "";
    } else if (name === "class") {
      this.className = "";
    }
  }
}

globalThis.Node = { ELEMENT_NODE: 1, TEXT_NODE: 3 };
const elementsById = new Map();
globalThis.document = {
  createElement(tag) {
    const element = new FakeElement(tag);
    element.ownerDocument = this;
    return element;
  },
  createElementNS(namespace, tag) {
    const element = new FakeElement(tag);
    element.namespaceURI = namespace;
    element.ownerDocument = this;
    return element;
  },
  createTextNode(text) {
    return new FakeText(text);
  },
  getElementById(id) {
    return elementsById.get(id) ?? null;
  },
  activeElement: null,
};

const svgMount = new FakeElement("div");
applySnapshot(svgMount, {
  root: {
    tag: "button",
    attrs: {},
    children: [
      {
        tag: "svg",
        attrs: {
          class: "source-icon",
          viewBox: "0 0 24 24",
          fill: "none",
          stroke: "currentColor",
          "stroke-width": "2",
        },
        children: [{ tag: "path", attrs: { d: "m9 17-5-5 5-5" }, children: [] }],
      },
    ],
  },
});
const renderedSvg = svgMount.firstChild.childNodes[0];
assert.equal(renderedSvg.namespaceURI, "http://www.w3.org/2000/svg");
assert.equal(renderedSvg.localName, "svg");
assert.equal(renderedSvg.childNodes[0].namespaceURI, "http://www.w3.org/2000/svg");
assert.equal(renderedSvg.getAttribute("viewBox"), "0 0 24 24");

const inputMount = new FakeElement("div");
const inputRoot = new FakeElement("main");
const liveInput = new FakeElement("input");
inputMount.append(inputRoot);
inputRoot.append(liveInput);
liveInput.ownerDocument = globalThis.document;
liveInput.setAttribute("data-sync-event", "input");
liveInput.setAttribute("value", "server");
liveInput.value = "server plus local typing";
globalThis.document.activeElement = liveInput;
applyDelta(inputMount, {
  type: "dom_patch",
  patches: [
    {
      op: "set_attr",
      path: [0],
      name: "value",
      value: "server",
    },
  ],
});
assert.equal(liveInput.getAttribute("value"), "server");
assert.equal(liveInput.value, "server plus local typing");

applyDelta(inputMount, {
  type: "dom_patch",
  patches: [
    {
      op: "remove_attr",
      path: [0],
      name: "value",
    },
  ],
});
assert.equal(liveInput.hasAttribute("value"), false);
assert.equal(liveInput.value, "server plus local typing");
globalThis.document.activeElement = null;

const mount = new FakeElement("div");
const root = new FakeElement("main");
const narrative = new FakeElement("section");
narrative.setAttribute("id", "narrative");
narrative.setAttribute("data-sync-follow", "bottom");
narrative.clientHeight = 100;
narrative.scrollHeight = 120;
narrative.scrollTop = 20;
elementsById.set("narrative", narrative);
const list = new FakeElement("ul");
mount.append(root);
root.append(narrative);
narrative.append(list);
narrative.scrollHeight = 120;
root.scrollHeight = 120;
mount.scrollHeight = 120;
applyDelta(mount, {
  type: "dom_patch",
  patches: [
    {
      op: "append_child",
      path: [0, 0],
      node: { tag: "li", children: [{ text: "server event" }] },
    },
  ],
});
assert.equal(narrative.scrollTop, narrative.scrollHeight);

narrative.scrollTop = 10;
const previousScrollTop = narrative.scrollTop;
applyDelta(mount, {
  type: "dom_patch",
  patches: [
    {
      op: "append_child",
      path: [0, 0],
      node: { tag: "li", children: [{ text: "later event" }] },
    },
  ],
});
assert.equal(narrative.scrollTop, previousScrollTop);
