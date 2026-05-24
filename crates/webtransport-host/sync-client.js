const MAGIC = [0x4d, 0x53, 0x59, 0x31];
const HEADER_LEN = 56;
const CHUNK_MAGIC = [0x4d, 0x53, 0x43, 0x31];
const CHUNK_HEADER_LEN = 24;

export const SyncKind = Object.freeze({
  HaveView: 1,
  NeedView: 2,
  ViewSnapshot: 3,
  ViewDelta: 4,
});

const KIND_NAMES = new Map([
  [SyncKind.HaveView, "HaveView"],
  [SyncKind.NeedView, "NeedView"],
  [SyncKind.ViewSnapshot, "ViewSnapshot"],
  [SyncKind.ViewDelta, "ViewDelta"],
]);
const SUPPORTED_TAGS = new Set([
  "a",
  "abbr",
  "address",
  "area",
  "article",
  "aside",
  "audio",
  "b",
  "bdi",
  "bdo",
  "blockquote",
  "br",
  "button",
  "canvas",
  "caption",
  "cite",
  "code",
  "col",
  "colgroup",
  "data",
  "datalist",
  "dd",
  "del",
  "details",
  "dfn",
  "dialog",
  "div",
  "dl",
  "dt",
  "em",
  "fieldset",
  "figcaption",
  "figure",
  "footer",
  "form",
  "h1",
  "h2",
  "h3",
  "h4",
  "h5",
  "h6",
  "header",
  "hr",
  "i",
  "img",
  "input",
  "ins",
  "kbd",
  "label",
  "legend",
  "li",
  "main",
  "map",
  "mark",
  "menu",
  "meter",
  "nav",
  "ol",
  "optgroup",
  "option",
  "output",
  "p",
  "picture",
  "pre",
  "progress",
  "q",
  "rp",
  "rt",
  "ruby",
  "s",
  "samp",
  "section",
  "select",
  "small",
  "source",
  "span",
  "strong",
  "sub",
  "summary",
  "sup",
  "table",
  "tbody",
  "td",
  "template",
  "textarea",
  "tfoot",
  "th",
  "thead",
  "time",
  "tr",
  "track",
  "u",
  "ul",
  "var",
  "video",
  "wbr",
]);
const SUPPORTED_ATTRIBUTES = new Set([
  "accept",
  "accept-charset",
  "action",
  "alt",
  "aria-busy",
  "aria-checked",
  "aria-controls",
  "aria-current",
  "aria-describedby",
  "aria-disabled",
  "aria-expanded",
  "aria-hidden",
  "aria-label",
  "aria-labelledby",
  "aria-live",
  "aria-pressed",
  "aria-selected",
  "autocomplete",
  "autofocus",
  "checked",
  "class",
  "cols",
  "colspan",
  "data-command",
  "data-entity",
  "data-sync-action",
  "data-sync-debounce",
  "data-sync-disable-with",
  "data-sync-event",
  "data-sync-follow",
  "data-sync-key",
  "data-sync-throttle",
  "data-sync-on-viewport-top",
  "data-sync-stable-top",
  "data-sync-viewport-threshold",
  "datetime",
  "disabled",
  "download",
  "draggable",
  "for",
  "height",
  "hidden",
  "href",
  "id",
  "lang",
  "list",
  "loading",
  "max",
  "maxlength",
  "method",
  "min",
  "minlength",
  "multiple",
  "name",
  "open",
  "pattern",
  "placeholder",
  "readonly",
  "rel",
  "required",
  "role",
  "rows",
  "rowspan",
  "selected",
  "size",
  "span",
  "src",
  "step",
  "tabindex",
  "target",
  "title",
  "type",
  "value",
  "width",
  "wrap",
]);
const SYNC_LOADING_CLASS = "sync-loading";
const SYNC_SUBMIT_LOADING_CLASS = "sync-submit-loading";
const FNV_OFFSET = 0xcbf29ce484222325n;
const FNV_PRIME = 0x100000001b3n;
const SIGNATURE_MASK = 0x007fffffffffffffn;

function writeU64(view, offset, value) {
  view.setBigUint64(offset, BigInt(value), true);
}

function readU64(view, offset) {
  return view.getBigUint64(offset, true).toString();
}

function readU32(view, offset) {
  return view.getUint32(offset, true);
}

function hasMagic(data, magic) {
  return data.length >= magic.length && magic.every((byte, index) => data[index] === byte);
}

function payloadBytes(payload) {
  if (payload instanceof Uint8Array) {
    return payload;
  }
  return new TextEncoder().encode(String(payload ?? ""));
}

export function encodeSyncEnvelope(envelope) {
  const payload = payloadBytes(envelope.payload);
  const bytes = new Uint8Array(HEADER_LEN + payload.length);
  bytes.set(MAGIC, 0);
  bytes[4] = envelope.kind;
  const view = new DataView(bytes.buffer);
  writeU64(view, 8, envelope.session);
  writeU64(view, 16, envelope.view);
  writeU64(view, 24, envelope.clientRevision ?? 0n);
  writeU64(view, 32, envelope.clientSignature ?? 0n);
  writeU64(view, 40, envelope.serverRevision ?? 0n);
  writeU64(view, 48, envelope.serverSignature ?? 0n);
  bytes.set(payload, HEADER_LEN);
  return bytes;
}

export function decodeSyncEnvelope(bytes) {
  const data = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  if (data.length < HEADER_LEN || !hasMagic(data, MAGIC)) {
    return { raw: Array.from(data) };
  }
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  return {
    kind: KIND_NAMES.get(data[4]) || `0x${data[4].toString(16)}`,
    session: readU64(view, 8),
    view: readU64(view, 16),
    clientRevision: readU64(view, 24),
    clientSignature: readU64(view, 32),
    serverRevision: readU64(view, 40),
    serverSignature: readU64(view, 48),
    payload: new TextDecoder().decode(data.slice(HEADER_LEN)),
  };
}

export function decodeChunkedSyncEnvelope(bytes, chunks) {
  const data = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  if (!hasMagic(data, CHUNK_MAGIC)) {
    return decodeSyncEnvelope(data);
  }
  if (data.length < CHUNK_HEADER_LEN) {
    return null;
  }

  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const message = readU32(view, 4);
  const index = readU32(view, 8);
  const count = readU32(view, 12);
  const totalLen = readU32(view, 16);
  const chunkLen = readU32(view, 20);
  if (
    count === 0 ||
    index >= count ||
    chunkLen > data.length - CHUNK_HEADER_LEN ||
    totalLen === 0
  ) {
    return null;
  }

  const entry = chunks.get(message) ?? {
    count,
    totalLen,
    received: 0,
    parts: new Array(count),
  };
  if (entry.count !== count || entry.totalLen !== totalLen) {
    chunks.delete(message);
    return null;
  }
  if (!entry.parts[index]) {
    entry.parts[index] = data.slice(CHUNK_HEADER_LEN, CHUNK_HEADER_LEN + chunkLen);
    entry.received += 1;
  }
  chunks.set(message, entry);

  if (entry.received !== count) {
    return null;
  }
  chunks.delete(message);
  const combined = new Uint8Array(totalLen);
  let offset = 0;
  for (const part of entry.parts) {
    if (!part || offset + part.length > combined.length) {
      return null;
    }
    combined.set(part, offset);
    offset += part.length;
  }
  if (offset !== totalLen) {
    return null;
  }
  return decodeSyncEnvelope(combined);
}

export function certificateHashOptions(hex) {
  const clean = String(hex ?? "").replace(/[^0-9a-f]/gi, "");
  if (clean.length === 0) {
    return undefined;
  }
  if (clean.length !== 64) {
    throw new Error("certificate hash must be 32 bytes of hex");
  }
  const value = new Uint8Array(32);
  for (let index = 0; index < value.length; index += 1) {
    value[index] = Number.parseInt(clean.slice(index * 2, index * 2 + 2), 16);
  }
  return {
    serverCertificateHashes: [
      {
        algorithm: "sha-256",
        value,
      },
    ],
  };
}

export function validateSnapshotEnvelope(envelope) {
  const payload = JSON.parse(envelope.payload);
  const expectedSignature = syncPayloadSignature(
    envelope.serverRevision,
    envelope.payload,
  );
  return {
    payload,
    valid:
      String(payload.view) === envelope.view &&
      String(payload.revision) === envelope.serverRevision &&
      expectedSignature.toString() === envelope.serverSignature,
  };
}

export function validateDeltaEnvelope(envelope) {
  const payload = JSON.parse(envelope.payload);
  // Delta signatures name the post-apply rendered state, not the patch bytes.
  return {
    payload,
    valid:
      payload.type === "dom_patch" &&
      Array.isArray(payload.patches) &&
      String(payload.view) === envelope.view &&
      String(payload.revision) === envelope.serverRevision &&
      BigInt(envelope.serverSignature) > 0n,
  };
}

function syncPayloadSignature(revision, payload) {
  let hash = FNV_OFFSET;
  let value = BigInt(revision);
  for (let index = 0; index < 8; index += 1) {
    hash ^= value & 0xffn;
    hash = BigInt.asUintN(64, hash * FNV_PRIME);
    value >>= 8n;
  }
  for (const byte of new TextEncoder().encode(payload)) {
    hash ^= BigInt(byte);
    hash = BigInt.asUintN(64, hash * FNV_PRIME);
  }
  return hash & SIGNATURE_MASK;
}

export function applySnapshot(mount, payload) {
  const follow = captureFollowBottomTargets(mount);
  const stable = captureStableTopTargets(mount);
  reconcileChildren(mount, [payload.root]);
  restoreFollowBottomTargets(mount, follow);
  restoreStableTopTargets(mount, stable);
}

export function applyDelta(mount, payload) {
  if (payload.type !== "dom_patch") {
    throw new Error(`unsupported delta type: ${payload.type}`);
  }
  const follow = captureFollowBottomTargets(mount);
  const stable = captureStableTopTargets(mount);
  for (const patch of payload.patches) {
    applyPatch(mount, patch);
  }
  restoreFollowBottomTargets(mount, follow);
  restoreStableTopTargets(mount, stable);
}

function captureFollowBottomTargets(mount) {
  if (!mount?.querySelectorAll) {
    return [];
  }
  return Array.from(mount.querySelectorAll('[data-sync-follow="bottom"]')).map(
    (element) => ({
      id: element.id || "",
      element,
      follow:
        element.scrollHeight <= element.clientHeight ||
        element.scrollTop + element.clientHeight >= element.scrollHeight - 24,
    }),
  );
}

function restoreFollowBottomTargets(mount, targets) {
  for (const target of targets) {
    if (!target.follow) {
      continue;
    }
    const element =
      target.id && globalThis.document?.getElementById
        ? globalThis.document.getElementById(target.id)
        : target.element;
    if (element && mount.contains(element)) {
      element.scrollTop = element.scrollHeight;
    }
  }
}

function captureStableTopTargets(mount) {
  if (!mount?.querySelectorAll) {
    return [];
  }
  return Array.from(
    mount.querySelectorAll('[data-sync-stable-top="true"]'),
  ).map((element) => ({
    id: element.id || "",
    element,
    scrollTop: element.scrollTop,
    scrollHeight: element.scrollHeight,
  }));
}

function restoreStableTopTargets(mount, targets) {
  for (const target of targets) {
    const element =
      target.id && globalThis.document?.getElementById
        ? globalThis.document.getElementById(target.id)
        : target.element;
    if (!element || !mount.contains(element)) {
      continue;
    }
    const heightDelta = element.scrollHeight - target.scrollHeight;
    if (heightDelta > 0) {
      element.scrollTop = target.scrollTop + heightDelta;
    }
  }
}

function bindViewportObservers(mount, sendFn) {
  const elements = mount?.querySelectorAll
    ? mount.querySelectorAll("[data-sync-on-viewport-top]")
    : [];
  const seen = new Set();
  for (const element of elements) {
    const eventName = element.getAttribute("data-sync-on-viewport-top");
    if (!eventName) {
      continue;
    }
    const id = element.id || "";
    const key = `${id}:${eventName}`;
    seen.add(key);
    if (element._viewportKey === key) {
      element._viewportFired = false;
      continue;
    }
    if (element._viewportKey) {
      element.removeEventListener("scroll", element._viewportHandler);
    }
    element._viewportKey = key;
    element._viewportFired = false;
    element._viewportHandler = () => {
      if (element._viewportFired) {
        return;
      }
      const threshold = parseInt(
        element.getAttribute("data-sync-viewport-threshold") || "80",
        10,
      );
      if (
        element.scrollTop <= threshold &&
        element.scrollHeight > element.clientHeight + threshold
      ) {
        element._viewportFired = true;
        sendFn({
          event: eventName,
          scrollTop: element.scrollTop,
          scrollHeight: element.scrollHeight,
          clientHeight: element.clientHeight,
        });
      }
    };
    element.addEventListener("scroll", element._viewportHandler, {
      passive: true,
    });
  }

  for (const element of mount.querySelectorAll("*") || []) {
    if (element._viewportKey && !seen.has(element._viewportKey)) {
      element.removeEventListener("scroll", element._viewportHandler);
      delete element._viewportKey;
      delete element._viewportFired;
      delete element._viewportHandler;
    }
  }
}

function applyPatch(mount, patch) {
  const target = nodeAtPath(mount, patch.path);
  if (patch.op === "replace") {
    const replacement = renderNode(patch.node);
    if (target === mount) {
      mount.replaceChildren(replacement);
    } else {
      target.replaceWith(replacement);
    }
    return;
  }
  if (patch.op === "set_text") {
    if (target.nodeType !== Node.TEXT_NODE) {
      throw new Error("set_text patch target is not a text node");
    }
    target.nodeValue = String(patch.text);
    return;
  }
  if (patch.op === "set_attr") {
    if (target.nodeType !== Node.ELEMENT_NODE) {
      throw new Error("set_attr patch target is not an element");
    }
    applySingleAttribute(target, String(patch.name), patch.value);
    return;
  }
  if (patch.op === "remove_attr") {
    if (target.nodeType !== Node.ELEMENT_NODE) {
      throw new Error("remove_attr patch target is not an element");
    }
    removeSingleAttribute(target, String(patch.name));
    return;
  }
  if (patch.op === "append_child") {
    if (target.nodeType !== Node.ELEMENT_NODE) {
      throw new Error("append_child patch target is not an element");
    }
    target.append(renderNode(patch.node));
    return;
  }
  if (patch.op === "insert_child") {
    if (target.nodeType !== Node.ELEMENT_NODE) {
      throw new Error("insert_child patch target is not an element");
    }
    const index = Number(patch.index);
    target.insertBefore(renderNode(patch.node), target.childNodes[index] ?? null);
    return;
  }
  if (patch.op === "remove_child") {
    if (target === mount) {
      throw new Error("cannot remove DOM mount");
    }
    target.remove();
    return;
  }
  throw new Error(`unsupported DOM patch op: ${patch.op}`);
}

function nodeAtPath(mount, path) {
  if (!Array.isArray(path)) {
    throw new Error("DOM patch path must be an array");
  }
  let node = mount.firstChild;
  if (node === null) {
    throw new Error("DOM patch requires a mounted root");
  }
  for (const index of path) {
    const child = node.childNodes[Number(index)];
    if (child === undefined) {
      throw new Error(`DOM patch path not found: ${path.join("/")}`);
    }
    node = child;
  }
  return node;
}

function renderNode(node) {
  if (Object.hasOwn(node, "text")) {
    return document.createTextNode(String(node.text));
  }

  const tag = String(node.tag);
  if (!SUPPORTED_TAGS.has(tag)) {
    throw new Error(`unsupported snapshot tag: ${tag}`);
  }
  const element = document.createElement(tag);
  if (node.id !== undefined) {
    element.id = String(node.id);
  }
  if (node.class !== undefined) {
    element.className = String(node.class);
  }
  applyAttributes(element, node.attrs ?? {});
  for (const child of node.children ?? []) {
    element.append(renderNode(child));
  }
  return element;
}

function reconcileNode(current, node) {
  if (Object.hasOwn(node, "text")) {
    const text = String(node.text);
    if (current?.nodeType !== Node.TEXT_NODE) {
      return document.createTextNode(text);
    }
    if (current.nodeValue !== text) {
      current.nodeValue = text;
    }
    return current;
  }

  const tag = String(node.tag);
  if (!SUPPORTED_TAGS.has(tag)) {
    throw new Error(`unsupported snapshot tag: ${tag}`);
  }
  if (
    current?.nodeType !== Node.ELEMENT_NODE ||
    current.localName !== tag
  ) {
    return renderNode(node);
  }

  if (node.id === undefined) {
    current.removeAttribute("id");
  } else if (current.id !== String(node.id)) {
    current.id = String(node.id);
  }

  if (node.class === undefined) {
    current.removeAttribute("class");
  } else if (current.className !== String(node.class)) {
    current.className = String(node.class);
  }

  applyAttributes(current, node.attrs ?? {});
  reconcileChildren(current, node.children ?? []);
  return current;
}

function applyAttributes(element, attrs) {
  const wanted = new Set();
  for (const [name, value] of Object.entries(attrs)) {
    validateAttributeName(name);
    wanted.add(name);
    applySingleAttribute(element, name, value);
  }

  for (const name of SUPPORTED_ATTRIBUTES) {
    if (!wanted.has(name) && element.hasAttribute(name)) {
      removeSingleAttribute(element, name);
    }
  }
}

function applySingleAttribute(element, name, value) {
  validateAttributeName(name);
  const text = String(value);
  if (element.getAttribute(name) !== text) {
    element.setAttribute(name, text);
  }
  if (name === "value" && "value" in element && element.value !== text) {
    element.value = text;
  }
}

function removeSingleAttribute(element, name) {
  validateAttributeName(name);
  element.removeAttribute(name);
  if (name === "value" && "value" in element) {
    element.value = "";
  }
}

function validateAttributeName(name) {
  if (!isSupportedAttribute(name)) {
    throw new Error(`unsupported snapshot attribute: ${name}`);
  }
}

function isSupportedAttribute(name) {
  return (
    SUPPORTED_ATTRIBUTES.has(name) ||
    hasCustomAttributePrefix(name, "aria-") ||
    hasCustomAttributePrefix(name, "data-")
  );
}

function hasCustomAttributePrefix(name, prefix) {
  if (!name.startsWith(prefix) || name.length === prefix.length) {
    return false;
  }
  for (let index = prefix.length; index < name.length; index += 1) {
    const code = name.charCodeAt(index);
    const isLowercase = code >= 0x61 && code <= 0x7a;
    const isDigit = code >= 0x30 && code <= 0x39;
    if (!isLowercase && !isDigit && code !== 0x2d) {
      return false;
    }
  }
  return true;
}

function reconcileChildren(parent, nodes) {
  for (let index = 0; index < nodes.length; index += 1) {
    const current = parent.childNodes[index];
    const next = reconcileNode(current, nodes[index]);
    if (current === undefined) {
      parent.append(next);
    } else if (next !== current) {
      parent.replaceChild(next, current);
    }
  }

  while (parent.childNodes.length > nodes.length) {
    parent.lastChild.remove();
  }
}

export function focusAfterSubmit(form) {
  const preferred =
    document.getElementById("command") ??
    form.querySelector("input[name='text']:not([type='hidden'])") ??
    document.querySelector("input[name='text']:not([type='hidden'])");
  preferred?.focus();
}

function addCssClass(element, name) {
  if (!element) {
    return;
  }
  if (element.classList?.add) {
    element.classList.add(name);
    return;
  }
  const classes = new Set(String(element.className ?? "").split(/\s+/).filter(Boolean));
  classes.add(name);
  element.className = Array.from(classes).join(" ");
}

function removeCssClass(element, name) {
  if (!element) {
    return;
  }
  if (element.classList?.remove) {
    element.classList.remove(name);
    return;
  }
  const classes = String(element.className ?? "")
    .split(/\s+/)
    .filter((value) => value && value !== name);
  element.className = classes.join(" ");
}

function formControls(form) {
  if (form?.elements) {
    return Array.from(form.elements);
  }
  return Array.from(form?.querySelectorAll?.("button, input, select, textarea") ?? []);
}

function isButtonControl(control) {
  const tag = String(control?.localName ?? control?.tagName ?? "").toLowerCase();
  return tag === "button" || control?.type === "submit" || control?.type === "button";
}

function isReadonlyControl(control) {
  const tag = String(control?.localName ?? control?.tagName ?? "").toLowerCase();
  return tag === "input" || tag === "textarea";
}

function setBooleanAttribute(element, name, value) {
  if (value) {
    element?.setAttribute?.(name, "true");
  } else {
    element?.removeAttribute?.(name);
  }
}

function showDisableWith(element) {
  const disableWith = element?.getAttribute?.("data-sync-disable-with");
  if (disableWith === null || disableWith === undefined || disableWith === "") {
    return null;
  }
  const indicator = document.createElement("span");
  indicator.setAttribute("class", "sync-disable-with-label");
  indicator.setAttribute("aria-hidden", "true");
  indicator.textContent = disableWith;
  element.append(indicator);
  return indicator;
}

function hideDisableWith(indicator) {
  indicator?.remove?.();
}

export function beginSubmitLoading(form, submit) {
  const token = {
    form,
    submit,
    disabled: [],
    readonly: [],
    disableWith: null,
  };
  addCssClass(form, SYNC_LOADING_CLASS);
  addCssClass(form, SYNC_SUBMIT_LOADING_CLASS);
  addCssClass(submit, SYNC_LOADING_CLASS);
  addCssClass(submit, SYNC_SUBMIT_LOADING_CLASS);
  setBooleanAttribute(form, "aria-busy", true);
  setBooleanAttribute(submit, "aria-busy", true);

  for (const control of formControls(form)) {
    if (isButtonControl(control)) {
      if (!control.disabled) {
        token.disabled.push(control);
        control.disabled = true;
      }
      continue;
    }
    if (isReadonlyControl(control) && control.type !== "hidden" && !control.readOnly) {
      token.readonly.push(control);
      control.readOnly = true;
    }
  }

  token.disableWith = showDisableWith(submit);
  return token;
}

export function endSubmitLoading(token) {
  if (!token) {
    return;
  }
  for (const control of token.disabled) {
    control.disabled = false;
  }
  for (const control of token.readonly) {
    control.readOnly = false;
  }
  hideDisableWith(token.disableWith);
  setBooleanAttribute(token.form, "aria-busy", false);
  setBooleanAttribute(token.submit, "aria-busy", false);
  removeCssClass(token.form, SYNC_LOADING_CLASS);
  removeCssClass(token.form, SYNC_SUBMIT_LOADING_CLASS);
  removeCssClass(token.submit, SYNC_LOADING_CLASS);
  removeCssClass(token.submit, SYNC_SUBMIT_LOADING_CLASS);
}

export function beginEventLoading(element) {
  const token = {
    element,
    disabled: false,
    readonly: false,
    disableWith: null,
  };
  addCssClass(element, SYNC_LOADING_CLASS);
  setBooleanAttribute(element, "aria-busy", true);
  if (isButtonControl(element) && !element.disabled) {
    token.disabled = true;
    element.disabled = true;
  } else if (
    isReadonlyControl(element) &&
    element.type !== "hidden" &&
    !element.readOnly
  ) {
    token.readonly = true;
    element.readOnly = true;
  }

  token.disableWith = showDisableWith(element);
  return token;
}

export function endEventLoading(token) {
  if (!token) {
    return;
  }
  if (token.disabled) {
    token.element.disabled = false;
  }
  if (token.readonly) {
    token.element.readOnly = false;
  }
  hideDisableWith(token.disableWith);
  setBooleanAttribute(token.element, "aria-busy", false);
  removeCssClass(token.element, SYNC_LOADING_CLASS);
}

function controlValue(control) {
  if (control?.type === "checkbox") {
    return control.checked ? (control.value ?? "true") : "false";
  }
  if (control?.type === "radio") {
    return control.checked ? (control.value ?? "on") : undefined;
  }
  return control?.value;
}

function syncValueAttributes(element) {
  if (element?.getAttributeNames) {
    return element
      .getAttributeNames()
      .filter((name) => name.startsWith("data-sync-value-"))
      .map((name) => [name, element.getAttribute(name)]);
  }
  if (element?.attributes instanceof Map) {
    return Array.from(element.attributes.entries()).filter(([name]) =>
      name.startsWith("data-sync-value-"),
    );
  }
  return [];
}

export function boundEventFields(element) {
  const form =
    typeof HTMLFormElement !== "undefined" && element instanceof HTMLFormElement
      ? element
      : (element?.form ?? element?.closest?.("form"));
  const fields = form ? Object.fromEntries(new FormData(form).entries()) : {};
  const name = element?.getAttribute?.("name") ?? element?.name;
  const value = controlValue(element);
  if (name && value !== undefined) {
    fields[name] = String(value);
  }
  for (const [attribute, attributeValue] of syncValueAttributes(element)) {
    const name = attribute.slice("data-sync-value-".length).replace(/-/g, "_");
    fields[name] = String(attributeValue ?? "");
  }
  return fields;
}

function parseDurationMs(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  const duration = Number.parseInt(String(value), 10);
  return Number.isFinite(duration) && duration >= 0 ? duration : null;
}

function scheduleBoundEvent(element, callback) {
  const throttleMs = parseDurationMs(element?.getAttribute?.("data-sync-throttle"));
  const now = Date.now();
  if (throttleMs !== null) {
    const next = element._syncThrottleUntil ?? 0;
    if (now < next) {
      return;
    }
    element._syncThrottleUntil = now + throttleMs;
  }

  const debounceMs = parseDurationMs(element?.getAttribute?.("data-sync-debounce"));
  if (debounceMs !== null) {
    clearTimeout(element._syncDebounceTimer);
    element._syncDebounceTimer = setTimeout(callback, debounceMs);
    return;
  }

  callback();
}

export function dispatchSyncLoading(kind, detail) {
  if (typeof window === "undefined" || typeof window.dispatchEvent !== "function") {
    return;
  }
  const EventCtor = globalThis.CustomEvent ?? Event;
  window.dispatchEvent(new EventCtor(`mica:sync-loading-${kind}`, { detail }));
}

function randomSessionId() {
  if (globalThis.crypto?.getRandomValues) {
    const values = new Uint32Array(1);
    globalThis.crypto.getRandomValues(values);
    return String(values[0] || 1);
  }
  return String(Date.now());
}

export class MicaWebTransportSyncClient {
  constructor(options) {
    this.url = options.url;
    this.certificateHash = options.certificateHash;
    this.onEnvelope = options.onEnvelope;
    this.onClose = options.onClose;
    this.onError = options.onError;
    this.transport = null;
    this.writer = null;
    this.chunks = new Map();
  }

  async connect() {
    this.transport = new WebTransport(
      this.url,
      certificateHashOptions(this.certificateHash),
    );
    await this.transport.ready;
    this.transport.datagrams.incomingHighWaterMark = Math.max(
      this.transport.datagrams.incomingHighWaterMark ?? 1,
      64,
    );
    this.transport.datagrams.outgoingHighWaterMark = Math.max(
      this.transport.datagrams.outgoingHighWaterMark ?? 1,
      64,
    );
    this.writer = this.transport.datagrams.writable.getWriter();
    this.readLoop().catch((error) => {
      this.onError?.(error);
    });
    this.readStreamLoop().catch((error) => {
      this.onError?.(error);
    });
    this.transport.closed.then(
      () => this.onClose?.(),
      (error) => this.onError?.(error),
    );
  }

  async sendEnvelope(envelope) {
    await this.writer.write(encodeSyncEnvelope(envelope));
  }

  async sendStreamEnvelope(envelope) {
    const payload = encodeSyncEnvelope(envelope);
    const stream = await this.transport.createUnidirectionalStream();
    const writer = stream.getWriter();
    try {
      await writer.write(payload);
    } finally {
      await writer.close();
    }
  }

  async needView(viewState) {
    await this.sendStreamEnvelope({
      kind: SyncKind.NeedView,
      session: viewState.session,
      view: viewState.view,
      clientRevision: viewState.clientRevision,
      clientSignature: viewState.clientSignature,
      payload: viewState.payload ?? "need",
    });
  }

  async haveView(viewState, options = {}) {
    const envelope = {
      kind: SyncKind.HaveView,
      session: viewState.session,
      view: viewState.view,
      clientRevision: viewState.clientRevision,
      clientSignature: viewState.clientSignature,
      payload: viewState.payload ?? "have",
    };
    if (options.reliable) {
      await this.sendStreamEnvelope(envelope);
      return;
    }
    await this.sendEnvelope(envelope);
  }

  async sendDomEvent(event) {
    const session = BigInt(event.session);
    const view = BigInt(event.view);
    const revision = BigInt(event.revision);
    const signature = BigInt(event.signature);
    await this.sendStreamEnvelope({
      kind: SyncKind.HaveView,
      session,
      view,
      clientRevision: revision,
      clientSignature: signature,
      serverRevision: revision,
      serverSignature: signature,
      payload: JSON.stringify({
        type: "dom_event",
        session: session.toString(),
        view: view.toString(),
        revision: revision.toString(),
        signature: signature.toString(),
        event: String(event.event),
        target: String(event.target ?? ""),
        action: String(event.action ?? ""),
        fields: event.fields ?? {},
      }),
    });
  }

  async readLoop() {
    const reader = this.transport.datagrams.readable.getReader();
    for (;;) {
      const { value, done } = await reader.read();
      if (done) {
        return;
      }
      try {
        const envelope = decodeChunkedSyncEnvelope(value, this.chunks);
        if (envelope && !envelope.raw) {
          this.onEnvelope?.(envelope);
        }
      } catch {
        // Endpoint-targeted legacy emissions can share the datagram channel.
        // DOM sync state is carried only by sync envelopes.
      }
    }
  }

  async readStreamLoop() {
    const incoming = this.transport.incomingUnidirectionalStreams;
    if (!incoming) {
      return;
    }
    const reader = incoming.getReader();
    for (;;) {
      const { value: stream, done } = await reader.read();
      if (done) {
        return;
      }
      try {
        const envelope = decodeSyncEnvelope(await readAllStreamBytes(stream));
        this.onEnvelope?.(envelope);
      } catch (error) {
        this.onError?.(error);
      }
    }
  }

  close() {
    this.transport?.close();
  }
}

export class MicaSseSyncClient {
  constructor(options) {
    this.streamUrl = options.streamUrl;
    this.sendUrl = options.sendUrl;
    this.onEnvelope = options.onEnvelope;
    this.onClose = options.onClose;
    this.onError = options.onError;
    this.source = null;
  }

  async connect() {
    await new Promise((resolve, reject) => {
      let settled = false;
      const source = new EventSource(this.streamUrl);
      const fail = (message) => {
        const error =
          message instanceof Error ? message : new Error(String(message));
        if (!settled) {
          settled = true;
          reject(error);
          return;
        }
        if (source.readyState === EventSource.CLOSED) {
          this.onClose?.();
          return;
        }
        this.onError?.(error);
      };
      source.addEventListener("open", () => {
        this.source = source;
        if (settled) {
          return;
        }
        settled = true;
        resolve();
      });
      source.addEventListener("sync", (event) => {
        try {
          this.onEnvelope?.(JSON.parse(event.data));
        } catch (error) {
          this.onError?.(error);
        }
      });
      source.addEventListener("error", () => {
        fail("SSE connection failed");
      });
    });
  }

  async sendEnvelope(envelope) {
    const response = await fetch(this.sendUrl, {
      method: "POST",
      headers: {
        "content-type": "application/octet-stream",
      },
      body: encodeSyncEnvelope(envelope),
    });
    if (!response.ok) {
      throw new Error(`sync input failed: ${response.status} ${response.statusText}`);
    }
  }

  async sendStreamEnvelope(envelope) {
    await this.sendEnvelope(envelope);
  }

  async needView(viewState) {
    await this.sendEnvelope({
      kind: SyncKind.NeedView,
      session: viewState.session,
      view: viewState.view,
      clientRevision: viewState.clientRevision,
      clientSignature: viewState.clientSignature,
      payload: viewState.payload ?? "need",
    });
  }

  async haveView(viewState) {
    await this.sendEnvelope({
      kind: SyncKind.HaveView,
      session: viewState.session,
      view: viewState.view,
      clientRevision: viewState.clientRevision,
      clientSignature: viewState.clientSignature,
      payload: viewState.payload ?? "have",
    });
  }

  async sendDomEvent(event) {
    const session = BigInt(event.session);
    const view = BigInt(event.view);
    const revision = BigInt(event.revision);
    const signature = BigInt(event.signature);
    await this.sendEnvelope({
      kind: SyncKind.HaveView,
      session,
      view,
      clientRevision: revision,
      clientSignature: signature,
      serverRevision: revision,
      serverSignature: signature,
      payload: JSON.stringify({
        type: "dom_event",
        session: session.toString(),
        view: view.toString(),
        revision: revision.toString(),
        signature: signature.toString(),
        event: String(event.event),
        target: String(event.target ?? ""),
        action: String(event.action ?? ""),
        fields: event.fields ?? {},
      }),
    });
  }

  close() {
    this.source?.close();
  }
}

async function readAllStreamBytes(stream) {
  const reader = stream.getReader();
  const chunks = [];
  let total = 0;
  for (;;) {
    const { value, done } = await reader.read();
    if (done) {
      break;
    }
    chunks.push(value);
    total += value.byteLength;
  }
  const bytes = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return bytes;
}

export function bootstrapServerRenderedSync(mount, status) {
  const params = new URLSearchParams(location.search);
  const transport =
    params.get("transport") ??
    mount.dataset.syncTransport ??
    (params.get("url") ? "webtransport" : "sse");
  const state = {
    transport,
    syncUrl: params.get("syncUrl") ?? mount.dataset.syncUrl ?? "/sync",
    url: params.get("url") ?? mount.dataset.syncTransportUrl ?? "",
    certificateHash: params.get("certHash") ?? "",
    session: BigInt(params.get("session") ?? randomSessionId()),
    view: BigInt(mount.dataset.view),
    revision: BigInt(mount.dataset.revision),
    signature: BigInt(mount.dataset.signature),
    pollMs: parseInt(params.get("pollMs") ?? "1000", 10),
  };
  let connected = false;
  let client;
  let connectPromise;
  let connectError = null;
  let initialSyncResolve;
  let initialSyncReject;
  let initialSynced = false;
  const initialSyncPromise = new Promise((resolve, reject) => {
    initialSyncResolve = resolve;
    initialSyncReject = reject;
  });
  let pollTimer = null;
  let inFlightDomEvent = null;
  const api = { client: null, state };

  function setStatus(text) {
    status.textContent = text;
  }

  function connectionFailureText(error) {
    const message = String(error ?? connectError ?? "connection failed");
    if (state.transport === "webtransport" && !state.certificateHash) {
      return `${message}. Open the URL printed by the smoke script, including certHash.`;
    }
    return message;
  }

  function viewState(payload) {
    return {
      session: state.session,
      view: state.view,
      clientRevision: state.revision,
      clientSignature: state.signature,
      payload,
    };
  }

  function stopPolling() {
    if (pollTimer !== null) {
      clearInterval(pollTimer);
      pollTimer = null;
    }
  }

  function startPolling() {
    stopPolling();
    if (!Number.isFinite(state.pollMs) || state.pollMs <= 0) {
      return;
    }
    pollTimer = setInterval(() => {
      if (connected) {
        client.haveView(viewState("poll")).catch((error) => setStatus(String(error)));
      }
    }, state.pollMs);
  }

  function finishInFlightDomEvent() {
    const event = inFlightDomEvent;
    if (event === null) {
      return;
    }
    event.end(event.loading);
    inFlightDomEvent = null;
    dispatchSyncLoading("stop", {
      kind: event.kind,
      target: event.target,
      action: event.action,
    });
  }

  function sendViewportEvent(data) {
    if (!connected || !initialSynced) {
      return;
    }
    client
      .sendDomEvent({
        session: state.session,
        view: state.view,
        revision: state.revision,
        signature: state.signature,
        event: "scroll",
        target: "",
        action: data.event,
        fields: {
          scroll_top: String(data.scrollTop),
          scroll_height: String(data.scrollHeight),
          client_height: String(data.clientHeight),
        },
      })
      .catch((error) => setStatus(`Viewport event failed: ${String(error)}`));
  }

  async function sendBoundEvent(element, eventName) {
    if (inFlightDomEvent !== null) {
      return;
    }
    if (!connected || !initialSynced) {
      if (connectError) {
        setStatus(connectionFailureText(connectError));
        return;
      }
      setStatus("Connecting");
      try {
        if (!(await connectPromise)) {
          setStatus(connectionFailureText(connectError));
          return;
        }
      } catch (error) {
        connectError = error;
        setStatus(connectionFailureText(error));
        return;
      }
    }

    const action = element.dataset.syncAction ?? "";
    const loading = beginEventLoading(element);
    inFlightDomEvent = {
      kind: eventName,
      target: element,
      action,
      loading,
      end: endEventLoading,
    };
    dispatchSyncLoading("start", {
      kind: eventName,
      target: element,
      action,
    });
    try {
      await client.sendDomEvent({
        session: state.session,
        view: state.view,
        revision: state.revision,
        signature: state.signature,
        event: eventName,
        target: element.id ?? "",
        action,
        fields: boundEventFields(element),
      });
    } catch (error) {
      setStatus(`Event failed: ${String(error)}`);
      finishInFlightDomEvent();
    }
  }

  function handleBoundEvent(event) {
    const element = event.target?.closest?.("[data-sync-event][data-sync-action]");
    if (!element || !mount.contains(element)) {
      return;
    }
    const eventName = element.dataset.syncEvent;
    if (eventName === "submit" || event.type !== eventName) {
      return;
    }
    event.preventDefault?.();
    scheduleBoundEvent(element, () => {
      sendBoundEvent(element, eventName).catch((error) =>
        setStatus(`Event failed: ${String(error)}`),
      );
    });
  }

  function accept(envelope) {
    state.revision = BigInt(envelope.serverRevision);
    state.signature = BigInt(envelope.serverSignature);
    mount.dataset.revision = envelope.serverRevision;
    mount.dataset.signature = envelope.serverSignature;
    setStatus(`Synced revision ${envelope.serverRevision}`);
    if (!initialSynced) {
      initialSynced = true;
      initialSyncResolve(true);
    }
    bindViewportObservers(mount, sendViewportEvent);
    finishInFlightDomEvent();
  }

  function handle(envelope) {
    if (envelope.kind === "ViewSnapshot") {
      const serverRevision = BigInt(envelope.serverRevision);
      const serverSignature = BigInt(envelope.serverSignature);
      if (
        initialSynced &&
        (serverRevision < state.revision ||
          (serverRevision === state.revision && serverSignature === state.signature))
      ) {
        return;
      }
      const snapshot = validateSnapshotEnvelope(envelope);
      if (!snapshot.valid) {
        setStatus("Snapshot rejected");
        return;
      }
      applySnapshot(mount, snapshot.payload);
      accept(envelope);
      return;
    }
    if (envelope.kind === "ViewDelta") {
      const serverRevision = BigInt(envelope.serverRevision);
      const serverSignature = BigInt(envelope.serverSignature);
      if (
        serverRevision < state.revision ||
        (serverRevision === state.revision && serverSignature === state.signature)
      ) {
        return;
      }
      if (
        BigInt(envelope.clientRevision) !== state.revision ||
        BigInt(envelope.clientSignature) !== state.signature
      ) {
        client
          .haveView(viewState("stale"), { reliable: true })
          .catch((error) => setStatus(String(error)));
        return;
      }
      const delta = validateDeltaEnvelope(envelope);
      try {
        if (!delta.valid) {
          throw new Error("Delta rejected");
        }
        applyDelta(mount, delta.payload);
        accept(envelope);
      } catch (error) {
        setStatus("Recovering");
        state.revision = 0n;
        state.signature = 0n;
        if (!initialSynced) {
          initialSyncReject(error);
        }
        client
          .needView(viewState("recover"))
          .catch((requestError) => setStatus(String(requestError)));
      }
    }
  }

  async function connect() {
    const onClose = () => {
      connected = false;
      stopPolling();
      finishInFlightDomEvent();
      if (!initialSynced) {
        initialSyncReject(
          new Error(
            state.transport === "webtransport"
              ? "WebTransport closed before initial sync"
              : "SSE stream closed before initial sync",
          ),
        );
      }
      setStatus("Disconnected");
    };
    const onError = (error) => {
      connected = false;
      stopPolling();
      finishInFlightDomEvent();
      if (!initialSynced) {
        initialSyncReject(error);
      }
      setStatus(String(error));
    };
    if (state.transport === "webtransport") {
      if (!state.url) {
        throw new Error(
          "missing WebTransport URL; pass ?transport=webtransport&url=https://.../view",
        );
      }
      client = new MicaWebTransportSyncClient({
        url: state.url,
        certificateHash: state.certificateHash,
        onEnvelope: handle,
        onClose,
        onError,
      });
    } else if (state.transport === "sse") {
      client = new MicaSseSyncClient({
        streamUrl: `${state.syncUrl}/events?session=${state.session}`,
        sendUrl: `${state.syncUrl}/input`,
        onEnvelope: handle,
        onClose,
        onError,
      });
    } else {
      throw new Error(`unsupported sync transport: ${state.transport}`);
    }
    api.client = client;
    await client.connect();
    connectError = null;
    startPolling();
    await client.needView(viewState("initial"));
    await initialSyncPromise;
    connected = true;
  }

  async function sendForm(form, submit) {
    if (inFlightDomEvent !== null) {
      return;
    }
    if (!connected || !initialSynced) {
      if (connectError) {
        setStatus(connectionFailureText(connectError));
        return;
      }
      setStatus("Connecting");
      try {
        if (!(await connectPromise)) {
          setStatus(connectionFailureText(connectError));
          return;
        }
      } catch (error) {
        connectError = error;
        setStatus(connectionFailureText(error));
        return;
      }
    }
    if (
      !(form instanceof HTMLFormElement) ||
      form.dataset.syncEvent !== "submit"
    ) {
      return;
    }
    submit ??= form.querySelector("button");
    const fields = Object.fromEntries(new FormData(form).entries());
    if (
      form.dataset.syncAction === "mud_command" &&
      String(fields.text ?? "").trim().length === 0
    ) {
      return;
    }
    const loading = beginSubmitLoading(form, submit);
    const action = form.dataset.syncAction ?? "";
    inFlightDomEvent = {
      kind: "submit",
      target: form,
      action,
      loading,
      end: endSubmitLoading,
    };
    dispatchSyncLoading("start", {
      kind: "submit",
      target: form,
      action,
    });
    try {
      await client.sendDomEvent({
        session: state.session,
        view: state.view,
        revision: state.revision,
        signature: state.signature,
        event: "submit",
        target: form.id,
        action,
        fields,
      });
      form.reset();
      focusAfterSubmit(form);
    } catch (error) {
      setStatus(`Event failed: ${String(error)}`);
      finishInFlightDomEvent();
    }
  }

  mount.addEventListener("submit", async (event) => {
    event.preventDefault();
    await sendForm(event.target, event.submitter);
  });
  mount.addEventListener("click", handleBoundEvent);
  mount.addEventListener("change", handleBoundEvent);
  mount.addEventListener("input", handleBoundEvent);

  connectPromise = connect().then(
    () => true,
    (error) => {
      connectError = error;
      setStatus(connectionFailureText(error));
      return false;
    },
  );

  return api;
}
