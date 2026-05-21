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
  "aside",
  "button",
  "div",
  "form",
  "h1",
  "h2",
  "header",
  "input",
  "li",
  "main",
  "nav",
  "p",
  "section",
  "span",
  "strong",
  "ul",
]);
const SUPPORTED_ATTRIBUTES = new Set([
  "aria-label",
  "aria-live",
  "autocomplete",
  "data-command",
  "data-entity",
  "data-sync-action",
  "data-sync-event",
  "data-sync-key",
  "class",
  "id",
  "name",
  "placeholder",
  "type",
  "value",
]);
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
  reconcileChildren(mount, [payload.root]);
}

export function applyDelta(mount, payload) {
  if (payload.type !== "dom_patch") {
    throw new Error(`unsupported delta type: ${payload.type}`);
  }
  for (const patch of payload.patches) {
    applyPatch(mount, patch);
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
  if (!SUPPORTED_ATTRIBUTES.has(name)) {
    throw new Error(`unsupported snapshot attribute: ${name}`);
  }
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
    this.writer = this.transport.datagrams.writable.getWriter();
    this.readLoop().catch((error) => {
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
    await this.writer.write(
      new TextEncoder().encode(
        JSON.stringify({
          type: "dom_event",
          session: BigInt(event.session).toString(),
          view: BigInt(event.view).toString(),
          revision: BigInt(event.revision).toString(),
          signature: BigInt(event.signature).toString(),
          event: String(event.event),
          target: String(event.target ?? ""),
          action: String(event.action ?? ""),
          fields: event.fields ?? {},
        }),
      ),
    );
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

  close() {
    this.transport?.close();
  }
}

export function bootstrapServerRenderedSync(mount, status) {
  const params = new URLSearchParams(location.search);
  const state = {
    url: params.get("url") ?? mount.dataset.webtransportUrl,
    certificateHash: params.get("certHash") ?? "",
    session: BigInt(params.get("session") ?? String(Date.now() % 1000000)),
    view: BigInt(mount.dataset.view),
    revision: BigInt(mount.dataset.revision),
    signature: BigInt(mount.dataset.signature),
  };
  let connected = false;
  let client;
  const api = { client: null, state };

  function setStatus(text) {
    status.textContent = text;
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

  function accept(envelope) {
    state.revision = BigInt(envelope.serverRevision);
    state.signature = BigInt(envelope.serverSignature);
    setStatus(`Synced revision ${envelope.serverRevision}`);
    client.haveView(viewState("have")).catch((error) => setStatus(String(error)));
  }

  function handle(envelope) {
    if (envelope.kind === "ViewSnapshot") {
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
        client
          .needView(viewState("recover"))
          .catch((requestError) => setStatus(String(requestError)));
      }
    }
  }

  async function connect() {
    client = new MicaWebTransportSyncClient({
      url: state.url,
      certificateHash: state.certificateHash,
      onEnvelope: handle,
      onClose: () => {
        connected = false;
        setStatus("Disconnected");
      },
      onError: (error) => {
        connected = false;
        setStatus(String(error));
      },
    });
    api.client = client;
    await client.connect();
    connected = true;
    await client.haveView(viewState("initial"));
  }

  mount.addEventListener("submit", async (event) => {
    event.preventDefault();
    if (!connected) {
      return;
    }
    const form = event.target;
    if (
      !(form instanceof HTMLFormElement) ||
      form.dataset.syncEvent !== "submit"
    ) {
      return;
    }
    const submit = form.querySelector("button[type='submit']");
    const fields = Object.fromEntries(new FormData(form).entries());
    if (String(fields.text ?? "").trim().length === 0) {
      return;
    }
    submit.disabled = true;
    try {
      await client.sendDomEvent({
        session: state.session,
        view: state.view,
        revision: state.revision,
        signature: state.signature,
        event: "submit",
        target: form.id,
        action: form.dataset.syncAction ?? "",
        fields,
      });
      form.reset();
      form.elements.namedItem("actor")?.focus();
    } finally {
      submit.disabled = false;
    }
  });

  connect().catch((error) => setStatus(String(error)));
  return api;
}
