const MAGIC = [0x4d, 0x53, 0x59, 0x31];
const HEADER_LEN = 56;

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
  "button",
  "div",
  "form",
  "input",
  "li",
  "main",
  "p",
  "section",
  "span",
  "ul",
]);

function writeU64(view, offset, value) {
  view.setBigUint64(offset, BigInt(value), true);
}

function readU64(view, offset) {
  return view.getBigUint64(offset, true).toString();
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
  if (data.length < HEADER_LEN || !MAGIC.every((byte, index) => data[index] === byte)) {
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
  const expectedSignature =
    BigInt(envelope.serverRevision) + BigInt(envelope.payload.length);
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
  const expectedSignature =
    BigInt(envelope.serverRevision) + BigInt(envelope.payload.length);
  return {
    payload,
    valid:
      payload.type === "append_message" &&
      String(payload.view) === envelope.view &&
      String(payload.revision) === envelope.serverRevision &&
      expectedSignature.toString() === envelope.serverSignature,
  };
}

export function applySnapshot(mount, payload) {
  mount.replaceChildren(renderNode(payload.root));
}

export function applyDelta(mount, payload) {
  if (payload.type !== "append_message") {
    throw new Error(`unsupported delta type: ${payload.type}`);
  }
  const messages = mount.querySelector("#messages");
  if (messages === null) {
    throw new Error("snapshot delta requires #messages mount");
  }
  messages.append(renderNode(payload.message));
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
  for (const child of node.children ?? []) {
    element.append(renderNode(child));
  }
  return element;
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

  async postChat(message) {
    await this.writer.write(
      new TextEncoder().encode(
        JSON.stringify({
          type: "chat_post",
          room: Number(message.room),
          actor: String(message.actor),
          text: String(message.text),
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
      this.onEnvelope?.(decodeSyncEnvelope(value));
    }
  }

  close() {
    this.transport?.close();
  }
}
