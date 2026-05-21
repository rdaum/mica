import assert from "node:assert/strict";
import {
  SyncKind,
  decodeChunkedSyncEnvelope,
  decodeSyncEnvelope,
  encodeSyncEnvelope,
  focusAfterSubmit,
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
