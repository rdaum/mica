import { bootstrapServerRenderedSync } from "/sync-client.js";
window.micaChat = bootstrapServerRenderedSync(
  document.getElementById("mount"),
  document.getElementById("status"),
);
