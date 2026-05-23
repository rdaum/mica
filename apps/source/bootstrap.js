import { bootstrapServerRenderedSync } from "/sync-client.js?surface=source";

window.micaSource = bootstrapServerRenderedSync(
  document.getElementById("mount"),
  document.getElementById("status"),
);
