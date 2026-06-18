import { bootstrapServerRenderedSync } from "/sync-client.js?surface=mud";
window.micaMud = bootstrapServerRenderedSync(document.getElementById("mount"));
