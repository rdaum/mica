#!/usr/bin/env node
import assert from "node:assert/strict";
import { createRequire } from "node:module";

const url = process.env.MICA_MUD_SMOKE_URL;
if (!url) {
  console.error(
    "usage: MICA_MUD_SMOKE_URL='http://127.0.0.1:8018/mud?...' node scripts/mud-browser-smoke.mjs",
  );
  console.error("start the fixture with scripts/mud.sh first");
  process.exit(2);
}

const require = createRequire(import.meta.url);
const browserName = process.env.MICA_BROWSER_NAME ?? "chromium";
let browsers;
try {
  browsers = require("playwright");
} catch {
  console.error("missing Playwright runtime; install it with: npm install --no-save playwright");
  process.exit(2);
}
const browserType = browsers[browserName];
if (!browserType) {
  console.error(`unknown Playwright browser: ${browserName}`);
  process.exit(2);
}

const browser = await browserType.launch({
  headless: process.env.MICA_BROWSER_HEADLESS !== "0",
});

try {
  const aliceContext = await browser.newContext();
  const bobContext = await browser.newContext();
  const createdContext = await browser.newContext();
  const alice = await aliceContext.newPage();
  const bob = await bobContext.newPage();
  const created = await createdContext.newPage();

  await alice.goto(url);
  await bob.goto(url);
  await created.goto(url);

  await signIn(alice, "alice", "alice-pass");
  await signIn(bob, "bob", "bob-pass");
  const createdLogin = `player-${Date.now()}`;
  await createPlayer(created, createdLogin, "player-pass");

  await alice.locator("#room").waitFor();
  await bob.locator("#room").waitFor();
  await created.locator("#room").waitFor();
  const shellWidth = await alice
    .locator(".mud-shell")
    .evaluate((element) => element.getBoundingClientRect().width);
  const viewportWidth = await alice.evaluate(() => window.innerWidth);
  assert.ok(shellWidth >= viewportWidth - 2, `mud shell width ${shellWidth} did not fill viewport ${viewportWidth}`);
  assert.equal(await alice.locator("#status, .sync-status, .status").count(), 0);
  await expectText(created, "First Room");
  await expectText(created, createdLogin);
  await alice.waitForFunction(() => {
    const room = document.querySelector("#room");
    return (
      room?.textContent.includes("First Room") &&
      room.textContent.includes("plain stone room") &&
      room.textContent.includes("north") &&
      room.textContent.includes("coin") &&
      room.textContent.includes("box") &&
      room.textContent.includes("button")
    );
  });
  await alice.waitForFunction(() => {
    const presence = document.querySelector("#presence");
    return (
      presence?.textContent.includes("Alice") &&
      presence.textContent.includes("Bob") &&
      presence.textContent.includes("Bob is here")
    );
  });

  const smokeLine = `say browser smoke ${Date.now()}`;

  await alice.locator("form[data-sync-action='mud_command'] button[data-command='get coin']").click();
  await alice.waitForFunction(() => {
    const room = document.querySelector("#room");
    const inventory = document.querySelector("#inventory");
    return (
      room?.textContent.includes("box") &&
      room.textContent.includes("button") &&
      !room.textContent.includes("coin") &&
      inventory?.textContent.includes("coin") &&
      !inventory.textContent.includes("Empty hands")
    );
  });

  const bobPlayerButton = alice.locator("#presence button.presence-name", { hasText: "Bob" }).first();
  await bobPlayerButton.waitFor();
  await bobPlayerButton.click();
  await alice.waitForFunction(() => {
    const inspector = document.querySelector("#inspector");
    return (
      inspector?.dataset.entity === "#bob" &&
      inspector.textContent.includes("Bob is here") &&
      inspector.querySelector(".entity-avatar")?.textContent === "B" &&
      inspector.querySelector(".entity-kind .entity-fact-value")?.textContent === "player" &&
      inspector.querySelector(".entity-location .entity-fact-value")?.textContent === "First Room" &&
      inspector.querySelector(".entity-visibility .entity-fact-value")?.textContent === "visible" &&
      inspector.querySelector(".retrieval-panel")?.textContent.includes("Related context") &&
      inspector.querySelector(".inspector-actions")?.textContent.includes("look") &&
      inspector.querySelector(".inspector-actions")?.textContent.includes("Mica inspect")
    );
  });
  await alice.waitForFunction(() => {
    const inspector = document.querySelector("#inspector");
    return (
      inspector?.classList.contains("inspect-flash-even") || inspector?.classList.contains("inspect-flash-odd")
    );
  });
  await alice.locator(".retrieval-action .retrieval-button").click();
  await alice.waitForFunction(() => {
    const panel = document.querySelector(".retrieval-panel");
    const rows = Array.from(panel?.querySelectorAll(".retrieval-row") ?? []);
    return (
      rows.length > 0 &&
      rows.some((row) => row.textContent.includes("ready")) &&
      rows.some((row) => row.querySelector("button"))
    );
  });

  await alice.locator("#mud-command input[name='text']").fill(smokeLine);
  await alice.locator("#mud-command button[type='submit']").click();

  await bob.waitForFunction((line) => {
    const narrative = document.querySelector("#narrative");
    return (
      narrative?.dataset.syncFollow === "bottom" &&
      !narrative.querySelector(".narrative-count") &&
      !narrative.querySelector(".event-seq") &&
      !narrative.textContent.includes(`> ${line}`) &&
      narrative.querySelector(".event-line.speech .event-kind")?.textContent === "speech" &&
      narrative.querySelector(".event-line.speech .actor-entity")?.textContent === "Alice" &&
      narrative.querySelector(".event-line.speech .event-quote")?.textContent &&
      narrative.textContent.includes(line.replace(/^say /, ""))
    );
  }, smokeLine);

  const aliceEventButton = bob.locator("#narrative button.actor-entity", { hasText: "Alice" }).first();
  await aliceEventButton.waitFor();
  await aliceEventButton.click();
  await bob.waitForFunction(() => {
    const inspector = document.querySelector("#inspector");
    return (
      inspector?.dataset.entity === "#alice" &&
      inspector.textContent.includes("Alice is alert and ready") &&
      inspector.querySelector(".entity-kind .entity-fact-value")?.textContent === "player"
    );
  });

  const inspectorText = await bob.locator("#inspector").innerText();
  assert.match(inspectorText, /Alice/);
  assert.match(inspectorText, /stranger affordances/);

  await alice.locator("#mud-command input[name='text']").fill("push button");
  await alice.locator("#mud-command button[type='submit']").click();
  await alice.waitForFunction(() => {
    const narrative = document.querySelector("#narrative");
    return narrative?.textContent.includes("begins to hum");
  });
  await bob.waitForFunction(() => {
    const narrative = document.querySelector("#narrative");
    return (
      narrative?.dataset.syncFollow === "bottom" &&
      narrative.querySelector(".event-line.alert .event-kind")?.textContent === "alert" &&
      narrative.querySelector(".event-line.alert .event-text")?.textContent.includes("cheerful ding") &&
      narrative.querySelector(".event-line.alert button.event-entity")?.textContent === "button"
    );
  });
  await bob.locator("#narrative .event-line.alert button.event-entity", { hasText: "button" }).click();
  await bob.waitForFunction(() => {
    const inspector = document.querySelector("#inspector");
    return (
      inspector?.dataset.entity === "#red_button" &&
      inspector.textContent.includes("red button protrudes") &&
      inspector.querySelector(".entity-kind .entity-fact-value")?.textContent === "thing" &&
      inspector.querySelector(".entity-mobility .entity-fact-value")?.textContent === "fixed"
    );
  });

  console.log("MUD browser smoke passed");
} finally {
  await browser.close();
}

async function expectText(page, text) {
  await page.waitForFunction((expected) => document.body?.textContent.includes(expected), text);
}

async function signIn(page, login, password) {
  const form = page.locator("form.local-signin");
  if (await form.count() === 0) {
    return;
  }
  await form.locator("input[name='login']").fill(login);
  await form.locator("input[name='password']").fill(password);
  await Promise.all([
    page.waitForURL(/\/mud(?:[?#].*)?$/),
    form.locator("button[type='submit']").click(),
  ]);
}

async function createPlayer(page, login, password) {
  const form = page.locator("form.local-signin");
  if (await form.count() === 0) {
    return;
  }
  await page.locator("a.auth-tab", { hasText: "Create Player" }).click();
  const createForm = page.locator("form.local-signin");
  await createForm.locator("input[name='login']").fill(login);
  await createForm.locator("input[name='password']").fill(password);
  await createForm.locator("input[name='confirm_password']").fill(password);
  await Promise.all([
    page.waitForURL(/\/mud(?:[?#].*)?$/),
    createForm.locator("button[type='submit']").click(),
  ]);
}
