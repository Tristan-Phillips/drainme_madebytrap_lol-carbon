// The terminal "wire" and the exact-amount payouts list. Everything is built with textContent;
// event data is never parsed as HTML.

import { shortAddress } from "./nanoaddr.js";
import { withCommas } from "./digits.js";

const MAX_LINES = 80;
const MAX_PAYOUTS = 8;
const HASH_RE = /^[0-9A-Fa-f]{64}$/;
const RAW_RE = /^\d{1,31}$/;

const span = (cls, text) => {
  const s = document.createElement("span");
  s.className = cls;
  s.textContent = text;
  return s;
};
// one formatter for every line: toLocaleTimeString builds a new one per call, which was the wire's second-largest cost
const timeFmt = new Intl.DateTimeFormat([], { hour: "2-digit", minute: "2-digit", second: "2-digit", hourCycle: "h23" });
const clock = (t) => timeFmt.format(t);

// isPayout is the shape check every payout event passes before any of it reaches the page
export const isPayout = (p) => typeof p?.a === "string" && RAW_RE.test(p.raw) && HASH_RE.test(p.hash);

export function blockLink(hash) {
  const link = document.createElement("a");
  link.href = `https://blocklattice.io/block/${hash.toUpperCase()}`;
  link.target = "_blank";
  link.rel = "noopener noreferrer";
  link.textContent = `${hash.slice(0, 10)}…${hash.slice(-8)}`;
  link.setAttribute("aria-label", `block ${hash}, opens in an explorer`);
  return link;
}

export function createFeed({ log, list }) {
  let paused = false;
  // lines wait here for the next frame, so ten lines a second cost one layout read and one write, not ten of each
  let pending = [];
  let frame = 0;

  function flush() {
    frame = 0;
    const stick = log.scrollTop + log.clientHeight >= log.scrollHeight - 24;
    log.append(...pending);
    pending = [];
    while (log.childElementCount > MAX_LINES) log.firstElementChild.remove();
    if (stick) log.scrollTop = log.scrollHeight;
  }

  function append(line) {
    pending.push(line);
    if (pending.length > MAX_LINES) pending.shift(); // a hidden tab gets no frames; keep only what would show
    if (!frame) frame = requestAnimationFrame(flush);
  }

  function line(kind, parts) {
    const row = document.createElement("div");
    row.className = `ln ${kind}`;
    row.append(...parts);
    append(row);
  }

  return {
    click({ a, t }) {
      if (paused || typeof a !== "string") return;
      line("clk", [span("t", clock(t)), span("a", shortAddress(a)), span("v", "+1 raw")]);
    },

    payout({ a, raw, hash, t }, { quiet = false } = {}) {
      if (!isPayout({ a, raw, hash })) return;
      // quiet: a payout from before this visit, for the list only, never a line on the live wire
      if (!paused && !quiet) line("pay", [span("t", clock(t)), span("a", shortAddress(a)), span("v", `paid ${withCommas(raw)} raw`)]);

      // every reconnect (a tab back from hiding, a dropped stream) resends the recent payouts, so the list is
      // keyed by block hash and kept newest first rather than prepended to
      const key = hash.toUpperCase();
      if ([...list.children].some((li) => li.dataset.hash === key)) return;
      const empty = list.querySelector(".empty");
      if (empty) empty.remove();

      const item = document.createElement("li");
      item.dataset.hash = key;
      item.dataset.t = String(t);
      const meta = span("meta", `${clock(t)} to ${shortAddress(a)} · block `);
      meta.append(blockLink(hash));
      item.append(span("amt", `${withCommas(raw)} raw`), meta);
      const after = [...list.children].find((li) => Number(li.dataset.t) < t);
      list.insertBefore(item, after ?? null);
      while (list.childElementCount > MAX_PAYOUTS) list.lastElementChild.remove();
    },

    system(text) { line("sys", [span("sys", text)]); },

    setPaused(next) {
      paused = next;
      this.system(next ? "-- paused --" : "-- resumed --");
    },
  };
}
