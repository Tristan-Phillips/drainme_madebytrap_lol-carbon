// Page wiring: snapshots into the readouts, address handling, clicking, cash-out, copy buttons.

import { validateAddress, shortAddress } from "./nanoaddr.js";
import { createVaultDigits, withCommas, formatUnit, toXno } from "./digits.js";
import { createOrb } from "./orb.js";
import { createFeed, isPayout, blockLink } from "./feed.js";
import { connectLive } from "./source.js";

const $ = (id) => document.getElementById(id);
const reduced = matchMedia("(prefers-reduced-motion: reduce)");
const mockMode = new URLSearchParams(location.search).has("mock");
const ADDR_KEY = "drainme.addr";
const MAX_IN_FLIGHT = 8;
const STALE_AFTER_MS = 4000;
const DEFAULT_HOST = "https://drainme.madebytrap.lol";
// every click is a request, so each carries as little as possible: no cookies (the server reads none) and no
// cache lookup; keepalive lets the last clicks of a hold land even if the tab closes
const CLICK_INIT = { method: "POST", keepalive: true, credentials: "omit", cache: "no-store" };

const store = {
  get(k) { try { return localStorage.getItem(k); } catch { return null; } },
  set(k, v) { try { localStorage.setItem(k, v); } catch { /* private mode: it just won't be remembered */ } },
  del(k) { try { localStorage.removeItem(k); } catch { /* same */ } },
};

const state = { addr: null, mode: "raw", snap: null, snapAt: 0, mine: 0, inFlight: 0, waiting: null, lastPaid: 0, conn: "connecting", lastErrAt: 0, vault: null };

const vault = createVaultDigits($("vault-digits"));
const feed = createFeed({ log: $("wire-log"), list: $("paylist") });
const orb = createOrb({ button: $("orb"), wrap: document.querySelector(".orb-wrap"), onFire: fire });

/* ---------- messages ---------- */

function say(el, text, kind = "err") {
  el.textContent = text;
  el.dataset.kind = kind;
  el.dataset.icon = kind === "ok" ? "✓" : "✕";
}

/* ---------- connection state ---------- */

const CONN_TEXT = { live: "live", stale: "stale, reconnecting", nosignal: "no signal, retrying", connecting: "connecting", full: "stream full, numbers every 30 s", demo: "demo data" };

function setConn(next) {
  state.conn = next;
  $("conn").dataset.state = next === "demo" ? "live" : next;
  // "stale" means we had data and lost it; before the first snapshot it is simply no signal
  $("conn-text").textContent = CONN_TEXT[next === "stale" && !state.snap ? "nosignal" : next];
  $("demo-banner").hidden = next !== "demo";
}

// snapAt is a performance.now() reading, so it is compared against that clock, never Date.now()
const isStale = () => !mockMode && state.snap && performance.now() - state.snapAt > STALE_AFTER_MS;

/* ---------- readouts ---------- */

function renderVault() {
  if (!state.snap || document.hidden) return;
  let rem = BigInt(state.snap.remaining);
  if (!reduced.matches && !isStale()) {
    const elapsed = (performance.now() - state.snapAt) / 1000;
    const drift = BigInt(Math.floor(state.snap.rate * elapsed));
    rem = rem > drift ? rem - drift : 0n;
  }
  vault.set(rem.toString());
}

function renderChain() {
  if (state.snap) $("chain-val").textContent = formatUnit(state.snap.chain, state.mode) + (state.mode === "xno" ? " XNO" : " raw");
}

function sciNode(str) {
  const m = /^(\d(?:\.\d+)?)e(\d+)$/.exec(str);
  const b = document.createElement("b");
  if (!m) { b.textContent = str; return b; }
  b.append(`${m[1]} × 10`);
  const sup = document.createElement("sup");
  sup.textContent = m[2];
  b.append(sup);
  return b;
}

function renderStats() {
  const s = state.snap;
  $("st-clicks").textContent = withCommas(s.clicks);
  $("st-rate").textContent = s.rate.toLocaleString("en-US", { maximumFractionDigits: 1 });
  const out = $("st-drain");
  out.textContent = "";
  if (s.remaining === "0") {
    out.append("already. That was never supposed to happen, so the payout worker stopped itself.");
    return;
  }
  if (!s.drain_years) {
    out.append("never, at this rate. Nobody is clicking, so I'm not going anywhere.");
    return;
  }
  out.append("at this pace, about ", sciNode(s.drain_years), " years");
  if (s.universe_x) out.append(". That is ", sciNode(s.universe_x), " times the age of the universe");
  out.append(".");
}

function onSnapshot(s) {
  if (!/^\d{1,31}$/.test(s.remaining) || !/^\d{1,31}$/.test(s.chain) || !/^\d+$/.test(s.clicks) || typeof s.rate !== "number") return;
  const first = !state.snap;
  state.snap = s;
  state.snapAt = performance.now();
  // polled snapshots arrive while the stream is full, which stays the honest label until a slot opens
  if (!mockMode && state.conn !== "live" && state.conn !== "full") setConn("live");
  $("halt-banner").hidden = !s.halted;
  $("notice-banner").hidden = s.notice !== true;
  if (Number.isInteger(s.here) && s.here >= 0) $("st-here").textContent = withCommas(String(s.here));
  renderVault();
  renderChain();
  renderStats();
  if (first) announce();
}

// one debounced summary for assistive tech; the ticking digits themselves are not announced
function announce() {
  if (!state.snap) return;
  const xno = toXno(state.snap.remaining);
  const approx = xno.slice(0, 8);
  $("vault-digits").setAttribute("aria-label", `The reserve holds about ${approx} XNO, exactly ${withCommas(state.snap.remaining)} raw`);
  $("sr-summary").textContent = `The reserve holds about ${approx} XNO. ${withCommas(state.snap.clicks)} clicks so far.`;
}

/* ---------- budget: the VPS, as distinct from Nano (D97) ---------- */

function onBudget(b) {
  if (![b.cpu_milli, b.cpu_pct, b.mem_mb, b.mem_max_mb, b.queue].every(Number.isInteger)) return;
  const alloc = b.cpu_milli > 0 ? `${b.cpu_milli / 1000} CPU` : "";
  const parts = [];
  if (b.cpu_pct >= 0) parts.push(alloc ? `cpu ${b.cpu_pct}% of ${alloc}` : `cpu ${b.cpu_pct}%`);
  if (b.mem_mb >= 0) parts.push(b.mem_max_mb > 0 ? `${b.mem_mb} of ${b.mem_max_mb} MB` : `${b.mem_mb} MB`);
  parts.push(b.queue === 1 ? "1 payout in line" : `${b.queue} payouts in line`);
  $("st-budget").textContent = parts.join(" · ");
  $("st-budget-row").hidden = false;
  $("budget-live").textContent = parts.length > 1 ? ` (right now: ${parts.slice(0, -1).join(", ")})` : "";
}

/* ---------- address ---------- */

function curlBase() {
  return !mockMode && /^https?:/.test(location.protocol) ? location.origin : DEFAULT_HOST;
}

function renderCommands() {
  const a = state.addr || "nano_YOUR_ADDRESS";
  const base = curlBase();
  $("cmd-click").textContent = `curl -X POST ${base}/click/${a}`;
  $("cmd-loop").textContent = `while true; do curl -s -o /dev/null -X POST ${base}/click/${a}; done`;
  $("cmd-withdraw").textContent = `curl -X POST ${base}/withdraw/${a}`;
}

function setAddress(addr) {
  state.addr = addr;
  const form = $("addr-form");
  form.dataset.saved = String(Boolean(addr));
  document.querySelector(".play").dataset.saved = String(Boolean(addr));
  $("addr-saved").hidden = !addr;
  if (addr) $("addr-short").textContent = shortAddress(addr);
  orb.setLit(Boolean(addr));
  $("orb-help").textContent = addr ? "Tap, or hold. Take all night. I can take it." : "Paste your nano_ address below to light me up.";
  $("cashout").setAttribute("aria-disabled", String(!addr));
  renderCommands();
}

$("addr-form").addEventListener("submit", (e) => {
  e.preventDefault();
  const input = $("addr-input");
  const result = validateAddress(input.value);
  if (!result.ok) {
    input.setAttribute("aria-invalid", "true");
    say($("addr-msg"), result.reason);
    return;
  }
  const addr = input.value.trim();
  if (isVault(addr)) {
    input.setAttribute("aria-invalid", "true");
    say($("addr-msg"), "That's me. Paying me to myself goes nowhere, so paste your own address.");
    return;
  }
  input.removeAttribute("aria-invalid");
  store.set(ADDR_KEY, addr);
  say($("addr-msg"), "", "ok");
  setAddress(addr);
  $("orb").focus();
});

$("addr-input").addEventListener("input", () => {
  $("addr-input").removeAttribute("aria-invalid");
  say($("addr-msg"), "", "ok");
});

$("addr-change").addEventListener("click", () => {
  store.del(ADDR_KEY);
  setAddress(null);
  const input = $("addr-input");
  input.value = "";
  input.focus();
});

/* ---------- clicking and cashing out ---------- */

let mock = null;

function needAddress(byPointer = false) {
  say($("play-msg"), "I'm not letting you touch me until I know where to send it. Paste your address below.");
  const go = () => setTimeout(() => {
    if (state.addr) return; // a press that slid off the orb leaves this waiting; by the next click it may be moot
    const input = $("addr-input");
    input.focus({ preventScroll: true });
    input.scrollIntoView({ block: "center", behavior: reduced.matches ? "auto" : "smooth" });
  }, 0);
  // a tap focuses the orb itself when it ends, so the input takes focus only after that click
  if (byPointer) $("orb").addEventListener("click", go, { once: true });
  else go();
}

async function sendClick() {
  if (state.inFlight >= MAX_IN_FLIGHT) return;
  state.inFlight++;
  try {
    const res = mock ? await mock.click(state.addr) : await fetch(`/click/${encodeURIComponent(state.addr)}`, CLICK_INIT);
    if (!res.ok) throw Object.assign(new Error("status"), { status: res.status });
    state.mine++;
    $("mine-n").textContent = withCommas(String(state.mine));
    taunt(state.mine);
  } catch (err) {
    const now = Date.now();
    if (now - state.lastErrAt > 3000) {
      state.lastErrAt = now;
      if (err.status === 400) {
        say($("play-msg"), isVault(state.addr)
          ? "That's my own address. Paying me to myself goes nowhere: press change and paste yours."
          : "That address won't take. Press change and paste it again.");
      } else if (err.status === 503) {
        say($("play-msg"), "That one didn't count: I'm full up on new addresses, or resting for a moment. An address I already owe keeps counting. Try again in a bit.");
      } else {
        const why = err.status ? `The server answered ${err.status}.` : "I couldn't reach the server.";
        say($("play-msg"), `That one didn't land. ${why} Falling over is allowed. Your balance and the vault are safe.`);
      }
    }
  } finally {
    state.inFlight--;
  }
}

// the orb answers back at milestones of what this visitor has taken, on its help line for a few seconds, so the
// payout messages below are never overwritten
const TAUNTS = new Map([
  [100, "100 raw. I barely felt that."],
  [1000, "1,000 raw. Cute. There are a nonillion more."],
  [10000, "10,000. Your finger must hurt. I don't have one."],
  [100000, "100,000 raw. Still nothing missing from where I'm standing."],
  [1000000, "A million. Call it a dent. It isn't."],
]);
let tauntTimer = 0;

function taunt(n) {
  const line = TAUNTS.get(n);
  if (!line) return;
  const help = $("orb-help");
  help.textContent = line;
  clearTimeout(tauntTimer);
  tauntTimer = setTimeout(() => { help.textContent = state.addr ? "Tap, or hold. Take all night. I can take it." : help.textContent; }, 4000);
}

function fire(byPointer) {
  if (!state.addr) { needAddress(byPointer); return; }
  // a tiny tick per click where the device can (Android; iOS has no vibration API), never under reduced motion
  if (!reduced.matches && "vibrate" in navigator) navigator.vibrate(5);
  sendClick();
}

$("cashout").addEventListener("click", async () => {
  const msg = $("play-msg");
  if (!state.addr) { needAddress(); return; }
  try {
    const res = mock
      ? await mock.withdraw(state.addr)
      : await fetch(`/withdraw/${encodeURIComponent(state.addr)}`, { method: "POST", credentials: "omit", cache: "no-store" }).then(async (r) => ({ ok: r.ok, status: r.status, body: r.ok ? await r.json() : null }));
    if (!res.ok) throw Object.assign(new Error("status"), { status: res.status });
    const { raw, position, eta_s: etaS } = res.body;
    // the server decides the amount and zeroes the balance it sent, so the counter starts over
    state.mine = 0;
    $("mine-n").textContent = "0";
    state.waiting = { addr: state.addr, raw: BigInt(raw) };
    say(msg, `Queued. ${withCommas(String(raw))} raw, number ${position} in line.${waitText(etaS)} Watch "what's left me" for the block.`, "ok");
  } catch (err) {
    const why = { 400: "I don't pay that address. Press change and paste your own.",
      409: "There's nothing to send yet. Take something first.",
      503: "The payout line is full. Your balance stays credited, so try again in a bit." }[err.status];
    say(msg, why ?? (err.status ? `The server answered ${err.status}, so nothing was queued.` : "I couldn't reach the server, so nothing was queued."));
  }
});

/* ---------- payouts from before this visit ---------- */

// the latest payouts arrive once on connecting: listed, never announced, and never taken for the visitor's own
function onRecent(rows) {
  if (!Array.isArray(rows) || rows.length > 8) return;
  for (const p of rows) {
    feed.payout(p, { quiet: true });
    if (isPayout(p) && Number.isInteger(p.t)) state.lastPaid = Math.max(state.lastPaid, p.t);
  }
  renderAge();
}

/* ---------- most taken (D104) ---------- */

// the server truncates as the wire does: nano_ and five characters, an ellipsis, the last six
const SHORT_ADDR_RE = /^nano_[13][13456789abcdefghijkmnopqrstuwxyz]{4}…[13456789abcdefghijkmnopqrstuwxyz]{6}$/;

function onLeaders(rows) {
  if (!Array.isArray(rows) || rows.length > 10) return;
  if (!rows.every((r) => r && typeof r.a === "string" && SHORT_ADDR_RE.test(r.a) && /^\d{1,39}$/.test(r.raw))) return;
  const list = $("board");
  list.textContent = "";
  if (rows.length === 0) {
    const li = document.createElement("li");
    li.className = "empty";
    li.textContent = "nobody has been paid yet.";
    list.append(li);
    return;
  }
  for (const r of rows) {
    const li = document.createElement("li");
    const raw = document.createElement("span");
    raw.className = "raw";
    raw.textContent = `${withCommas(r.raw)} raw`;
    li.append(`${r.a} · `, raw);
    list.append(li);
  }
}

/* ---------- the vault, and how long since it last paid (D104) ---------- */

// the server refuses the vault's own address too; this only says so before a click is wasted
const isVault = (addr) => state.vault !== null && addr.slice(-60) === state.vault.slice(-60);

function onVault(v) {
  if (typeof v.a === "string" && validateAddress(v.a).ok) {
    state.vault = v.a;
    const link = $("proof-link");
    link.href = `https://blocklattice.io/account/${v.a}`;
    link.hidden = false;
  }
  if (Number.isInteger(v.last_paid) && v.last_paid > 0) state.lastPaid = Math.max(state.lastPaid, v.last_paid);
  renderAge();
}

function ago(ms) {
  const s = Math.max(0, Math.round(ms / 1000));
  if (s < 90) return `${s} s`;
  if (s < 90 * 60) return `${Math.round(s / 60)} min`;
  return `${Math.round(s / 3600)} h`;
}

function renderAge() {
  if (!state.lastPaid) return;
  $("pay-age-n").textContent = ago(Date.now() - state.lastPaid);
  $("pay-age").hidden = false;
}

// the wait is the server's estimate from how long payouts have lately taken
function waitText(etaS) {
  if (!Number.isInteger(etaS) || etaS <= 0) return "";
  return etaS < 90 ? ` About ${etaS} s.` : ` About ${Math.round(etaS / 60)} min.`;
}

// the queued message becomes the sent one when the stream announces this address's payout. A payout smaller than
// what was queued is an earlier send still landing; a larger one is this withdrawal merged with a later one.
function onPayout(p) {
  feed.payout(p);
  if (isPayout(p) && Number.isInteger(p.t)) {
    state.lastPaid = Math.max(state.lastPaid, p.t);
    renderAge();
  }
  const w = state.waiting;
  if (!w || !isPayout(p) || p.a !== w.addr || BigInt(p.raw) < w.raw) return;
  state.waiting = null;
  // your own payout landing: a short double buzz where the device can, never under reduced motion
  if (!reduced.matches && "vibrate" in navigator) navigator.vibrate([12, 60, 12]);
  const msg = $("play-msg");
  say(msg, `Sent. ${withCommas(p.raw)} raw, block `, "ok");
  msg.append(blockLink(p.hash), ".");
}

/* ---------- controls ---------- */

document.querySelectorAll('input[name="unit"]').forEach((r) => r.addEventListener("change", () => {
  state.mode = r.value;
  vault.setMode(state.mode);
  renderVault();
  renderChain();
}));

$("wire-pause").addEventListener("click", (e) => {
  const next = e.currentTarget.getAttribute("aria-pressed") !== "true";
  e.currentTarget.setAttribute("aria-pressed", String(next));
  e.currentTarget.textContent = next ? "resume" : "pause";
  feed.setPaused(next);
});

document.querySelectorAll("[data-copy]").forEach((btn) => btn.addEventListener("click", async () => {
  const code = $(btn.dataset.copy);
  try {
    await navigator.clipboard.writeText(code.textContent);
    say($("copy-msg"), "Copied.", "ok");
  } catch {
    getSelection().selectAllChildren(code);
    say($("copy-msg"), "Couldn't reach the clipboard. It's selected, so copy it by hand.");
  }
}));

/* ---------- start ---------- */

const handlers = { onSnapshot, onBudget, onVault, onLeaders, onRecent, onClick: (c) => feed.click(c), onPayout, onState: setConn };

setConn("connecting");
const saved = store.get(ADDR_KEY);
setAddress(saved && validateAddress(saved).ok ? saved : null);

// the demo source is fetched only when asked for, so real visitors never download it
if (mockMode) mock = (await import("./mock.js")).createMock(handlers);
else connectLive(handlers);

setInterval(renderVault, 100);
setInterval(() => { if (isStale() && state.conn === "live") setConn("stale"); }, 1000);
setInterval(announce, 30000);
setInterval(renderAge, 1000);
