// Demo data source, used only with ?mock=1 and always labelled as demo on the page.
// It speaks the same shape as the SSE events so main.js cannot tell the difference, and it keeps one consistent
// set of books: paid <= credited = clicks, reserve = vault - credited, chain = vault - paid.
// A "demo controls" panel drives it, so every state the page can show is reachable without a server.

const VAULT = 10n ** 30n;
const SECONDS_PER_YEAR = 31557600;
const UNIVERSE_YEARS = 1.38e10;
const B32 = "13456789abcdefghijkmnopqrstuwxyz";

const sci = (n) => n.toExponential(2).replace("e+", "e");
const pick = (chars, n) => Array.from({ length: n }, () => chars[Math.floor(Math.random() * chars.length)]).join("");
const fakeAddress = () => `nano_${pick("13", 1)}${pick(B32, 59)}`; // looks right, checksum is fake
const fakeHash = () => pick("0123456789ABCDEF", 64);
// a real, valid address (the well-known burn address), so the demo's "check the vault" link has something to show
const DEMO_VAULT = "nano_1111111111111111111111111111111111111111111111111111hifc8npp";

// crowd rate slider: position 0 is nobody, then one decade per 10 steps up to 10^12 clicks a second
const rateAt = (pos) => (pos === 0 ? 0 : Math.round(10 ** (pos / 10)));
const posOf = (rate) => (rate < 1 ? 0 : Math.round(10 * Math.log10(rate)));

export function createMock(handlers) {
  const sim = { rate: 420, wander: true, payEvery: 7, payDelay: 3, feedPerSec: 10, halted: false, notice: false, here: 37, stream: "demo", click: "ok", withdraw: "ok" };
  let credited = 48210n; // every demo click is credited, so this is also the click count
  let paid = 20117n;
  let carry = 0; // fractional clicks between ticks, so slow rates still add up
  const owedTo = new Map(); // the visitor's own unsent clicks per address, as the ledger keeps them
  const unpaid = () => credited - paid;
  const live = () => sim.stream === "demo";
  const timers = [];

  // each loop re-reads its period from sim, so a slider change applies on the next round
  function loop(periodMs, fn) {
    const run = () => {
      const ms = periodMs();
      if (ms > 0 && live()) fn();
      timers.push(setTimeout(run, ms > 0 ? ms : 500));
    };
    run();
  }

  function snapshot() {
    if (sim.wander) sim.rate = Math.max(0, sim.rate * (0.85 + Math.random() * 0.3));
    carry += sim.rate;
    const add = BigInt(Math.floor(carry));
    carry -= Number(add);
    credited = credited + add > VAULT ? VAULT : credited + add; // clicks stop counting at zero, as the ledger refuses them
    const remaining = VAULT - credited;
    const years = sim.rate > 0 && remaining > 0n ? Number(remaining) / sim.rate / SECONDS_PER_YEAR : null;
    handlers.onSnapshot({
      remaining: remaining.toString(),
      chain: (VAULT - paid).toString(),
      unpaid: unpaid().toString(),
      clicks: credited.toString(),
      rate: Math.round(sim.rate * 10) / 10,
      drain_years: years === null ? null : sci(years),
      universe_x: years === null ? null : sci(years / UNIVERSE_YEARS),
      halted: sim.halted || remaining === 0n, // an empty reserve is the halt case (D20)
      here: sim.here,
      ...(sim.notice ? { notice: true } : {}),
    });
    panel?.sync();
  }

  // a payout can only be a share of what is owed, never more
  function payout(addr = fakeAddress(), raw = null) {
    const owed = unpaid();
    if (owed <= 0n) return;
    let amt = raw ?? BigInt(1 + Math.floor(Math.random() * Number(owed < 5000n ? owed : 5000n)));
    if (amt > owed) amt = owed;
    paid += amt;
    handlers.onPayout({ a: addr, raw: amt.toString(), hash: fakeHash(), t: Date.now() });
  }

  const reset = () => {
    Object.assign(sim, { rate: 420, wander: true, payEvery: 7, payDelay: 3, feedPerSec: 10, halted: false, notice: false, here: 37, click: "ok", withdraw: "ok" });
    credited = 48210n;
    paid = 20117n;
    owedTo.clear();
    setStream("demo");
    snapshot();
  };
  const setStream = (s) => {
    sim.stream = s;
    handlers.onState(s);
  };
  const drainTo = (left) => {
    credited = VAULT - left;
    if (paid > credited) paid = credited;
    snapshot();
  };

  const panel = buildPanel(sim, { reset, setStream, drainTo, snapshot });

  snapshot();
  handlers.onRecent(Array.from({ length: 5 }, (_, i) => ({ a: fakeAddress(), raw: String(1 + Math.floor(Math.random() * 5000)), hash: fakeHash(), t: Date.now() - (5 - i) * 9000 })));
  handlers.onVault({ a: DEMO_VAULT, last_paid: Date.now() - 42000 });
  const short = (a) => `${a.slice(0, 10)}…${a.slice(-6)}`;
  handlers.onLeaders(Array.from({ length: 10 }, (_, i) => ({ a: short(fakeAddress()), raw: String(Math.round(90000 / (i + 1))) })));
  loop(() => 1000, snapshot);
  loop(() => (sim.feedPerSec > 0 ? 1000 / sim.feedPerSec : 0), () => handlers.onClick({ a: fakeAddress(), t: Date.now() }));
  loop(() => sim.payEvery * 1000, () => payout());
  // the container readout, as the server sends it every 10 s (D97); the numbers are invented
  const budget = () => handlers.onBudget({ cpu_milli: 500, cpu_pct: Math.min(100, Math.round(sim.rate / 60)), mem_mb: 28 + Math.floor(Math.random() * 6), mem_max_mb: 256, queue: Math.floor(Math.random() * 3) });
  budget();
  loop(() => 10000, budget);
  handlers.onState("demo");

  // the visitor's own requests answer as the panel says the server would
  const answer = (mode) => {
    if (mode === "offline") throw new TypeError("demo: server unreachable");
    return { ok: false, status: Number(mode), body: null };
  };

  return {
    async click(addr) {
      if (sim.click !== "ok") return answer(sim.click);
      if (credited >= VAULT) return { ok: false, status: 503, body: null };
      credited += 1n;
      owedTo.set(addr, (owedTo.get(addr) ?? 0n) + 1n);
      return { ok: true, status: 200 };
    },
    // like the ledger: the whole unsent balance is taken and zeroed at once, and an empty balance sends nothing
    async withdraw(addr) {
      if (sim.withdraw !== "ok") return answer(sim.withdraw);
      const amt = owedTo.get(addr) ?? 0n;
      if (amt === 0n) return { ok: false, status: 409, body: null };
      owedTo.delete(addr);
      setTimeout(() => payout(addr, amt), sim.payDelay * 1000); // the visitor's own payout lands a little later
      const position = 1 + Math.floor(Math.random() * 5);
      return { ok: true, status: 200, body: { raw: amt.toString(), position, eta_s: position * 4 } };
    },
    close() {
      timers.forEach(clearTimeout);
      panel.remove();
    },
  };
}

/* ---------- demo controls: test knobs, never shown without ?mock=1 ---------- */

function buildPanel(sim, act) {
  // every node is built from constant text and properties, never from markup, so nothing here parses HTML
  const el = (tag, props = {}, ...kids) => {
    const node = Object.assign(document.createElement(tag), props);
    node.append(...kids);
    return node;
  };

  function slider(label, min, max, get, set, show) {
    const out = el("output");
    const input = el("input", { type: "range", min, max, step: 1 });
    const sync = () => { input.value = String(get()); out.textContent = show(); };
    input.addEventListener("input", () => { set(Number(input.value)); sync(); });
    sync();
    return { row: el("label", { className: "mockctl__row" }, el("span", {}, label), out, input), sync };
  }
  function choice(label, options, get, set) {
    const select = el("select", {}, ...options.map(([value, text]) => el("option", { value, textContent: text })));
    select.value = get();
    select.addEventListener("change", () => set(select.value));
    return { row: el("label", { className: "mockctl__row" }, el("span", {}, label), select), sync: () => { select.value = get(); } };
  }
  function toggle(label, get, set) {
    const box = el("input", { type: "checkbox" });
    box.checked = get();
    box.addEventListener("change", () => set(box.checked));
    return { row: el("label", { className: "mockctl__row mockctl__row--check" }, box, el("span", {}, label)), sync: () => { box.checked = get(); } };
  }
  const button = (text, fn) => {
    const b = el("button", { type: "button", className: "btn btn--ghost btn--small", textContent: text });
    b.addEventListener("click", fn);
    return b;
  };

  const answers = [["ok", "200 ok"], ["409", "409 nothing to send"], ["503", "503 full"], ["500", "500 error"], ["offline", "unreachable"]];
  const controls = [
    slider("crowd clicks/s", 0, 120, () => posOf(sim.rate), (p) => { sim.rate = rateAt(p); sim.wander = false; }, () => Math.round(sim.rate).toLocaleString("en-US")),
    toggle("let the crowd wander", () => sim.wander, (v) => { sim.wander = v; }),
    slider("stranger payout every", 0, 60, () => sim.payEvery, (v) => { sim.payEvery = v; }, () => (sim.payEvery ? `${sim.payEvery} s` : "off")),
    slider("your payout lands after", 0, 30, () => sim.payDelay, (v) => { sim.payDelay = v; }, () => `${sim.payDelay} s`),
    slider("wire clicks shown/s", 0, 20, () => sim.feedPerSec, (v) => { sim.feedPerSec = v; }, () => (sim.feedPerSec ? `${sim.feedPerSec}` : "off")),
    toggle("payout worker halted", () => sim.halted, (v) => { sim.halted = v; act.snapshot(); }),
    toggle("bots being checked (notice)", () => sim.notice, (v) => { sim.notice = v; act.snapshot(); }),
    choice("stream", [["demo", "live (demo)"], ["stale", "stale"], ["full", "stream full"], ["connecting", "connecting"]], () => sim.stream, act.setStream),
    choice("/click answers", answers.filter(([v]) => v !== "409"), () => sim.click, (v) => { sim.click = v; }),
    choice("/withdraw answers", answers, () => sim.withdraw, (v) => { sim.withdraw = v; }),
  ];

  const root = el("details", { className: "mockctl" },
    el("summary", { textContent: "demo controls" }),
    el("div", { className: "mockctl__body" },
      ...controls.map((c) => c.row),
      el("div", { className: "mockctl__buttons" },
        button("leave 1,000 raw", () => act.drainTo(1000n)),
        button("reset", act.reset))));
  document.body.append(root);

  return {
    sync() { if (root.open) controls.forEach((c) => c.sync()); },
    remove() { root.remove(); },
  };
}
