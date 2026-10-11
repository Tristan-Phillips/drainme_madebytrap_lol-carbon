// Live data over SSE (GET /events). Contract locked in docs/DECISIONS.md D90:
//   event: snapshot  data: {"remaining":"<raw>","chain":"<raw>","unpaid":"<raw>","clicks":"<n>",
//                           "rate":<clicks per second, 60 s window>,"drain_years":"7.7e19"|null,
//                           "universe_x":"5.6e9"|null,"halted":false,"here":<open streams>,
//                           "notice":true (only while Cloudflare is challenging bots)}   about once a second
//   event: click     data: {"a":"nano_...","t":<epoch ms>}                 sampled, <= ~10/s
//   event: payout    data: {"a":"nano_...","raw":"<raw>","hash":"<64 hex>","t":<epoch ms>}
//   event: budget    data: {"cpu_milli":500,"cpu_pct":12,"mem_mb":31,"mem_max_mb":256,"queue":0}
//                    the container's allocation and use, every 10 s and on connect (D97); -1 unknown, 0 unlimited
//   event: vault     data: {"a":"nano_...","last_paid":<epoch ms, absent if none>}   once, on connecting (D104)
//   event: leaders   data: [{"a":"nano_1abcd…wxyz12","raw":"<raw paid, all time>"}, …]   up to 10, most first;
//                    on connecting and whenever a payout changes it (D104)
//   event: recent    data: [payout objects as above, oldest first, up to 8]   once, on connecting: list them,
//                    announce nothing
//   event: full      data: {}   server is at its viewer cap; it then closes the stream, and the page polls
//                    GET /snapshot (the snapshot JSON) every 30 s until a stream slot opens (D104)
// HTTP routes are locked in docs/DECISIONS.md D87:
// POST /click/{address} replies 204; 400 bad address; 503 no room for a new address, vault halted, or shutting down.
// POST /withdraw/{address} replies {"raw":"<raw>","position":<n>,"eta_s":<estimated wait>} with the whole unsent balance, which the server
// zeroes; 409 when there is nothing to send, 503 when the payout line is full (balance stays credited).
// Reconnects are manual so the backoff can be jittered, which EventSource's built-in retry is not.
// A tab hidden for HIDDEN_CLOSE_MS closes its stream, freeing a viewer slot (D34) and the per-second update,
// and reopens it the moment it is visible again; the page shows "stale" meanwhile, which is true.

const HIDDEN_CLOSE_MS = 30000;
const POLL_MS = 30000;

export function connectLive(handlers) {
  let es = null;
  let timer = null;
  let hiddenTimer = null;
  let attempt = 0;
  let poll = null; // while the stream is full: the snapshot every POLL_MS, so the numbers still move

  function startPolling() {
    if (poll) return;
    const once = () => {
      if (document.hidden) return;
      fetch("/snapshot", { credentials: "omit" })
        .then((r) => (r.ok ? r.json() : null))
        .then((s) => { if (s && !es) handlers.onSnapshot(s); })
        .catch(() => { /* the next poll tries again */ });
    };
    once();
    poll = setInterval(once, POLL_MS);
  }
  const stopPolling = () => { clearInterval(poll); poll = null; };

  document.addEventListener("visibilitychange", () => {
    clearTimeout(hiddenTimer);
    if (document.hidden) {
      hiddenTimer = setTimeout(() => { clearTimeout(timer); if (es) { es.close(); es = null; } }, HIDDEN_CLOSE_MS);
    } else if (!es) {
      clearTimeout(timer); // a pending reconnect would otherwise open a second stream
      attempt = 0;
      open();
    }
  });

  const parse = (fn) => (e) => {
    try { fn(JSON.parse(e.data)); } catch { /* a malformed event is dropped, never fatal */ }
  };

  function schedule(ms) {
    clearTimeout(timer);
    timer = setTimeout(() => { if (!document.hidden) open(); }, ms);
  }

  function open() {
    es = new EventSource("/events");
    es.addEventListener("open", () => { attempt = 0; stopPolling(); handlers.onState("live"); });
    es.addEventListener("snapshot", parse(handlers.onSnapshot));
    es.addEventListener("click", parse(handlers.onClick));
    es.addEventListener("payout", parse(handlers.onPayout));
    es.addEventListener("budget", parse(handlers.onBudget));
    es.addEventListener("vault", parse(handlers.onVault));
    es.addEventListener("leaders", parse(handlers.onLeaders));
    es.addEventListener("recent", parse(handlers.onRecent));
    es.addEventListener("full", () => {
      es.close();
      es = null;
      handlers.onState("full");
      startPolling();
      schedule(30000 + Math.random() * 30000);
    });
    es.addEventListener("error", () => {
      es.close();
      es = null;
      handlers.onState("stale");
      const base = Math.min(30000, 1000 * 2 ** attempt++);
      schedule(base * (0.5 + Math.random() * 0.5));
    });
  }

  open();
  return { close() { clearTimeout(timer); stopPolling(); if (es) es.close(); } };
}
