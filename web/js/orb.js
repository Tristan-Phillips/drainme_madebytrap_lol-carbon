// The orb button: pointer tap and hold-to-repeat, keyboard (Enter repeats, Space on release), assistive-tech clicks.
// It only reports "one click happened", and whether a pointer did it; what a click does is main.js's business.

const HOLD_DELAY_MS = 380;
const REPEAT_MS = 85;
const RIPPLE_GAP_MS = 110;
const MAX_RIPPLES = 6;
const GLITCH_GAP_MS = 350;

export function createOrb({ button, wrap, onFire }) {
  const reduced = matchMedia("(prefers-reduced-motion: reduce)");
  let delay = null;
  let repeat = null;
  let lastRipple = 0;
  let lastGlitch = 0;
  let lit = false;

  function flair() {
    if (reduced.matches) return;
    const now = performance.now();
    // a held orb fires ~12 times a second; the glitch is capped near 3/s to stay under flash limits
    if (now - lastGlitch >= GLITCH_GAP_MS) {
      lastGlitch = now;
      button.dataset.glitch = "true";
      setTimeout(() => { button.dataset.glitch = "false"; }, 180);
    }

    if (now - lastRipple < RIPPLE_GAP_MS || button.querySelectorAll(".ripple").length >= MAX_RIPPLES) return;
    lastRipple = now;
    const r = document.createElement("i");
    r.className = "ripple";
    r.setAttribute("aria-hidden", "true");
    r.addEventListener("animationend", () => r.remove(), { once: true });
    button.append(r);
  }

  function fire(byPointer = false) {
    if (lit) flair();
    onFire(byPointer);
  }

  function stopHold() {
    clearTimeout(delay);
    clearInterval(repeat);
    button.dataset.down = "false";
  }

  button.addEventListener("pointerdown", (e) => {
    if (e.button !== 0) return;
    // capture keeps a hold going when the finger drifts; a pointer already gone throws, which must not cost the click
    try { button.setPointerCapture(e.pointerId); } catch { /* no capture, still a click */ }
    button.dataset.down = "true";
    fire(true);
    // an unlit orb answers once; holding it must not repeat the same complaint a dozen times a second
    if (lit) delay = setTimeout(() => { repeat = setInterval(fire, REPEAT_MS); }, HOLD_DELAY_MS);
  });
  for (const type of ["pointerup", "pointercancel", "lostpointercapture"]) button.addEventListener(type, stopHold);
  button.addEventListener("blur", stopHold);
  button.addEventListener("contextmenu", (e) => e.preventDefault());

  button.addEventListener("keydown", (e) => {
    if (e.key === "Enter") { e.preventDefault(); button.dataset.down = "true"; fire(); }
    else if (e.key === " ") e.preventDefault();
  });
  button.addEventListener("keyup", (e) => {
    if (e.key === "Enter") button.dataset.down = "false";
    else if (e.key === " ") { e.preventDefault(); fire(); }
  });

  // pointer and keyboard are handled above; a click with no detail is a screen reader or switch activating the button
  button.addEventListener("click", (e) => { if (e.detail === 0) fire(); });

  return {
    setLit(on) {
      lit = on;
      button.setAttribute("aria-disabled", on ? "false" : "true");
      wrap.dataset.lit = String(on);
      button.querySelector(".orb__label").textContent = on ? "drain" : "address?";
    },
  };
}
