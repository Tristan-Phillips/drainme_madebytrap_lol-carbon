// The 31-digit vault readout. Always 31 fixed slots so the layout never shifts as the number falls.
// Values are decimal strings end to end; nothing here ever becomes a float.

const SIZES = [1, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3]; // 31 digits, grouped in threes from the right
const LIVE_FROM = 19; // the last 12 digits are the ones that actually move

export const SLOTS = 31;

export function createVaultDigits(root) {
  let mode = "raw";
  let cells = [];

  function build() {
    root.textContent = "";
    root.dataset.waiting = "true";
    cells = [];
    SIZES.forEach((n, gi) => {
      const group = document.createElement("span");
      group.className = "g";
      for (let i = 0; i < n; i++) {
        const d = document.createElement("span");
        d.className = cells.length >= LIVE_FROM ? "d live" : "d";
        d.style.setProperty("--i", cells.length);
        d.textContent = "·";
        cells.push(d);
        group.append(d);
      }
      if (mode === "xno" && gi === 0) {
        const pt = document.createElement("span");
        pt.className = "pt";
        pt.textContent = ".";
        group.append(pt);
      }
      root.append(group);
      if (gi === 5) {
        const br = document.createElement("span");
        br.className = "br";
        root.append(br);
      }
    });
  }

  function set(raw) {
    root.dataset.waiting = "false";
    const s = raw.padStart(SLOTS, "0");
    const first = mode === "raw" ? Math.min(s.search(/[1-9]/) === -1 ? SLOTS - 1 : s.search(/[1-9]/), LIVE_FROM) : 0;
    for (let i = 0; i < SLOTS; i++) {
      const c = cells[i];
      if (c.textContent !== s[i]) c.textContent = s[i];
      c.classList.toggle("z", i < first);
      c.classList.toggle("hi", i >= first && i < first + 3 && i < LIVE_FROM);
    }
  }

  function setMode(next) {
    mode = next;
    build();
  }

  build();
  return { set, setMode };
}

// "1000000000000000000000000000000" -> "1,000,000,000,000,000,000,000,000,000,000"
export function withCommas(digits) {
  return digits.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

// exact raw -> XNO as a 30-decimal string, no rounding
export function toXno(raw) {
  const s = raw.padStart(SLOTS, "0");
  return `${s[0]}.${s.slice(1)}`;
}

export function formatUnit(raw, mode) {
  return mode === "xno" ? toXno(raw) : withCommas(raw);
}
