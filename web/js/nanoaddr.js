// Client-side nano_ address validation: format plus the real Blake2b-40 checksum.
// The server re-validates; this only exists so a typo is caught before the first click.

const ALPHABET = "13456789abcdefghijkmnopqrstuwxyz";
const MASK = (1n << 64n) - 1n;

const IV = [
  0x6a09e667f3bcc908n, 0xbb67ae8584caa73bn, 0x3c6ef372fe94f82bn, 0xa54ff53a5f1d36f1n,
  0x510e527fade682d1n, 0x9b05688c2b3e6c1fn, 0x1f83d9abfb41bd6bn, 0x5be0cd19137e2179n,
];

const SIGMA = [
  [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
  [14, 10, 4, 8, 9, 15, 13, 6, 1, 12, 0, 2, 11, 7, 5, 3],
  [11, 8, 12, 0, 5, 2, 15, 13, 10, 14, 3, 6, 7, 1, 9, 4],
  [7, 9, 3, 1, 13, 12, 11, 14, 2, 6, 5, 10, 4, 0, 15, 8],
  [9, 0, 5, 7, 2, 4, 10, 15, 14, 1, 11, 12, 6, 8, 3, 13],
  [2, 12, 6, 10, 0, 11, 8, 3, 4, 13, 7, 5, 15, 14, 1, 9],
  [12, 5, 1, 15, 14, 13, 4, 10, 0, 7, 6, 3, 9, 2, 8, 11],
  [13, 11, 7, 14, 12, 1, 3, 9, 5, 0, 15, 4, 8, 6, 2, 10],
  [6, 15, 14, 9, 11, 3, 0, 8, 12, 2, 13, 7, 1, 4, 10, 5],
  [10, 2, 8, 4, 7, 6, 1, 5, 15, 11, 9, 14, 3, 12, 13, 0],
];

const rotr = (x, n) => ((x >> n) | (x << (64n - n))) & MASK;

// Blake2b of a single block (input <= 128 bytes, no key), truncated to outLen bytes.
function blake2b(input, outLen) {
  const h = IV.slice();
  h[0] ^= 0x01010000n ^ BigInt(outLen);

  const block = new Uint8Array(128);
  block.set(input);
  const m = [];
  for (let i = 0; i < 16; i++) {
    let w = 0n;
    for (let b = 7; b >= 0; b--) w = (w << 8n) | BigInt(block[i * 8 + b]);
    m.push(w);
  }

  const v = [...h, ...IV];
  v[12] ^= BigInt(input.length);
  v[14] ^= MASK;

  const g = (a, b, c, d, x, y) => {
    v[a] = (v[a] + v[b] + x) & MASK; v[d] = rotr(v[d] ^ v[a], 32n);
    v[c] = (v[c] + v[d]) & MASK;     v[b] = rotr(v[b] ^ v[c], 24n);
    v[a] = (v[a] + v[b] + y) & MASK; v[d] = rotr(v[d] ^ v[a], 16n);
    v[c] = (v[c] + v[d]) & MASK;     v[b] = rotr(v[b] ^ v[c], 63n);
  };
  for (let r = 0; r < 12; r++) {
    const s = SIGMA[r % 10];
    g(0, 4, 8, 12, m[s[0]], m[s[1]]);
    g(1, 5, 9, 13, m[s[2]], m[s[3]]);
    g(2, 6, 10, 14, m[s[4]], m[s[5]]);
    g(3, 7, 11, 15, m[s[6]], m[s[7]]);
    g(0, 5, 10, 15, m[s[8]], m[s[9]]);
    g(1, 6, 11, 12, m[s[10]], m[s[11]]);
    g(2, 7, 8, 13, m[s[12]], m[s[13]]);
    g(3, 4, 9, 14, m[s[14]], m[s[15]]);
  }

  const out = new Uint8Array(outLen);
  for (let i = 0; i < outLen; i++) {
    const word = h[i >> 3] ^ v[i >> 3] ^ v[(i >> 3) + 8];
    out[i] = Number((word >> BigInt(8 * (i & 7))) & 0xffn);
  }
  return out;
}

function decode(str) {
  let n = 0n;
  for (const ch of str) n = (n << 5n) | BigInt(ALPHABET.indexOf(ch));
  return n;
}

// Returns { ok: true } or { ok: false, reason } where reason is a plain-text explanation.
export function validateAddress(raw) {
  const addr = raw.trim();
  if (!addr) return { ok: false, reason: "Give me an address first." };
  if (!addr.startsWith("nano_")) {
    return { ok: false, reason: "That isn't a nano_ address. The old xrb_ prefix is retired, and I only take nano_." };
  }
  if (addr.length !== 65) {
    return { ok: false, reason: `A nano_ address is 65 characters long. This one is ${addr.length}.` };
  }
  const body = addr.slice(5);
  for (const ch of body) {
    if (!ALPHABET.includes(ch)) {
      return { ok: false, reason: `"${ch}" isn't a character Nano uses. Addresses never contain 0, 2, l or v, and are lowercase.` };
    }
  }
  if (body[0] !== "1" && body[0] !== "3") {
    return { ok: false, reason: "The first character after nano_ must be a 1 or a 3. This isn't a valid key." };
  }

  const key = decode(body.slice(0, 52)); // 260 bits, top 4 are padding
  const keyBytes = new Uint8Array(32);
  let k = key;
  for (let i = 31; i >= 0; i--) { keyBytes[i] = Number(k & 0xffn); k >>= 8n; }

  const digest = blake2b(keyBytes, 5);
  let expected = 0n;
  for (let i = 4; i >= 0; i--) expected = (expected << 8n) | BigInt(digest[i]);

  if (expected !== decode(body.slice(52))) {
    return { ok: false, reason: "That address fails its checksum, so a character is wrong somewhere. Check it before I send you anything." };
  }
  return { ok: true };
}

// first 10 and last 6 characters, as the payouts feed shows them
export function shortAddress(addr) {
  return addr.length > 20 ? `${addr.slice(0, 10)}…${addr.slice(-6)}` : addr;
}
