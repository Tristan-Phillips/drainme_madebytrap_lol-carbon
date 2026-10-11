# nanocrypto test data

Static vectors, generated once offline and committed (D62, D76). Nothing here is produced by the Go code under test.

- `vectors.json`: keys (seed, index, private, public, address), state block hashes and signatures, and work difficulties.
  Generated with an independent implementation: the RFC 8032 section 6 reference Ed25519 code with SHA-512 swapped
  for Blake2b-512 (Nano's variant), plus Python's `hashlib` Blake2b for derivation, addresses, block hashes and work.
  The all-zero seed entry also matches the widely published Nano vector
  (`nano_3i1aq1cchnmbn9x5rsbap8b15akfh7wj7pwskuzi7ahz8oq6cobd99d4r3b7`).
  The two valid `work` values were returned by live mainnet `work_generate` endpoints on 2026-10-03.
- `chain_blocks.json`: four real mainnet state blocks (two sends, two receives) fetched via `block_info` on
  2026-10-03. Their hashes, signatures and work were checked against the same reference implementation before
  being committed, so they tie the vectors to what the network itself accepted.

These are public chain data and throwaway test seeds. No real key material is in this directory.
