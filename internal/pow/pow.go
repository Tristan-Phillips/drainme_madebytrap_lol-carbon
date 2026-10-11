// Package pow finds Nano proof of work on the CPU, for the local fallback work server (D104). A work value's
// difficulty is BLAKE2b with an 8-byte digest over the work (8 bytes, little-endian) and the 32-byte root, read
// as a little-endian uint64. Each thread reuses one x/crypto hasher: measured, that is faster than a hand-unrolled
// pure-Go compression, because x/crypto's own assembly beats it (D104 note), so no custom crypto is involved.
package pow

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/nanocrypto"
	"golang.org/x/crypto/blake2b"
)

// Solve searches for work on root that reaches threshold, on threads goroutines, until it finds one or ctx ends.
// Each thread starts at its own random point, so threads and repeated searches do not repeat each other.
func Solve(ctx context.Context, root [32]byte, threshold uint64, threads int) (uint64, error) {
	if threads < 1 {
		return 0, errors.New("pow: need at least one thread")
	}
	var found atomic.Bool
	var result uint64
	var once sync.Once
	var wg sync.WaitGroup
	for t := 0; t < threads; t++ {
		var seed [8]byte
		if _, err := rand.Read(seed[:]); err != nil {
			return 0, err
		}
		wg.Add(1)
		go func(w uint64) {
			defer wg.Done()
			h, _ := blake2b.New(8, nil) // only fails for a bad size or key
			buf := make([]byte, 40)
			copy(buf[8:], root[:])
			out := make([]byte, 0, 8)
			for {
				// a winner or a cancellation is noticed within 2^16 attempts: rare enough to cost nothing
				for i := 0; i < 1<<16; i++ {
					binary.LittleEndian.PutUint64(buf, w)
					h.Reset()
					h.Write(buf)
					out = h.Sum(out[:0])
					if binary.LittleEndian.Uint64(out) >= threshold {
						once.Do(func() { result = w; found.Store(true) })
						return
					}
					w++
				}
				if found.Load() || ctx.Err() != nil {
					return
				}
			}
		}(binary.LittleEndian.Uint64(seed[:]))
	}
	wg.Wait()
	if !found.Load() {
		return 0, ctx.Err()
	}
	// the answer is checked by the one definition the rest of drainme uses, before anyone sees it
	if nanocrypto.Difficulty(root, result) < threshold {
		return 0, errors.New("pow: found work does not verify")
	}
	return result, nil
}
