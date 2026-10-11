// Reading the seed file. The seed is the only secret drainme holds; it never appears in errors or logs.
package config

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

// a seed file is 64 hex chars plus maybe a newline; anything much bigger is the wrong file
const maxSeedFileBytes = 256

// ErrSeedFormat means the file was readable but does not contain exactly one 64-hex-digit seed.
var ErrSeedFormat = errors.New("config: seed file must contain exactly 64 hex digits")

// ReadSeed loads the 32-byte seed from path. Errors describe the shape of the problem, never the content.
// Zeroing the returned array afterwards is best effort only: Go may have copied it, and the real
// boundary is process and user isolation (stack-pitfalls P4).
func ReadSeed(path string) ([32]byte, error) {
	var seed [32]byte

	f, err := os.Open(path)
	if err != nil {
		return seed, fmt.Errorf("config: opening seed file: %w", err)
	}
	defer f.Close()

	buf, err := io.ReadAll(io.LimitReader(f, maxSeedFileBytes+1))
	if err != nil {
		return seed, fmt.Errorf("config: reading seed file: %w", err)
	}
	defer clear(buf)
	if len(buf) > maxSeedFileBytes {
		return seed, fmt.Errorf("%w (file is larger than %d bytes)", ErrSeedFormat, maxSeedFileBytes)
	}

	text := strings.TrimSpace(string(buf))
	if len(text) != 64 {
		return seed, fmt.Errorf("%w (found %d characters)", ErrSeedFormat, len(text))
	}
	if _, err := hex.Decode(seed[:], []byte(text)); err != nil {
		clear(seed[:])
		return seed, fmt.Errorf("%w (contains a non-hex character)", ErrSeedFormat)
	}
	return seed, nil
}
