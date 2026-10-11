package pow

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/nanocrypto"
)

func TestSolve(t *testing.T) {
	var root [32]byte
	rand.Read(root[:])
	const threshold = 0xfff0000000000000 // about 4,096 tries
	for threads := 1; threads <= 4; threads++ {
		w, err := Solve(context.Background(), root, threshold, threads)
		if err != nil || nanocrypto.Difficulty(root, w) < threshold {
			t.Fatalf("%d threads: work %016x: %v", threads, w, err)
		}
	}
	// distinct searches start at random points, so they do not keep returning the same answer
	a, _ := Solve(context.Background(), root, threshold, 1)
	b, _ := Solve(context.Background(), root, threshold, 1)
	if a == b {
		t.Fatal("two searches returned the same work; the starting points are not random")
	}
}

func TestSolveStops(t *testing.T) {
	var root [32]byte
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := Solve(ctx, root, ^uint64(0), 2); err == nil {
		t.Fatal("an impossible threshold was met")
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("Solve ignored its deadline")
	}
	if _, err := Solve(context.Background(), root, 1, 0); err == nil {
		t.Fatal("zero threads accepted")
	}
}
