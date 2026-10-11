// Package sysinfo reads the container's own resource allocation and use from its cgroup v2 files, so the page can
// show which waits are the VPS budget and which are Nano. It makes no request and runs no command; outside a
// container, or on a host without cgroup v2, it reports nothing.
package sysinfo

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Budget is one reading. Every field is an integer (D86 R3); -1 means unknown, 0 for a limit means unlimited.
type Budget struct {
	CPUMilli int64 // allocation in thousandths of a CPU
	CPUPct   int64 // use since the previous reading, as a percentage of the allocation
	MemBytes int64
	MemMax   int64
}

type Reader struct {
	root      string
	lastUsage int64 // cpu.stat usage_usec at the previous reading
	lastAt    time.Time
	now       func() time.Time
}

// New reads from root, which is /sys/fs/cgroup inside a container.
func New(root string) *Reader { return &Reader{root: root, now: time.Now} }

// Read takes a reading; ok is false when there is no cgroup to read. The first reading has no CPU use yet.
func (r *Reader) Read() (b Budget, ok bool) {
	mem, err := r.number("memory.current")
	if err != nil {
		return b, false
	}
	b = Budget{CPUMilli: -1, CPUPct: -1, MemBytes: mem, MemMax: -1}
	if v, err := r.number("memory.max"); err == nil {
		b.MemMax = v
	} else if r.text("memory.max") == "max" {
		b.MemMax = 0
	}
	// cpu.max is "quota period" in microseconds, or "max period" when unlimited
	if f := strings.Fields(r.text("cpu.max")); len(f) == 2 {
		period, perr := strconv.ParseInt(f[1], 10, 64)
		quota, qerr := strconv.ParseInt(f[0], 10, 64)
		switch {
		case perr == nil && period > 0 && f[0] == "max":
			b.CPUMilli = 0
		case perr == nil && qerr == nil && period > 0 && quota > 0:
			b.CPUMilli = quota * 1000 / period
		}
	}
	now := r.now()
	if usage, ok := r.usage(); ok {
		if !r.lastAt.IsZero() && b.CPUMilli != 0 {
			elapsed := now.Sub(r.lastAt).Microseconds()
			alloc := b.CPUMilli
			if alloc < 0 {
				alloc = 1000 // unknown allocation: report against one CPU
			}
			if elapsed > 0 {
				// used µs over available µs: (usage delta) / (elapsed × alloc/1000), as a percentage
				b.CPUPct = (usage - r.lastUsage) * 100 * 1000 / (elapsed * alloc)
			}
		}
		r.lastUsage, r.lastAt = usage, now
	}
	return b, true
}

func (r *Reader) usage() (int64, bool) {
	for _, line := range strings.Split(r.text("cpu.stat"), "\n") {
		if v, found := strings.CutPrefix(line, "usage_usec "); found {
			n, err := strconv.ParseInt(v, 10, 64)
			return n, err == nil
		}
	}
	return 0, false
}

func (r *Reader) number(name string) (int64, error) {
	return strconv.ParseInt(r.text(name), 10, 64)
}

// text reads a small cgroup file; the files are a line long, so a short limit is plenty
func (r *Reader) text(name string) string {
	f, err := os.Open(filepath.Join(r.root, name))
	if err != nil {
		return ""
	}
	defer f.Close()
	buf := make([]byte, 4096)
	n, _ := f.Read(buf)
	return strings.TrimSpace(string(buf[:n]))
}
