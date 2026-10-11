package sysinfo

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func cgroup(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// a container like drainme-app: 0.5 CPU, 256 MB
func TestReadsTheAllocationAndUse(t *testing.T) {
	dir := cgroup(t, map[string]string{
		"cpu.max":        "50000 100000\n",
		"cpu.stat":       "usage_usec 1000000\nuser_usec 800000\n",
		"memory.current": "33554432\n",
		"memory.max":     "268435456\n",
	})
	r := New(dir)
	clock := time.Unix(1000, 0)
	r.now = func() time.Time { return clock }
	b, ok := r.Read()
	if !ok || b.CPUMilli != 500 || b.MemBytes != 33554432 || b.MemMax != 268435456 || b.CPUPct != -1 {
		t.Fatalf("first reading %+v %v", b, ok)
	}
	// 10 s later, 1 s of CPU used: half a CPU over 10 s is 5 s available, so 20 % of the allocation
	os.WriteFile(filepath.Join(dir, "cpu.stat"), []byte("usage_usec 2000000\n"), 0o600)
	clock = clock.Add(10 * time.Second)
	if b, _ = r.Read(); b.CPUPct != 20 {
		t.Fatalf("cpu %d%%, want 20", b.CPUPct)
	}
}

func TestUnlimitedAndMissing(t *testing.T) {
	r := New(cgroup(t, map[string]string{"cpu.max": "max 100000", "memory.current": "5", "memory.max": "max"}))
	if b, ok := r.Read(); !ok || b.CPUMilli != 0 || b.MemMax != 0 || b.CPUPct != -1 {
		t.Fatalf("unlimited %+v %v", b, ok)
	}
	if _, ok := New(t.TempDir()).Read(); ok {
		t.Fatal("a directory with no cgroup files read as a container")
	}
	r = New(cgroup(t, map[string]string{"cpu.max": "garbage", "memory.current": "7", "memory.max": "nope"}))
	if b, ok := r.Read(); !ok || b.CPUMilli != -1 || b.MemMax != -1 {
		t.Fatalf("malformed files %+v %v", b, ok)
	}
}
