// Package policy holds repository-wide rules that are cheaper to prove by test than to review by eye.
package policy

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// R3: money in Go is raw-only exact integers. No float may exist, and no XNO unit, so a factor-10^30 conversion
// slip on the payout path is impossible rather than merely unlikely. XNO exists only in the browser, for display.
func TestNoFloatsOrXNOInGo(t *testing.T) {
	scanned := map[string]bool{}
	err := filepath.WalkDir("../..", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && (d.Name() == "web" || d.Name() == "testdata" || strings.HasPrefix(d.Name(), ".")) && path != "../.." {
			return filepath.SkipDir
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		scanned[filepath.ToSlash(path)] = true
		for _, v := range violations(t, path, nil) {
			t.Errorf("%s", v)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// a walk that silently found nothing would pass, so prove the money code was actually read
	for _, must := range []string{"../../internal/ledger/ledger.go", "../../internal/nanocrypto/nanocrypto.go",
		"../../internal/nanoclient/nanoclient.go", "../../internal/payout/payout.go",
		"../../internal/ingest/ingest.go", "../../internal/api/api.go", "../../internal/stats/stats.go"} {
		if !scanned[must] {
			t.Errorf("%s was not scanned (scanned %d files)", must, len(scanned))
		}
	}
}

// the detector must fire on each pattern it claims to catch
func TestDetectorCatches(t *testing.T) {
	src := `package x
import ("math/big"; "strconv")
var a float64
var b = float32(1)
var c = new(big.Float)
var d, _ = strconv.ParseFloat("1", 64)
func toXNO() {}
var rawPerXno = 1`
	if got := violations(t, "sample.go", []byte(src)); len(got) != 6 {
		t.Fatalf("want 6 violations, got %d: %v", len(got), got)
	}
}

var xno = regexp.MustCompile(`(?i)xno`)

func violations(t *testing.T, path string, src []byte) []string {
	t.Helper()
	fset := token.NewFileSet()
	var source any
	if src != nil {
		source = src
	}
	f, err := parser.ParseFile(fset, path, source, parser.SkipObjectResolution)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	report := func(n ast.Node, what string) { out = append(out, fset.Position(n.Pos()).String()+": "+what) }
	ast.Inspect(f, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.Ident:
			switch {
			case n.Name == "float32" || n.Name == "float64":
				report(n, "float type "+n.Name)
			case xno.MatchString(n.Name):
				report(n, "XNO-unit identifier "+n.Name)
			}
		case *ast.SelectorExpr:
			if pkg, ok := n.X.(*ast.Ident); ok {
				if (pkg.Name == "big" && strings.Contains(n.Sel.Name, "Float")) || (pkg.Name == "strconv" && strings.Contains(n.Sel.Name, "Float")) {
					report(n, "float conversion "+pkg.Name+"."+n.Sel.Name)
				}
			}
		}
		return true
	})
	return out
}
