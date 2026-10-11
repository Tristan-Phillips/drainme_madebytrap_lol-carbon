package site

import (
	"bytes"
	"compress/gzip"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/web"
)

func get(t *testing.T, h http.Handler, method, path string, header ...string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, nil)
	for i := 0; i+1 < len(header); i += 2 {
		r.Header.Set(header[i], header[i+1])
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func handler(t *testing.T) http.Handler {
	t.Helper()
	h, err := New(web.Files)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func TestServesEveryFile(t *testing.T) {
	h := handler(t)
	fs.WalkDir(web.Files, ".", func(name string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		want, _ := fs.ReadFile(web.Files, name)
		plain := get(t, h, "GET", "/"+name)
		if plain.Code != 200 || !bytes.Equal(plain.Body.Bytes(), want) || plain.Header().Get("Content-Encoding") != "" {
			t.Errorf("%s: %d, %d bytes", name, plain.Code, plain.Body.Len())
		}
		zipped := get(t, h, "GET", "/"+name, "Accept-Encoding", "br, gzip")
		zr, err := gzip.NewReader(zipped.Body)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		got, _ := io.ReadAll(zr)
		if !bytes.Equal(got, want) || zipped.Header().Get("Content-Encoding") != "gzip" || zipped.Body.Len() >= len(want) {
			t.Errorf("%s: gzip body wrong or not smaller (%d vs %d)", name, zipped.Body.Len(), len(want))
		}
		for _, k := range []string{"Content-Security-Policy", "X-Content-Type-Options", "ETag", "Cache-Control", "Content-Type", "Strict-Transport-Security", "Cross-Origin-Resource-Policy"} {
			if plain.Header().Get(k) == "" {
				t.Errorf("%s: no %s", name, k)
			}
		}
		return nil
	})
}

func TestCaching(t *testing.T) {
	h := handler(t)
	page := get(t, h, "GET", "/")
	if page.Header().Get("Cache-Control") != "no-cache" || !strings.HasPrefix(page.Header().Get("Content-Type"), "text/html") {
		t.Fatalf("index headers %v", page.Header())
	}
	css := get(t, h, "GET", "/css/style.css")
	if css.Header().Get("Cache-Control") != "public, max-age=300" || !strings.HasPrefix(css.Header().Get("Content-Type"), "text/css") {
		t.Fatalf("asset headers %v", css.Header())
	}
	tag := css.Header().Get("ETag")
	if !strings.HasPrefix(tag, `W/"`) {
		t.Fatalf("ETag %s is not weak; Cloudflare strips strong tags when it re-encodes", tag)
	}
	for _, inm := range []string{tag, strings.TrimPrefix(tag, "W/")} {
		if r := get(t, h, "GET", "/css/style.css", "If-None-Match", inm); r.Code != http.StatusNotModified || r.Body.Len() != 0 {
			t.Errorf("If-None-Match %s: %d with %d bytes", inm, r.Code, r.Body.Len())
		}
	}
	if r := get(t, h, "GET", "/css/style.css", "If-None-Match", `"other"`); r.Code != 200 {
		t.Errorf("stale tag: %d", r.Code)
	}
	if head := get(t, h, "HEAD", "/"); head.Code != 200 || head.Body.Len() != 0 || head.Header().Get("Content-Length") == "" {
		t.Errorf("HEAD: %d, %d bytes", head.Code, head.Body.Len())
	}
}

func TestRefuses(t *testing.T) {
	h := handler(t)
	for _, path := range []string{"/nope", "/js/", "/../go.mod", "/embed.go", "/index.html/"} {
		if r := get(t, h, "GET", path); r.Code != http.StatusNotFound {
			t.Errorf("%s: %d", path, r.Code)
		}
	}
	if r := get(t, h, "POST", "/"); r.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST /: %d", r.Code)
	}
	if _, err := New(fstest.MapFS{"index.html": {}, "x.exe": {}}); err == nil {
		t.Error("a file of unknown type was accepted")
	}
	if _, err := New(fstest.MapFS{"a.css": {}}); err == nil {
		t.Error("no index.html was accepted")
	}
}

// every file the page asks for is embedded: stylesheet, scripts, preloads and module imports, lazy ones included
func TestPageReferencesExist(t *testing.T) {
	refs := regexp.MustCompile(`(?:href|src)="((?:css|js)/[^"]+)"|from "\./([^"]+)"|import\("\./([^"]+)"\)`)
	found := 0
	fs.WalkDir(web.Files, ".", func(name string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		body, _ := fs.ReadFile(web.Files, name)
		for _, m := range refs.FindAllStringSubmatch(string(body), -1) {
			ref := m[1] + m[2] + m[3]
			if strings.HasPrefix(name, "js/") && m[1] == "" {
				ref = "js/" + ref
			}
			found++
			if _, err := fs.Stat(web.Files, ref); err != nil {
				t.Errorf("%s references %s, which is not embedded", name, ref)
			}
		}
		return nil
	})
	if found < 10 {
		t.Fatalf("only %d references found; the pattern is not matching the page", found)
	}
}
