// Package site serves the embedded page (D50). Every file is compressed once at startup rather than per request,
// revalidated by ETag so a returning visitor gets 304s with no body, and sent with a strict Content-Security-
// Policy: the page loads nothing but itself.
package site

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"net/http"
	"path"
	"strconv"
	"strings"
)

const csp = "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; " +
	"base-uri 'none'; form-action 'none'; frame-ancestors 'none'"

var types = map[string]string{
	".html": "text/html; charset=utf-8",
	".css":  "text/css; charset=utf-8",
	".js":   "text/javascript; charset=utf-8",
}

type file struct {
	body, gz           []byte
	etag, ctype, cache string
}

// New loads every file under fsys. A file of an unknown type is an error, so nothing is ever served as a guess.
func New(fsys fs.FS) (http.Handler, error) {
	files := map[string]*file{}
	err := fs.WalkDir(fsys, ".", func(name string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		ctype, ok := types[path.Ext(name)]
		if !ok {
			return fmt.Errorf("site: %s: no content type for %q", name, path.Ext(name))
		}
		body, err := fs.ReadFile(fsys, name)
		if err != nil {
			return err
		}
		var gz bytes.Buffer
		w, _ := gzip.NewWriterLevel(&gz, gzip.BestCompression) // a constant valid level cannot fail
		w.Write(body)
		if err := w.Close(); err != nil {
			return err
		}
		h := sha256.Sum256(body)
		// the page itself always revalidates, so a deploy shows at once; its assets may be reused for five
		// minutes, which spares a request per file on every load while a session lasts
		cache := "public, max-age=300"
		if name == "index.html" {
			cache = "no-cache"
		}
		// a weak tag: the body is sent gzipped or plain, and Cloudflare drops strong tags when it re-encodes
		files["/"+name] = &file{body, gz.Bytes(), `W/"` + hex.EncodeToString(h[:12]) + `"`, ctype, cache}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if files["/index.html"] == nil {
		return nil, fmt.Errorf("site: no index.html")
	}
	files["/"] = files["/index.html"]
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f := files[r.URL.Path]
		if f == nil {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		h := w.Header()
		h.Set("Content-Security-Policy", csp)
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Cross-Origin-Opener-Policy", "same-origin")
		h.Set("Cross-Origin-Resource-Policy", "same-origin")
		// two years of https only; browsers ignore it over plain http, so local runs are unaffected (D104 scan)
		h.Set("Strict-Transport-Security", "max-age=63072000; includeSubDomains")
		h.Set("Cache-Control", f.cache)
		h.Set("ETag", f.etag)
		h.Set("Vary", "Accept-Encoding")
		// weak comparison (RFC 9110 §13.1.2): a client or proxy may echo the tag without its W/
		if inm := r.Header.Get("If-None-Match"); inm == f.etag || inm == strings.TrimPrefix(f.etag, "W/") {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		body := f.body
		if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			body = f.gz
			h.Set("Content-Encoding", "gzip")
		}
		h.Set("Content-Type", f.ctype)
		h.Set("Content-Length", strconv.Itoa(len(body)))
		if r.Method == http.MethodHead {
			return
		}
		w.Write(body)
	}), nil
}
