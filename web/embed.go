// Package web holds the page's static files so the binary carries them (D50). The files themselves are plain
// HTML, CSS and ES modules with no build step.
package web

import "embed"

//go:embed index.html css js
var Files embed.FS
