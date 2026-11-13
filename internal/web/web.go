package web

import (
	"bytes"
	"embed"
	"net/http"
	"time"
)

//go:embed static/index.html static/app.css static/app.js
var webFiles embed.FS

func ServeClient(w http.ResponseWriter, r *http.Request) {
	var path, contentType string
	switch r.URL.Path {
	case "/":
		path, contentType = "static/index.html", "text/html; charset=utf-8"
	case "/app.css":
		path, contentType = "static/app.css", "text/css; charset=utf-8"
	case "/app.js":
		path, contentType = "static/app.js", "text/javascript; charset=utf-8"
	default:
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	data, err := webFiles.ReadFile(path)
	if err != nil {
		http.Error(w, "page unavailable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "no-cache")
	http.ServeContent(w, r, path, time.Time{}, bytes.NewReader(data))
}
