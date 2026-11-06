package main

import (
	"bytes"
	"embed"
	"net/http"
	"time"
)

//go:embed web/index.html web/app.css web/app.js
var webFiles embed.FS

func serveWebClient(w http.ResponseWriter, r *http.Request) {
	var path, contentType string
	switch r.URL.Path {
	case "/":
		path, contentType = "web/index.html", "text/html; charset=utf-8"
	case "/app.css":
		path, contentType = "web/app.css", "text/css; charset=utf-8"
	case "/app.js":
		path, contentType = "web/app.js", "text/javascript; charset=utf-8"
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
