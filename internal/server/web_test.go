package server

import (
	"net/http/httptest"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/radianceded/GoliTube/internal/auth"
)

func TestWebClientRoutes(t *testing.T) {
	router := newRouter(&auth.Handler{}, testHub, nil, localPublish)
	for _, test := range []struct {
		method, path, contentType, body string
		status                          int
	}{
		{"GET", "/", "text/html", "GoliTube", 200},
		{"GET", "/app.js", "text/javascript", "refreshIdentity", 200},
		{"GET", "/app.css", "text/css", ".workspace", 200},
		{"HEAD", "/", "text/html", "", 200},
		{"POST", "/", "text/plain", "method not allowed", 405},
		{"GET", "/main.go", "text/plain", "404", 404},
		{"GET", "/web/", "text/plain", "404", 404},
		{"GET", "/missing", "text/plain", "404", 404},
		{"GET", "/health", "text/plain", "ccb", 200},
	} {
		t.Run(test.method+test.path, func(t *testing.T) {
			r := httptest.NewRequest(test.method, test.path, nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, r)
			if w.Code != test.status || !strings.HasPrefix(w.Header().Get("Content-Type"), test.contentType) ||
				!strings.Contains(w.Body.String(), test.body) || !utf8.Valid(w.Body.Bytes()) {
				t.Fatalf("status=%d content-type=%q", w.Code, w.Header().Get("Content-Type"))
			}
			if test.method == "HEAD" && w.Body.Len() != 0 {
				t.Fatal("HEAD returned a body")
			}
		})
	}
}
