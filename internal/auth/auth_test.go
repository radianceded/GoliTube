package auth

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCredentialsAndCookie(t *testing.T) {
	for _, body := range []string{
		`{"username":"ab","password":"password123"}`,
		`{"username":"valid_name","password":"short"}`,
		`{"username":"valid_name","password":"` + strings.Repeat("x", 73) + `"}`,
		`{"username":"valid_name","password":"password123","id":"forged"}`,
		`{"username":"valid_name","password":"password123"} {}`,
		`null`, strings.Repeat("x", 4097),
	} {
		request := httptest.NewRequest("POST", "/auth/register", strings.NewReader(body))
		if _, err := readCredentials(httptest.NewRecorder(), request); err == nil {
			t.Fatalf("accepted invalid credentials: %s", body[:min(len(body), 30)])
		}
	}
	input, err := readCredentials(httptest.NewRecorder(), httptest.NewRequest("POST", "/auth/register", strings.NewReader(`{"username":"  Alice_1 ","password":" password123 "}`)))
	if err != nil || input.Username != "alice_1" || input.Password != " password123 " {
		t.Fatal("normalization changed password or failed username")
	}
	for _, secure := range []bool{false, true} {
		a := &Handler{SecureCookie: secure}
		response := httptest.NewRecorder()
		a.setCookie(response, httptest.NewRequest("GET", "http://localhost/auth/me", nil), strings.Repeat("a", 26), 86400)
		cookie := response.Result().Cookies()[0]
		if !cookie.HttpOnly || cookie.SameSite != http.SameSiteLaxMode || cookie.Secure != secure || cookie.Path != "/" || cookie.Domain != "" || cookie.MaxAge != 86400 {
			t.Fatal("unsafe cookie attributes")
		}
	}
}

func TestAuthMethodsAndOrigin(t *testing.T) {
	a := &Handler{}
	for _, test := range []struct {
		method, path, origin string
		status               int
	}{
		{"GET", "/auth/register", "", 405}, {"POST", "/auth/me", "", 405},
		{"POST", "/auth/login", "https://evil.example", 403},
		{"POST", "/auth/logout", "null", 403}, {"POST", "/auth/register", "http://example.com.evil.example", 403},
		{"GET", "/auth/unknown", "", 404}, {"POST", "/auth/login", "", 503},
	} {
		request := httptest.NewRequest(test.method, "http://example.com"+test.path, nil)
		request.Header.Set("Origin", test.origin)
		response := httptest.NewRecorder()
		a.ServeHTTP(response, request)
		if response.Code != test.status {
			t.Fatalf("%s %s: got %d want %d", test.method, test.path, response.Code, test.status)
		}
		if response.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("auth response can be cached")
		}
	}
}

func TestAuthClientIP(t *testing.T) {
	for _, test := range []struct {
		trusted                 bool
		remote, forwarded, want string
	}{
		{false, "127.0.0.1:1000", "203.0.113.1", "127.0.0.1"},
		{true, "127.0.0.1:1000", "203.0.113.1", "203.0.113.1"},
		{true, "[::1]:1000", "2001:db8::1", "2001:db8::1"},
		{true, "198.51.100.1:1000", "203.0.113.1", "198.51.100.1"},
		{true, "127.0.0.1:1000", "203.0.113.1, 198.51.100.1", "127.0.0.1"},
		{true, "127.0.0.1:1000", "", "127.0.0.1"},
	} {
		r := httptest.NewRequest("POST", "/auth/login", nil)
		r.RemoteAddr = test.remote
		r.Header.Set("X-Real-IP", test.forwarded)
		a := &Handler{TrustLoopbackProxy: test.trusted}
		if got := a.clientIP(r); got != test.want {
			t.Fatalf("trusted=%t remote=%q forwarded=%q got=%q want=%q", test.trusted, test.remote, test.forwarded, got, test.want)
		}
	}
}
