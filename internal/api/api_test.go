package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type rt func(*http.Request) *httptest.ResponseRecorder

func (f rt) RoundTrip(r *http.Request) (*http.Response, error) { return f(r).Result(), nil }

func client(fn func(w http.ResponseWriter, r *http.Request)) *Client {
	return &Client{Origin: DefaultConsole, Token: strings.Repeat("a", 64), HTTP: &http.Client{Transport: rt(func(r *http.Request) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		fn(w, r)
		return w
	})}}
}

func TestOrigin(t *testing.T) {
	for in, want := range map[string]string{"https://app.zoctl.com": "https://app.zoctl.com", "https://APP.zoctl.com/": "https://app.zoctl.com", " https://x.example:8443 ": "https://x.example:8443"} {
		if got, err := Origin(in); err != nil || got != want {
			t.Errorf("Origin(%q) = %q, %v", in, got, err)
		}
	}
	for _, in := range []string{"", "http://app.zoctl.com", "https://app.zoctl.com/x", "https://app.zoctl.com?a", "https://u@app.zoctl.com", "app.zoctl.com"} {
		if _, err := Origin(in); !errors.Is(err, ErrOrigin) {
			t.Errorf("Origin(%q) accepted", in)
		}
	}
}

func TestRedirectsAreNotFollowed(t *testing.T) {
	var hosts []string
	c := client(func(w http.ResponseWriter, r *http.Request) {
		hosts = append(hosts, r.URL.Host)
		http.Redirect(w, r, "https://elsewhere.example/steal", http.StatusFound)
	})
	if _, err := c.Who(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Who: %v", err)
	}
	if len(hosts) != 1 {
		t.Fatalf("followed the redirect: %v", hosts)
	}
}

func TestStatusCodes(t *testing.T) {
	for code, want := range map[int]error{401: ErrSignedOut, 404: ErrNotFound, 403: ErrRefused, 400: ErrRefused, 503: ErrUnavailable, 502: ErrUnavailable} {
		c := client(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "x", code) })
		if _, err := c.Moves(context.Background()); !errors.Is(err, want) {
			t.Errorf("%d: %v, want %v", code, err, want)
		}
	}
}

func TestErrorsNeverCarryTheToken(t *testing.T) {
	c := client(func(w http.ResponseWriter, r *http.Request) { http.Error(w, r.Header.Get("Authorization"), 500) })
	_, err := c.Who(context.Background())
	if err == nil || strings.Contains(err.Error(), c.Token) {
		t.Fatalf("Who: %v", err)
	}
}

func TestNoTokenSendsNothing(t *testing.T) {
	sent := false
	c := client(func(w http.ResponseWriter, r *http.Request) { sent = true })
	c.Token = ""
	if _, err := c.Who(context.Background()); !errors.Is(err, ErrSignedOut) || sent {
		t.Fatalf("Who: %v, sent %v", err, sent)
	}
}

func TestAMoveIDIsCheckedBeforeItGoesInAPath(t *testing.T) {
	sent := false
	c := client(func(w http.ResponseWriter, r *http.Request) { sent = true })
	for _, id := range []string{"../whoami", strings.Repeat("A", 32), "abc"} {
		if _, err := c.Move(context.Background(), id); !errors.Is(err, ErrNotFound) || sent {
			t.Fatalf("Move(%q): %v, sent %v", id, err, sent)
		}
	}
}

func TestStartLoginKeepsThePageOnTheConsole(t *testing.T) {
	c := client(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"device":"` + strings.Repeat("d", 64) + `","code":"BCDF-GHJK","url":"https://phish.example/cli","expires":"2026-11-20T12:10:00Z","interval":0}`))
	})
	s, err := c.StartLogin(context.Background())
	if err != nil || s.URL != DefaultConsole+"/cli" || s.Interval != 5 {
		t.Fatalf("StartLogin: %+v, %v", s, err)
	}
}
