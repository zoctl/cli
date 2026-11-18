package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zoctl/cli/internal/creds"
)

var (
	device = strings.Repeat("d", 64)
	token  = strings.Repeat("7", 64)
	moveA  = "0123456789abcdef0123456789abcdef"
	moveB  = "0123fedcba9876543210fedcba987654"
)

// fakeConsole answers the command-line routes the way the console does.
type fakeConsole struct {
	mu       sync.Mutex
	polls    int
	typedAt  int // the poll that finds the code typed in
	signedIn map[string]bool
	seen     []*http.Request
	locked   bool
	// name is the member's name, when a test wants one of its own.
	name string
}

func (f *fakeConsole) RoundTrip(r *http.Request) (*http.Response, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seen = append(f.seen, r)
	w := httptest.NewRecorder()
	f.serve(w, r)
	return w.Result(), nil
}

func (f *fakeConsole) serve(w http.ResponseWriter, r *http.Request) {
	if r.URL.Host != "app.zoctl.com" || r.URL.Scheme != "https" {
		http.Error(w, "wrong host", http.StatusBadGateway)
		return
	}
	if f.locked {
		http.Error(w, "locked", http.StatusUnauthorized)
		return
	}
	authed := func() bool {
		tok, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || !f.signedIn[tok] {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return false
		}
		return true
	}
	js := func(v any) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(v)
	}
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	switch r.Method + " " + r.URL.Path {
	case "POST /v1/cli/login":
		if r.Header.Get("Content-Type") != "application/json" {
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}
		js(map[string]any{"device": device, "code": "BCDF-GHJK", "url": "https://app.zoctl.com/cli", "expires": now.Add(10 * time.Minute), "interval": 5})
	case "POST /v1/cli/token":
		var in map[string]string
		if json.NewDecoder(r.Body).Decode(&in) != nil || in["device"] != device {
			http.Error(w, "Not Found", http.StatusNotFound)
			return
		}
		f.polls++
		if f.polls < f.typedAt {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		if f.polls > f.typedAt {
			http.Error(w, "Not Found", http.StatusNotFound)
			return
		}
		f.signedIn[token] = true
		js(map[string]any{"token": token, "org": "org1", "expires": now.Add(30 * 24 * time.Hour)})
	case "GET /v1/cli/whoami":
		if authed() {
			name := "Ada"
			if f.name != "" {
				name = f.name
			}
			js(map[string]any{"org": "org1", "organization": "Acme", "subject": "sub1", "name": name, "role": "operator", "expires": now.Add(time.Hour)})
		}
	case "GET /v1/cli/moves":
		if authed() {
			js(map[string]any{"moves": []map[string]any{
				{"id": moveA, "source": "aws", "target": "hetzner", "stage": "blocked", "freshness": "live", "attention": true,
					"counts": map[string]int{"planned": 4, "created": 1, "alerts": 2}, "received": now.Add(-3 * time.Minute),
					"findings": []map[string]string{{"code": "not_portable", "text": "Something can't move as it is"}}},
				{"id": moveB, "source": "heroku", "target": "aws", "stage": "running", "freshness": "live", "received": now},
			}})
		}
	case "GET /v1/cli/moves/" + moveA:
		if authed() {
			js(map[string]any{"id": moveA, "source": "aws", "target": "hetzner", "stage": "blocked", "freshness": "stale", "runner": "home runner",
				"findings": []map[string]string{{"code": "not_portable", "text": "Something can't move as it is"}}})
		}
	case "POST /v1/cli/signout":
		if authed() {
			delete(f.signedIn, token)
			w.WriteHeader(http.StatusNoContent)
		}
	default:
		http.Error(w, "Not Found", http.StatusNotFound)
	}
}

type harness struct {
	app    *App
	fake   *fakeConsole
	out    *bytes.Buffer
	err    *bytes.Buffer
	opened []string
	env    map[string]string
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{fake: &fakeConsole{typedAt: 3, signedIn: map[string]bool{}}, out: &bytes.Buffer{}, err: &bytes.Buffer{}, env: map[string]string{}}
	h.app = &App{
		Version: "test",
		Stdout:  h.out,
		Stderr:  h.err,
		Getenv:  func(k string) string { return h.env[k] },
		HTTP:    &http.Client{Transport: h.fake},
		Store:   creds.Store{Path: filepath.Join(t.TempDir(), "session.json")},
		Now:     func() time.Time { return time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC) },
		Open:    func(u string) error { h.opened = append(h.opened, u); return nil },
		Sleep:   func(ctx context.Context, _ time.Duration) error { return ctx.Err() },
	}
	return h
}

func (h *harness) run(args ...string) int {
	h.out.Reset()
	h.err.Reset()
	return h.app.Run(context.Background(), args)
}

func (h *harness) login(t *testing.T) {
	t.Helper()
	if code := h.run("login"); code != 0 {
		t.Fatalf("login: %d %s", code, h.err)
	}
}

func TestLoginShowsTheCodeAndKeepsTheSession(t *testing.T) {
	h := newHarness(t)
	h.login(t)
	if !strings.Contains(h.err.String(), "BCDF-GHJK") || !strings.Contains(h.err.String(), "https://app.zoctl.com/cli") {
		t.Fatalf("login didn't show the code and page: %s", h.err)
	}
	if len(h.opened) != 1 || h.opened[0] != "https://app.zoctl.com/cli" {
		t.Fatalf("opened %v", h.opened)
	}
	if got := h.out.String(); got != "Signed in to Acme as Ada, operator.\n" {
		t.Fatalf("login said %q", got)
	}
	if h.fake.polls != 3 {
		t.Fatalf("polled %d times", h.fake.polls)
	}
	s, err := h.app.Store.Load()
	if err != nil || s.Token != token || s.Console != "https://app.zoctl.com" || s.Org != "org1" {
		t.Fatalf("saved %+v, %v", s, err)
	}
	// The secrets are never shown.
	for _, secret := range []string{token, device} {
		if strings.Contains(h.out.String()+h.err.String(), secret) {
			t.Fatal("login printed a secret")
		}
	}
	for _, r := range h.fake.seen {
		if r.Header.Get("User-Agent") != "zoctl-cli/test" {
			t.Fatalf("User-Agent %q", r.Header.Get("User-Agent"))
		}
		if r.Header.Get("Origin") != "" {
			t.Fatal("sent an Origin header")
		}
	}
}

func TestLoginWithoutABrowser(t *testing.T) {
	h := newHarness(t)
	if code := h.run("login", "-no-browser"); code != 0 {
		t.Fatalf("login: %d %s", code, h.err)
	}
	if len(h.opened) != 0 {
		t.Fatalf("opened %v", h.opened)
	}
}

func TestLoginWhenTheCodeRunsOut(t *testing.T) {
	h := newHarness(t)
	h.fake.typedAt = 1000
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	h.app.Now = func() time.Time { now = now.Add(time.Minute); return now }
	if code := h.run("login"); code != 1 || !strings.Contains(h.err.String(), "nobody typed the code in time") {
		t.Fatalf("login: %d %s", code, h.err)
	}
	if _, err := h.app.Store.Load(); !errors.Is(err, creds.ErrNone) {
		t.Fatalf("kept a session: %v", err)
	}
}

func TestLoginStopsOnCtrlC(t *testing.T) {
	h := newHarness(t)
	h.fake.typedAt = 1000
	ctx, cancel := context.WithCancel(context.Background())
	n := 0
	h.app.Sleep = func(ctx context.Context, _ time.Duration) error {
		if n++; n == 2 {
			cancel()
		}
		return ctx.Err()
	}
	if code := h.app.Run(ctx, []string{"login"}); code != 130 {
		t.Fatalf("login: %d %s", code, h.err)
	}
}

func TestLoginToALockedConsole(t *testing.T) {
	h := newHarness(t)
	h.fake.locked = true
	if code := h.run("login"); code != 1 || !strings.Contains(h.err.String(), "asked for a password") {
		t.Fatalf("login: %d %s", code, h.err)
	}
}

func TestWhoami(t *testing.T) {
	h := newHarness(t)
	if code := h.run("whoami"); code != 1 || !strings.Contains(h.err.String(), "Run zoctl login") {
		t.Fatalf("whoami signed out: %d %s", code, h.err)
	}
	h.login(t)
	if code := h.run("whoami"); code != 0 {
		t.Fatalf("whoami: %d %s", code, h.err)
	}
	for _, want := range []string{"Acme", "Ada", "operator", "https://app.zoctl.com"} {
		if !strings.Contains(h.out.String(), want) {
			t.Errorf("whoami lacks %q:\n%s", want, h.out)
		}
	}
	if code := h.run("whoami", "-json"); code != 0 {
		t.Fatalf("whoami -json: %d", code)
	}
	var w map[string]any
	if err := json.Unmarshal(h.out.Bytes(), &w); err != nil || w["role"] != "operator" {
		t.Fatalf("whoami -json: %s", h.out)
	}
}

func TestMoves(t *testing.T) {
	h := newHarness(t)
	h.login(t)
	if code := h.run("moves"); code != 0 {
		t.Fatalf("moves: %d %s", code, h.err)
	}
	lines := strings.Split(strings.TrimSpace(h.out.String()), "\n")
	if len(lines) != 3 || !strings.HasPrefix(lines[0], "MOVE") {
		t.Fatalf("moves:\n%s", h.out)
	}
	if !strings.Contains(lines[1], moveA[:12]) || !strings.Contains(lines[1], "1/4") || !strings.Contains(lines[1], "3m ago") || !strings.Contains(lines[1], "needs attention") {
		t.Fatalf("first move: %q", lines[1])
	}
	if code := h.run("moves", "-json"); code != 0 {
		t.Fatalf("moves -json: %d", code)
	}
	var list []map[string]any
	if err := json.Unmarshal(h.out.Bytes(), &list); err != nil || len(list) != 2 {
		t.Fatalf("moves -json: %s", h.out)
	}
}

func TestMoveByPrefix(t *testing.T) {
	h := newHarness(t)
	h.login(t)
	if code := h.run("move", moveA[:6]); code != 0 {
		t.Fatalf("move: %d %s", code, h.err)
	}
	for _, want := range []string{moveA, "blocked", "Something can't move as it is (not_portable)", "https://app.zoctl.com/moves/" + moveA} {
		if !strings.Contains(h.out.String(), want) {
			t.Errorf("move lacks %q:\n%s", want, h.out)
		}
	}
	if code := h.run("move", "0123"); code != 1 || !strings.Contains(h.err.String(), "2 moves") {
		t.Fatalf("ambiguous: %d %s", code, h.err)
	}
	if code := h.run("move", "ffff"); code != 1 || !strings.Contains(h.err.String(), "no move's ID starts with ffff") {
		t.Fatalf("no match: %d %s", code, h.err)
	}
	if code := h.run("move", strings.Repeat("e", 32)); code != 1 || !strings.Contains(h.err.String(), "no move with that ID") {
		t.Fatalf("unknown: %d %s", code, h.err)
	}
	if code := h.run("move"); code != 2 {
		t.Fatalf("no ID: %d", code)
	}
}

func TestOpen(t *testing.T) {
	h := newHarness(t)
	if code := h.run("open"); code != 0 || h.opened[0] != "https://app.zoctl.com/" {
		t.Fatalf("open: %d %v", code, h.opened)
	}
	h.login(t)
	h.opened = nil
	if code := h.run("open", moveA[:8]); code != 0 || h.opened[0] != "https://app.zoctl.com/moves/"+moveA {
		t.Fatalf("open move: %d %v %s", code, h.opened, h.err)
	}
}

func TestLogout(t *testing.T) {
	h := newHarness(t)
	h.login(t)
	if code := h.run("logout"); code != 0 || h.out.String() != "Signed out.\n" {
		t.Fatalf("logout: %d %q %s", code, h.out, h.err)
	}
	if h.fake.signedIn[token] {
		t.Fatal("the console still has the session")
	}
	if _, err := h.app.Store.Load(); !errors.Is(err, creds.ErrNone) {
		t.Fatalf("still saved: %v", err)
	}
	if code := h.run("logout"); code != 0 || !strings.Contains(h.out.String(), "wasn't signed in") {
		t.Fatalf("second logout: %d %q", code, h.out)
	}
}

// A session the console ended (the Account page, a role change) is
// forgotten here at the next command.
func TestASessionEndedOnTheConsoleIsForgotten(t *testing.T) {
	h := newHarness(t)
	h.login(t)
	delete(h.fake.signedIn, token)
	if code := h.run("moves"); code != 1 || !strings.Contains(h.err.String(), "Run zoctl login") {
		t.Fatalf("moves: %d %s", code, h.err)
	}
	if _, err := h.app.Store.Load(); !errors.Is(err, creds.ErrNone) {
		t.Fatalf("still saved: %v", err)
	}
}

func TestAnotherConsole(t *testing.T) {
	h := newHarness(t)
	h.login(t)
	// A session is for the console it was made on, never sent elsewhere.
	h.env["ZOCTL_CONSOLE"] = "https://other.example"
	n := len(h.fake.seen)
	if code := h.run("whoami"); code != 1 || !strings.Contains(h.err.String(), "Run zoctl login") {
		t.Fatalf("whoami: %d %s", code, h.err)
	}
	if len(h.fake.seen) != n {
		t.Fatal("sent a request")
	}
	for _, bad := range []string{"http://app.zoctl.com", "https://app.zoctl.com/path", "https://u:p@app.zoctl.com", "app.zoctl.com"} {
		h.env["ZOCTL_CONSOLE"] = bad
		if code := h.run("login"); code != 1 || !strings.Contains(h.err.String(), "isn't a console address") {
			t.Fatalf("ZOCTL_CONSOLE=%s: %d %s", bad, code, h.err)
		}
	}
}

func TestUsage(t *testing.T) {
	h := newHarness(t)
	if code := h.run(); code != 2 {
		t.Fatalf("no command: %d", code)
	}
	if code := h.run("nope"); code != 2 || !strings.Contains(h.err.String(), `no command "nope"`) {
		t.Fatalf("unknown: %d %s", code, h.err)
	}
	if code := h.run("help"); code != 0 || !strings.Contains(h.out.String(), "zoctl login") {
		t.Fatalf("help: %d", code)
	}
	if code := h.run("version"); code != 0 || h.out.String() != "zoctl test\n" {
		t.Fatalf("version: %q", h.out)
	}
	if code := h.run("moves", "-bogus"); code != 2 {
		t.Fatalf("bad flag: %d", code)
	}
}

// A name is whatever someone typed on the console, and the rest of an answer
// is the console's. Neither can drive the terminal it's printed on.
func TestWhatTheConsoleSaysCannotDriveTheTerminal(t *testing.T) {
	h := newHarness(t)
	h.login(t)
	h.fake.name = "Ada\x1b]0;owned\x07\x1b[2J\u202e\u009b31m\r\nLovelace"
	if code := h.run("whoami"); code != 0 {
		t.Fatalf("whoami: %d %s", code, h.err)
	}
	if !strings.Contains(h.out.String(), "Ada]0;owned[2J31mLovelace") {
		t.Fatalf("the name's letters went too:\n%q", h.out)
	}
	for _, r := range h.out.String() {
		if r != '\n' && (r < ' ' || r == 0x7f || (r >= 0x80 && r < 0xa0) || r == '\u202e') {
			t.Fatalf("whoami printed %U:\n%q", r, h.out)
		}
	}
	if got := plain(strings.Repeat("a", 3*maxPlain)); len(got) != maxPlain {
		t.Fatalf("plain kept %d characters", len(got))
	}
	if got := plain("  Zo\u00eb Ltd.  "); got != "Zo\u00eb Ltd." {
		t.Fatalf("plain changed a name: %q", got)
	}
}
