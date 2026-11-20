package runner

import (
	"context"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zoctl/cli/internal/datasync"
	"github.com/zoctl/cli/internal/transfer"
)

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func reply(code int, body string) *http.Response {
	return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{}}
}

func workspace(t *testing.T) *transfer.Workspace {
	w, err := transfer.OpenWorkspace(filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	return w
}

func TestOrigin(t *testing.T) {
	for _, bad := range []string{"http://console.example.test", "https://", "https://a@console.example.test", "https://console.example.test/x", "https://console.example.test/?a=b", "ftp://x"} {
		if _, err := Origin(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	if o, err := Origin("https://console.example.test/"); err != nil || o != "https://console.example.test" {
		t.Fatal(o, err)
	}
}

func TestPairKeepsTheTokenAndNeverFollowsARedirect(t *testing.T) {
	code := strings.Repeat("c", 64)
	tok := strings.Repeat("d", 64)
	w := workspace(t)
	client := &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
		if r.URL.String() != "https://console.example.test/v1/runner/pair" || r.Header.Get("Authorization") != "" {
			t.Errorf("pair %s", r.URL)
		}
		return reply(200, `{"org":"o","runner":"r","token":"`+tok+`"}`), nil
	})}
	if _, err := Pair(context.Background(), client, w, "https://console.example.test", code, "https://runner.example.test"); err != nil {
		t.Fatal(err)
	}
	p, err := Load(w)
	if err != nil || p.Token != tok {
		t.Fatalf("%v", err)
	}

	sent := 0
	client.Transport = roundTrip(func(r *http.Request) (*http.Response, error) {
		sent++
		if r.Header.Get("Authorization") != "Bearer "+tok {
			t.Error("no token")
		}
		res := reply(302, "")
		res.Header.Set("Location", "https://elsewhere.example.test/")
		return res, nil
	})
	if err := p.Send(context.Background(), client, Report{}); !errors.Is(err, ErrConsole) || sent != 1 {
		t.Fatalf("redirect: %v, %d requests", err, sent)
	}

	for _, bad := range []string{"short", strings.Repeat("C", 64)} {
		if _, err := Pair(context.Background(), client, w, "https://console.example.test", bad, "https://runner.example.test"); !errors.Is(err, ErrConfig) {
			t.Errorf("code %q: %v", bad, err)
		}
	}
}

func TestFromSyncCarriesNoNames(t *testing.T) {
	m := datasync.Move{ID: strings.Repeat("a", 32), Source: "aws", Target: "gcp"}
	st := datasync.Status{Stage: "running", Observed: time.Date(2026, 11, 20, 12, 0, 0, 5, time.UTC), Datasets: []datasync.DatasetStatus{
		{Kind: "objects", Copied: 3, Failed: 1},
		{Kind: "postgres", LagSeconds: 9, Verified: true, Findings: []string{"replica_identity_missing"}},
	}}
	r := FromSync(m, st)
	if r.Counts.Datasets != 2 || r.Counts.Verified != 1 || r.Counts.Alerts != 1 || r.LagSeconds != 9 || r.Ready ||
		len(r.Findings) != 1 || r.Observed.Nanosecond() != 0 || r.Move != m.ID {
		t.Fatalf("%+v", r)
	}
}
