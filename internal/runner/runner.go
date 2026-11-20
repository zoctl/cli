// Package runner is the customer's side of console status sharing. A runner
// pairs once with a code an owner made in the console, then sends each move's
// status out over HTTPS: stage, counts, lag and finding codes, nothing named.
// Nothing comes back in, and the console can't reach the runner.
package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/zoctl/cli/internal/datasync"
	"github.com/zoctl/cli/internal/transfer"
)

var (
	// ErrConfig means an origin, code, contact or state was refused.
	ErrConfig = errors.New("runner: refused")
	// ErrConsole means the console didn't take the request.
	ErrConsole = errors.New("runner: the console refused")
)

const (
	stateName   = "runner"
	maxResponse = 4 << 10
	timeout     = 15 * time.Second
)

var hex64 = regexp.MustCompile(`^[0-9a-f]{64}$`)

// Paired is what a runner keeps after pairing. The token is a secret; it
// lives in the runner's private workspace and is never printed.
type Paired struct {
	Console string `json:"console"`
	Org     string `json:"org"`
	Runner  string `json:"runner"`
	Token   string `json:"token"`
}

// Report is the console's status schema. It has no field a name could go in.
type Report struct {
	Move        string    `json:"move"`
	Source      string    `json:"source"`
	Target      string    `json:"target"`
	Stage       string    `json:"stage"`
	Counts      Counts    `json:"counts"`
	LagSeconds  int       `json:"lag_seconds"`
	Findings    []string  `json:"findings,omitempty"`
	Ready       bool      `json:"ready"`
	Observed    time.Time `json:"observed"`
	ReturnUntil time.Time `json:"return_until,omitzero"`
	// Switched is when a finished cutover was made, and Returned when a
	// finished return was: only the report that says one is done has
	// either. The console counts a move's 30 days from Switched.
	Switched time.Time `json:"switched,omitzero"`
	Returned time.Time `json:"returned,omitzero"`
}

// Counts are the report's numbers.
type Counts struct {
	Resources int `json:"resources"`
	Planned   int `json:"planned"`
	Created   int `json:"created"`
	Verified  int `json:"verified"`
	Datasets  int `json:"datasets"`
	Alerts    int `json:"alerts"`
}

// Origin checks a console origin: HTTPS, a host and nothing else.
func Origin(s string) (string, error) {
	u, err := url.Parse(s)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return "", ErrConfig
	}
	return "https://" + u.Host, nil
}

func post(ctx context.Context, c *http.Client, origin, path, token string, body any, out any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return ErrConfig
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, origin+path, bytes.NewReader(b))
	if err != nil {
		return ErrConfig
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if c == nil {
		c = &http.Client{}
	}
	// The console never redirects these, so a redirect is refused rather
	// than followed with the token.
	cc := *c
	cc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := cc.Do(req)
	if err != nil {
		return ErrConsole
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponse))
	if err != nil || resp.StatusCode/100 != 2 {
		return ErrConsole
	}
	if out != nil && json.Unmarshal(data, out) != nil {
		return ErrConsole
	}
	return nil
}

// Pair redeems a pairing code at the console and keeps the token in w.
// contact is the runner's own HTTPS page the console links to.
func Pair(ctx context.Context, c *http.Client, w *transfer.Workspace, origin, code, contact string) (Paired, error) {
	origin, err := Origin(origin)
	if err != nil || !hex64.MatchString(code) || !strings.HasPrefix(contact, "https://") {
		return Paired{}, ErrConfig
	}
	var out struct {
		Org    string `json:"org"`
		Runner string `json:"runner"`
		Token  string `json:"token"`
	}
	if err := post(ctx, c, origin, "/v1/runner/pair", "", map[string]string{"code": code, "contact": contact}, &out); err != nil {
		return Paired{}, err
	}
	if !hex64.MatchString(out.Token) {
		return Paired{}, ErrConsole
	}
	p := Paired{Console: origin, Org: out.Org, Runner: out.Runner, Token: out.Token}
	return p, w.Save(stateName, p)
}

// Load reads a pairing kept by Pair.
func Load(w *transfer.Workspace) (Paired, error) {
	var p Paired
	if err := w.Load(stateName, &p); err != nil {
		return Paired{}, ErrConfig
	}
	if _, err := Origin(p.Console); err != nil || !hex64.MatchString(p.Token) {
		return Paired{}, ErrConfig
	}
	return p, nil
}

// Send posts one status report.
func (p Paired) Send(ctx context.Context, c *http.Client, r Report) error {
	return post(ctx, c, p.Console, "/v1/runner/status", p.Token, r, nil)
}

// FromSync turns a sync's status into a report: the stage, how many datasets
// there are and how many checked out, the largest lag and finding codes.
func FromSync(m datasync.Move, st datasync.Status) Report {
	r := Report{
		Move: m.ID, Source: m.Source, Target: m.Target, Stage: st.Stage,
		LagSeconds: st.Lag(), Findings: st.Findings(), Ready: st.Verified(),
		Observed: st.Observed.UTC().Truncate(time.Second),
	}
	r.Counts.Datasets = len(st.Datasets)
	for _, d := range st.Datasets {
		if d.Verified {
			r.Counts.Verified++
		}
		if d.Failed > 0 {
			r.Counts.Alerts++
		}
	}
	if r.Observed.IsZero() {
		r.Observed = time.Now().UTC().Truncate(time.Second)
	}
	return r
}

// FromCut is a cutover's or a return's report: blocked or planned, running
// while DNS changes, then verified with when the cutover was made and the
// date switching back is kept ready until. A return that's done reports
// paused, with when.
func FromCut(m datasync.Move, stage string, p datasync.CutPlan, now time.Time) Report {
	r := Report{Move: m.ID, Source: m.Source, Target: m.Target, Stage: stage, Findings: p.Findings,
		Observed: now.UTC().Truncate(time.Second), Counts: Counts{Planned: p.Change, Verified: p.Already}}
	switch {
	case stage == "verified":
		r.Ready = true
		r.ReturnUntil = p.ReturnUntil.UTC().Truncate(time.Second)
		r.Counts.Created = p.Change
		if !p.Back && !p.At.IsZero() {
			r.Switched = p.At.UTC().Truncate(time.Second)
		}
	case stage == "paused" && p.Back && !p.At.IsZero():
		r.Returned = p.At.UTC().Truncate(time.Second)
	}
	return r
}
