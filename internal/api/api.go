// Package api is the zoctl console's command-line API: signing a terminal in
// with a code typed in the browser, and reading what the member can read.
// It holds no credential but the console session it is given, and never puts
// one in an error.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// DefaultConsole is the hosted console.
const DefaultConsole = "https://app.zoctl.com"

var (
	// ErrSignedOut is a session the console no longer takes, or none.
	ErrSignedOut = errors.New("not signed in")
	// ErrNotFound is something the console doesn't have for you, or a
	// sign-in code that expired or was used.
	ErrNotFound = errors.New("not found")
	// ErrRefused is a request the console refused outright.
	ErrRefused = errors.New("the console refused the request")
	// ErrUnavailable is a console that couldn't answer just now.
	ErrUnavailable = errors.New("the console isn't available right now")
	// ErrOrigin is a console address that isn't one.
	ErrOrigin = errors.New("a console address is https:// and a host name, nothing else")
)

const (
	maxResponse = 1 << 20
	timeout     = 30 * time.Second
)

var (
	hex64  = regexp.MustCompile(`^[0-9a-f]{64}$`)
	moveID = regexp.MustCompile(`^[0-9a-f]{32}$`)
)

// Origin checks a console address: https, a host and nothing after it.
func Origin(s string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(s))
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return "", ErrOrigin
	}
	return "https://" + strings.ToLower(u.Host), nil
}

// Client talks to one console. Token is the terminal's session, "" before
// it signs in.
type Client struct {
	Origin    string
	Token     string
	UserAgent string
	HTTP      *http.Client
}

// Start is what the console answers a new sign-in with. Device is the
// terminal's secret for asking how it went: never print it.
type Start struct {
	Device   string    `json:"device"`
	Code     string    `json:"code"`
	URL      string    `json:"url"`
	Expires  time.Time `json:"expires"`
	Interval int       `json:"interval"`
}

// Session is a signed-in terminal's. Token is a secret: never print it.
type Session struct {
	Token   string    `json:"token"`
	Org     string    `json:"org"`
	Expires time.Time `json:"expires"`
}

// Who is the member a session is for.
type Who struct {
	Org          string    `json:"org"`
	Organization string    `json:"organization,omitempty"`
	Subject      string    `json:"subject"`
	Name         string    `json:"name,omitempty"`
	Role         string    `json:"role"`
	Billing      bool      `json:"billing"`
	Expires      time.Time `json:"expires"`
}

// Counts are a move's numbers, as its runner last reported them.
type Counts struct {
	Resources int `json:"resources"`
	Planned   int `json:"planned"`
	Created   int `json:"created"`
	Verified  int `json:"verified"`
	Datasets  int `json:"datasets"`
	Alerts    int `json:"alerts"`
}

// Finding is something a runner flagged, by code and in words.
type Finding struct {
	Code string `json:"code"`
	Text string `json:"text"`
}

// Move is one move as the console shows it. A runner reports only codes,
// counts and times, so there are no names here.
type Move struct {
	ID          string    `json:"id"`
	Source      string    `json:"source"`
	Target      string    `json:"target"`
	Stage       string    `json:"stage"`
	Freshness   string    `json:"freshness"`
	Counts      Counts    `json:"counts"`
	LagSeconds  int       `json:"lag_seconds"`
	Findings    []Finding `json:"findings"`
	Ready       bool      `json:"ready"`
	Attention   bool      `json:"attention"`
	Runner      string    `json:"runner,omitempty"`
	Observed    time.Time `json:"observed"`
	Received    time.Time `json:"received"`
	ReturnUntil time.Time `json:"return_until,omitzero"`
}

// StartLogin asks the console for a code to type.
func (c *Client) StartLogin(ctx context.Context) (Start, error) {
	var out Start
	if _, err := c.do(ctx, http.MethodPost, "/v1/cli/login", false, struct{}{}, &out); err != nil {
		return Start{}, err
	}
	if !hex64.MatchString(out.Device) || out.Code == "" || out.Expires.IsZero() {
		return Start{}, fmt.Errorf("%w: an answer it shouldn't give", ErrUnavailable)
	}
	// The page to type the code on is the console's own and nowhere else.
	if u, err := url.Parse(out.URL); err != nil || "https://"+u.Host != c.Origin || u.Scheme != "https" || u.User != nil {
		out.URL = c.Origin + "/cli"
	}
	if out.Interval < 1 || out.Interval > 60 {
		out.Interval = 5
	}
	return out, nil
}

// PollLogin asks whether the code was typed. Waiting is true until it is.
func (c *Client) PollLogin(ctx context.Context, device string) (s Session, waiting bool, err error) {
	code, err := c.do(ctx, http.MethodPost, "/v1/cli/token", false, map[string]string{"device": device}, &s)
	if err != nil {
		return Session{}, false, err
	}
	if code == http.StatusAccepted {
		return Session{}, true, nil
	}
	if !hex64.MatchString(s.Token) {
		return Session{}, false, fmt.Errorf("%w: an answer it shouldn't give", ErrUnavailable)
	}
	return s, false, nil
}

// Who is the member this terminal is signed in as.
func (c *Client) Who(ctx context.Context) (Who, error) {
	var out Who
	_, err := c.do(ctx, http.MethodGet, "/v1/cli/whoami", true, nil, &out)
	return out, err
}

// Moves are the organization's moves, the ones needing attention first.
func (c *Client) Moves(ctx context.Context) ([]Move, error) {
	var out struct {
		Moves []Move `json:"moves"`
	}
	if _, err := c.do(ctx, http.MethodGet, "/v1/cli/moves", true, nil, &out); err != nil {
		return nil, err
	}
	// An ID goes into an address and a command, so one that isn't an ID
	// is no answer.
	for _, m := range out.Moves {
		if !moveID.MatchString(m.ID) {
			return nil, fmt.Errorf("%w: an answer it shouldn't give", ErrUnavailable)
		}
	}
	return out.Moves, nil
}

// Move is one move, by its full ID.
func (c *Client) Move(ctx context.Context, id string) (Move, error) {
	if !moveID.MatchString(id) {
		return Move{}, ErrNotFound
	}
	var out Move
	if _, err := c.do(ctx, http.MethodGet, "/v1/cli/moves/"+id, true, nil, &out); err != nil {
		return Move{}, err
	}
	if out.ID != id {
		return Move{}, fmt.Errorf("%w: an answer it shouldn't give", ErrUnavailable)
	}
	return out, nil
}

// SignOut ends this terminal's session on the console.
func (c *Client) SignOut(ctx context.Context) error {
	_, err := c.do(ctx, http.MethodPost, "/v1/cli/signout", true, nil, nil)
	return err
}

// do sends one request and decodes a 200's body into out. It returns the
// status code for the callers that tell 200 from 202.
func (c *Client) do(ctx context.Context, method, path string, signed bool, body, out any) (int, error) {
	if _, err := Origin(c.Origin); err != nil {
		return 0, err
	}
	if signed && !hex64.MatchString(c.Token) {
		return 0, ErrSignedOut
	}
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, err
		}
		rd = bytes.NewReader(b)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, c.Origin+path, rd)
	if err != nil {
		return 0, ErrOrigin
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	if c.UserAgent != "" {
		req.Header.Set("User-Agent", c.UserAgent)
	}
	if signed {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	hc := http.Client{}
	if c.HTTP != nil {
		hc = *c.HTTP
	}
	// The console never redirects these, so a redirect is refused, not
	// followed somewhere with the token.
	hc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := hc.Do(req)
	if err != nil {
		if ctx.Err() != nil && errors.Is(err, context.Canceled) {
			return 0, err
		}
		// The error names the console, never the token: url.Error
		// holds the URL, and these have no credential in it.
		return 0, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxResponse+1))
	if err != nil || len(b) > maxResponse {
		return 0, fmt.Errorf("%w: an answer it couldn't read", ErrUnavailable)
	}
	switch resp.StatusCode {
	case http.StatusOK:
		if out != nil {
			if err := json.Unmarshal(b, out); err != nil {
				return 0, fmt.Errorf("%w: an answer it couldn't read", ErrUnavailable)
			}
		}
		return resp.StatusCode, nil
	case http.StatusAccepted, http.StatusNoContent:
		return resp.StatusCode, nil
	case http.StatusUnauthorized:
		return resp.StatusCode, ErrSignedOut
	case http.StatusNotFound:
		return resp.StatusCode, ErrNotFound
	case http.StatusBadRequest, http.StatusForbidden:
		return resp.StatusCode, ErrRefused
	default:
		return resp.StatusCode, fmt.Errorf("%w (%d)", ErrUnavailable, resp.StatusCode)
	}
}
