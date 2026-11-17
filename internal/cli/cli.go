// Package cli is the zoctl command line: sign in to the zoctl console and
// read your organization's moves from a terminal.
package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"runtime"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/zoctl/cli/internal/api"
	"github.com/zoctl/cli/internal/creds"
)

// App is one run of the command line, with everything it touches outside.
type App struct {
	Version string
	Stdout  io.Writer
	Stderr  io.Writer
	Getenv  func(string) string
	HTTP    *http.Client
	Store   creds.Store
	Now     func() time.Time
	// Open shows a page in the browser. Nil opens nothing.
	Open func(url string) error
	// Sleep waits between questions while signing in.
	Sleep func(ctx context.Context, d time.Duration) error
}

// errUsage is a command line that couldn't be read. The message is printed
// already.
var errUsage = errors.New("usage")

const usage = `zoctl signs in to the zoctl console and shows your organization's moves.

Usage:
  zoctl login [-no-browser]   sign this terminal in with a code typed in the browser
  zoctl whoami [-json]        who this terminal is signed in as
  zoctl moves [-json]         the moves your runners report
  zoctl move [-json] ID       one move; the first characters of its ID will do
  zoctl open [ID]             open the console, or a move's page, in the browser
  zoctl logout                sign this terminal out
  zoctl version               print the version

Environment:
  ZOCTL_CONSOLE     the console to sign in to (default ` + api.DefaultConsole + `)
  ZOCTL_CONFIG_DIR  where the session is kept (default: your configuration folder)
`

// Run runs one command and returns the exit status.
func (a *App) Run(ctx context.Context, args []string) int {
	if len(args) == 0 {
		fmt.Fprint(a.Stderr, usage)
		return 2
	}
	cmds := map[string]func(context.Context, []string) error{
		"login":  a.login,
		"logout": a.logout,
		"whoami": a.whoami,
		"moves":  a.moves,
		"move":   a.move,
		"open":   a.open,
		"version": func(context.Context, []string) error {
			fmt.Fprintf(a.Stdout, "zoctl %s\n", a.Version)
			return nil
		},
	}
	switch args[0] {
	case "help", "-h", "-help", "--help":
		fmt.Fprint(a.Stdout, usage)
		return 0
	case "-version", "--version":
		args[0] = "version"
	}
	cmd, ok := cmds[args[0]]
	if !ok {
		fmt.Fprintf(a.Stderr, "zoctl: there's no command %q.\n\n%s", args[0], usage)
		return 2
	}
	err := cmd(ctx, args[1:])
	switch {
	case err == nil:
		return 0
	case errors.Is(err, errUsage):
		return 2
	case errors.Is(err, context.Canceled):
		fmt.Fprintln(a.Stderr, "zoctl: stopped.")
		return 130
	default:
		fmt.Fprintf(a.Stderr, "zoctl: %s\n", say(err))
		return 1
	}
}

// say is an error in words for a person.
func say(err error) string {
	switch {
	case errors.Is(err, api.ErrSignedOut):
		return "this terminal isn't signed in. Run zoctl login."
	case errors.Is(err, errLocked):
		return err.Error()
	case errors.Is(err, api.ErrOrigin):
		return "ZOCTL_CONSOLE isn't a console address: " + api.ErrOrigin.Error() + "."
	}
	return err.Error()
}

// flags is a command's flag set, printing its errors the same way.
func (a *App) flags(name, args string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(a.Stderr)
	fs.Usage = func() {
		fmt.Fprintf(a.Stderr, "Usage: zoctl %s %s\n", name, args)
		fs.PrintDefaults()
	}
	return fs
}

func (a *App) parse(fs *flag.FlagSet, args []string, want int) error {
	if err := fs.Parse(args); err != nil {
		return errUsage
	}
	if fs.NArg() > want {
		fs.Usage()
		return errUsage
	}
	return nil
}

// console is the console address to sign in to.
func (a *App) console() (string, error) {
	if s := a.Getenv("ZOCTL_CONSOLE"); s != "" {
		return api.Origin(s)
	}
	return api.DefaultConsole, nil
}

func (a *App) userAgent() string { return "zoctl-cli/" + a.Version }

// client is the signed-in client. A session for another console than
// ZOCTL_CONSOLE names is none.
func (a *App) client() (*api.Client, creds.Saved, error) {
	origin, err := a.console()
	if err != nil {
		return nil, creds.Saved{}, err
	}
	s, err := a.Store.Load()
	if errors.Is(err, creds.ErrNone) || (err == nil && s.Console != origin) {
		return nil, creds.Saved{}, api.ErrSignedOut
	}
	if err != nil {
		return nil, creds.Saved{}, fmt.Errorf("the saved session couldn't be read: %w", err)
	}
	if !s.Expires.IsZero() && !a.Now().Before(s.Expires) {
		_ = a.Store.Remove()
		return nil, creds.Saved{}, api.ErrSignedOut
	}
	return &api.Client{Origin: origin, Token: s.Token, UserAgent: a.userAgent(), HTTP: a.HTTP}, s, nil
}

// signedOut forgets a session the console no longer takes, and passes err on.
func (a *App) signedOut(err error) error {
	if errors.Is(err, api.ErrSignedOut) {
		_ = a.Store.Remove()
	}
	return err
}

func (a *App) json(v any) error {
	enc := json.NewEncoder(a.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// errLocked is a console that wanted a password before it would answer:
// one not open to the public yet.
var errLocked = errors.New("the console asked for a password before it would answer, so it isn't open to the command line yet")

func (a *App) login(ctx context.Context, args []string) error {
	fs := a.flags("login", "[-no-browser]")
	noBrowser := fs.Bool("no-browser", false, "print the address only; don't open a browser")
	if err := a.parse(fs, args, 0); err != nil {
		return err
	}
	origin, err := a.console()
	if err != nil {
		return err
	}
	c := &api.Client{Origin: origin, UserAgent: a.userAgent(), HTTP: a.HTTP}
	start, err := c.StartLogin(ctx)
	if errors.Is(err, api.ErrSignedOut) {
		return errLocked
	}
	if err != nil {
		return err
	}
	fmt.Fprintf(a.Stderr, "To sign this terminal in, open %s and type this code:\n\n    %s\n\n", start.URL, start.Code)
	if !*noBrowser && a.Open != nil && a.Open(start.URL) == nil {
		fmt.Fprintln(a.Stderr, "Your browser should be open on that page now.")
	}
	fmt.Fprintf(a.Stderr, "Waiting for the code. It works until %s; Ctrl-C stops.\n", start.Expires.Local().Format("15:04"))

	wait := time.Duration(start.Interval) * time.Second
	for {
		if err := a.Sleep(ctx, wait); err != nil {
			return err
		}
		s, waiting, err := c.PollLogin(ctx, start.Device)
		switch {
		case errors.Is(err, api.ErrNotFound):
			return errors.New("that code expired or was already used. Run zoctl login again")
		case errors.Is(err, api.ErrUnavailable):
			// A busy or restarting console: ask again, more slowly,
			// until the code runs out.
			if !a.Now().Before(start.Expires) {
				return err
			}
			wait = min(2*wait, time.Minute)
			continue
		case err != nil:
			return err
		case waiting:
			if !a.Now().Before(start.Expires) {
				return errors.New("nobody typed the code in time. Run zoctl login again")
			}
			continue
		}
		// A session this terminal had before is ended on the console
		// too, not just forgotten here.
		if old, err := a.Store.Load(); err == nil && old.Console == origin && old.Token != s.Token {
			_ = (&api.Client{Origin: origin, Token: old.Token, UserAgent: a.userAgent(), HTTP: a.HTTP}).SignOut(ctx)
		}
		if err := a.Store.Save(creds.Saved{Console: origin, Org: s.Org, Token: s.Token, Expires: s.Expires}); err != nil {
			_ = (&api.Client{Origin: origin, Token: s.Token, UserAgent: a.userAgent(), HTTP: a.HTTP}).SignOut(ctx)
			return fmt.Errorf("signed in, but the session couldn't be kept, so it was ended: %w", err)
		}
		c.Token = s.Token
		who, err := c.Who(ctx)
		if err != nil {
			fmt.Fprintln(a.Stdout, "Signed in.")
			return nil
		}
		fmt.Fprintf(a.Stdout, "Signed in to %s as %s, %s.\n", orgName(who), memberName(who), who.Role)
		return nil
	}
}

func (a *App) logout(ctx context.Context, args []string) error {
	if err := a.parse(a.flags("logout", ""), args, 0); err != nil {
		return err
	}
	c, _, err := a.client()
	if errors.Is(err, api.ErrSignedOut) {
		fmt.Fprintln(a.Stdout, "This terminal wasn't signed in.")
		return nil
	}
	if err != nil {
		return err
	}
	if err := c.SignOut(ctx); err != nil && !errors.Is(err, api.ErrSignedOut) {
		// Kept, so logout can be run again once the console answers.
		return fmt.Errorf("the console couldn't end the session, so it's still kept here: %w", err)
	}
	if err := a.Store.Remove(); err != nil {
		return err
	}
	fmt.Fprintln(a.Stdout, "Signed out.")
	return nil
}

func orgName(w api.Who) string {
	if w.Organization != "" {
		return w.Organization
	}
	return "organization " + w.Org
}

func memberName(w api.Who) string {
	if w.Name != "" {
		return w.Name
	}
	return w.Subject
}

func (a *App) whoami(ctx context.Context, args []string) error {
	fs := a.flags("whoami", "[-json]")
	asJSON := fs.Bool("json", false, "print JSON")
	if err := a.parse(fs, args, 0); err != nil {
		return err
	}
	c, _, err := a.client()
	if err != nil {
		return err
	}
	w, err := c.Who(ctx)
	if err != nil {
		return a.signedOut(err)
	}
	if *asJSON {
		return a.json(w)
	}
	tw := tabwriter.NewWriter(a.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintf(tw, "Console\t%s\n", c.Origin)
	fmt.Fprintf(tw, "Organization\t%s\n", orgName(w))
	fmt.Fprintf(tw, "You\t%s\n", memberName(w))
	role := w.Role
	if w.Billing {
		role += ", with billing access"
	}
	fmt.Fprintf(tw, "Role\t%s\n", role)
	fmt.Fprintf(tw, "Signed in until\t%s\n", w.Expires.Local().Format("2 Jan 2006 15:04"))
	return tw.Flush()
}

func (a *App) moves(ctx context.Context, args []string) error {
	fs := a.flags("moves", "[-json]")
	asJSON := fs.Bool("json", false, "print JSON")
	if err := a.parse(fs, args, 0); err != nil {
		return err
	}
	c, _, err := a.client()
	if err != nil {
		return err
	}
	list, err := c.Moves(ctx)
	if err != nil {
		return a.signedOut(err)
	}
	if *asJSON {
		if list == nil {
			list = []api.Move{}
		}
		return a.json(list)
	}
	if len(list) == 0 {
		fmt.Fprintln(a.Stdout, "No moves yet. A paired runner's moves show here once it reports.")
		return nil
	}
	tw := tabwriter.NewWriter(a.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "MOVE\tFROM\tTO\tSTAGE\tCREATED\tALERTS\tLAST REPORT\t")
	now := a.Now()
	for _, m := range list {
		flag := ""
		if m.Attention {
			flag = "needs attention"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d/%d\t%d\t%s\t%s\n", short(m.ID), m.Source, m.Target, m.Stage,
			m.Counts.Created, m.Counts.Planned, m.Counts.Alerts, ago(now, m.Received), flag)
	}
	return tw.Flush()
}

// short is the start of a move's ID, enough to tell moves apart.
func short(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}

// ago is how long before now t was, roughly.
func ago(now, t time.Time) string {
	if t.IsZero() {
		return "never"
	}
	d := now.Sub(t)
	switch {
	case d < time.Minute:
		return "just now"
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d/time.Minute))
	case d < 48*time.Hour:
		return fmt.Sprintf("%dh ago", int(d/time.Hour))
	default:
		return fmt.Sprintf("%dd ago", int(d/(24*time.Hour)))
	}
}

// find is the move whose ID is id or starts with it.
func (a *App) find(ctx context.Context, c *api.Client, id string) (api.Move, error) {
	id = strings.ToLower(strings.TrimSpace(id))
	if len(id) == 32 {
		return c.Move(ctx, id)
	}
	if len(id) < 4 {
		return api.Move{}, errors.New("give at least the first 4 characters of the move's ID")
	}
	list, err := c.Moves(ctx)
	if err != nil {
		return api.Move{}, err
	}
	var found []api.Move
	for _, m := range list {
		if strings.HasPrefix(m.ID, id) {
			found = append(found, m)
		}
	}
	switch len(found) {
	case 0:
		return api.Move{}, fmt.Errorf("no move's ID starts with %s. zoctl moves lists them", id)
	case 1:
		return found[0], nil
	}
	return api.Move{}, fmt.Errorf("%d moves' IDs start with %s. Give more of it", len(found), id)
}

func (a *App) move(ctx context.Context, args []string) error {
	fs := a.flags("move", "[-json] ID")
	asJSON := fs.Bool("json", false, "print JSON")
	if err := a.parse(fs, args, 1); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		fs.Usage()
		return errUsage
	}
	c, _, err := a.client()
	if err != nil {
		return err
	}
	m, err := a.find(ctx, c, fs.Arg(0))
	if errors.Is(err, api.ErrNotFound) {
		return errors.New("there's no move with that ID in your organization")
	}
	if err != nil {
		return a.signedOut(err)
	}
	if *asJSON {
		return a.json(m)
	}
	now := a.Now()
	tw := tabwriter.NewWriter(a.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintf(tw, "Move\t%s\n", m.ID)
	fmt.Fprintf(tw, "From\t%s\n", m.Source)
	fmt.Fprintf(tw, "To\t%s\n", m.Target)
	fmt.Fprintf(tw, "Stage\t%s\n", m.Stage)
	if m.Runner != "" {
		fmt.Fprintf(tw, "Runner\t%s\n", m.Runner)
	}
	fmt.Fprintf(tw, "Last report\t%s, %s\n", ago(now, m.Received), freshness(m.Freshness))
	ct := m.Counts
	fmt.Fprintf(tw, "Resources\t%d found, %d planned, %d created, %d verified\n", ct.Resources, ct.Planned, ct.Created, ct.Verified)
	fmt.Fprintf(tw, "Data\t%d datasets, %ds behind\n", ct.Datasets, m.LagSeconds)
	fmt.Fprintf(tw, "Alerts\t%d\n", ct.Alerts)
	ready := "no"
	if m.Ready {
		ready = "yes"
	}
	fmt.Fprintf(tw, "Ready to cut over\t%s\n", ready)
	if !m.ReturnUntil.IsZero() {
		fmt.Fprintf(tw, "Switch back until\t%s\n", m.ReturnUntil.Local().Format("2 Jan 2006 15:04"))
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	if len(m.Findings) > 0 {
		fmt.Fprintln(a.Stdout, "\nFindings:")
		for _, f := range m.Findings {
			fmt.Fprintf(a.Stdout, "  %s (%s)\n", f.Text, f.Code)
		}
	}
	fmt.Fprintf(a.Stdout, "\nThe plan stays on your runner. Its page in the console: %s/moves/%s\n", c.Origin, m.ID)
	return nil
}

func freshness(f string) string {
	switch f {
	case "live":
		return "reporting"
	case "stale":
		return "stale: nothing heard lately"
	}
	return "disconnected: no report, state unknown"
}

func (a *App) open(ctx context.Context, args []string) error {
	if err := a.parse(a.flags("open", "[ID]"), args, 1); err != nil {
		return err
	}
	origin, err := a.console()
	if err != nil {
		return err
	}
	page := origin + "/"
	if len(args) == 1 {
		c, _, err := a.client()
		if err != nil {
			return err
		}
		m, err := a.find(ctx, c, args[0])
		if errors.Is(err, api.ErrNotFound) {
			return errors.New("there's no move with that ID in your organization")
		}
		if err != nil {
			return a.signedOut(err)
		}
		page = origin + "/moves/" + m.ID
	}
	fmt.Fprintln(a.Stdout, page)
	if a.Open != nil {
		_ = a.Open(page)
	}
	return nil
}

// OpenBrowser shows url in the default browser.
func OpenBrowser(url string) error {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", url)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	default:
		cmd = exec.Command("xdg-open", url)
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	go func() { _ = cmd.Wait() }()
	return nil
}

// Sleep waits d, or until ctx ends.
func Sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
