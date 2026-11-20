package movecmd

// zoctl sync copies a move's data to the target under one approval. It runs
// where the customer runs it, with both sides' keys in its environment, and
// prints counts: never a bucket, key, table or account.

import (
	"context"
	"crypto/subtle"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/zoctl/cli/internal/datasync"
	"github.com/zoctl/cli/internal/runner"
	"github.com/zoctl/cli/internal/transfer"
	"github.com/zoctl/cli/internal/transfer/dns"
	"github.com/zoctl/cli/internal/transfer/objects"
)

// syncClient is the HTTP client storage calls use. Large objects take as
// long as they take, so only the connection and headers are timed.
var syncClient = func() *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.ResponseHeaderTimeout = 2 * time.Minute
	return &http.Client{Transport: t}
}

// errDiffer means -check found the two sides different.
var errDiffer = errors.New("the two sides differ")

// lastSend is how long a status may take to reach the console once the
// command's own context is over.
const lastSend = 30 * time.Second

// moveEnv takes every credential the move names out of the environment.
// Two datasets on one provider share their names, so each is taken once.
// The old side's write keys, the STANDBY_ names, are taken only when
// standby is set: for a reverse sync and for switching back.
func moveEnv(m datasync.Move, standby bool) func(string) string {
	env := map[string]string{}
	take := func(name string) {
		if _, ok := env[name]; !ok {
			env[name] = takeEnv(name)
		}
	}
	for _, d := range m.Datasets {
		for _, n := range objects.CredentialNames(d.From, "SOURCE") {
			take(n)
		}
		for _, n := range objects.CredentialNames(d.To, "TARGET") {
			take(n)
		}
		if d.FromEnv != "" {
			take(d.FromEnv)
			take(d.ToEnv)
		}
		if !standby {
			continue
		}
		for _, n := range objects.CredentialNames(d.From, "STANDBY") {
			take(n)
		}
		if d.BackEnv != "" {
			take(d.BackEnv)
			take(d.BackFromEnv)
		}
	}
	if m.Cutover != nil {
		for _, n := range dns.CredentialNames(m.Cutover.Zone, m.Cutover.Credentials) {
			take(n)
		}
	}
	return func(name string) string { return env[name] }
}

func printSyncPlan(out io.Writer, p datasync.Plan, back bool) {
	for i, d := range p.Datasets {
		switch {
		case d.Kind == "objects" && back:
			fmt.Fprintf(out, "dataset %d, objects: %d to copy back to the old side (%d bytes), %d already there, %d only on the old side and left alone\n", i+1, d.Copy, d.CopyBytes, d.Same, d.Extra)
		case d.Kind == "objects":
			fmt.Fprintf(out, "dataset %d, objects: %d to copy (%d bytes), %d already there, %d only on the target and left alone\n", i+1, d.Copy, d.CopyBytes, d.Same, d.Extra)
		case back:
			fmt.Fprintf(out, "dataset %d, %s: %d tables replicate back to the old side\n", i+1, d.Kind, d.Tables)
		default:
			fmt.Fprintf(out, "dataset %d, %s: %d tables published\n", i+1, d.Kind, d.Tables)
		}
		for _, f := range d.Findings {
			fmt.Fprintf(out, "  finding: %s\n", f)
		}
		switch {
		case d.Blocked && back:
			fmt.Fprintln(out, "  blocked: fix the findings above, then plan again")
		case d.Blocked:
			fmt.Fprintln(out, "  blocked: fix the findings above on the source, then plan again")
		}
	}
	fmt.Fprintf(out, "limits: %d objects, %d bytes, %d hours\n", p.Limits.MaxObjects, p.Limits.MaxBytes, p.Limits.Hours)
}

func printSyncStatus(out io.Writer, st datasync.Status) {
	for i, d := range st.Datasets {
		if d.Kind == "postgres" {
			// The subscription copies the rows, so there's no count of
			// them to give: say what's still copying and how far behind.
			fmt.Fprintf(out, "%s round %d dataset %d, postgres: tables still copying %d, tables whose row counts differ %d, lag %d bytes (%ds), verified %v\n",
				st.Stage, st.Rounds, i+1, d.Pending, d.Extra, d.LagBytes, d.LagSeconds, d.Verified)
		} else {
			fmt.Fprintf(out, "%s round %d dataset %d: %d copied (%d bytes), %d failed, %d pending, lag %ds, verified %v\n",
				st.Stage, st.Rounds, i+1, d.Copied, d.Bytes, d.Failed, d.Pending, d.LagSeconds, d.Verified)
		}
		for _, f := range d.Findings {
			fmt.Fprintf(out, "  finding: %s\n", f)
		}
	}
}

// syncData plans a move's data sync, and with the plan's digest typed back
// runs it. -check only compares the two sides.
func syncData(args []string, out io.Writer, in io.Reader) error {
	flags := flag.NewFlagSet("sync", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	file := flags.String("move", "", "move file")
	dir := flags.String("workspace", "", "private directory for this sync")
	every := flags.Duration("every", 5*time.Minute, "time between catch-up rounds")
	rounds := flags.Int("rounds", 0, "stop after this many catch-up rounds; 0 runs until the limits")
	only := flags.Bool("check", false, "compare the two sides and stop")
	report := flags.Bool("report", false, "send status to the console the workspace is paired with")
	back := flags.Bool("back", false, "after a cutover, copy the target's data back to the old side")
	// The console's status has no word for which way a sync runs, so a
	// reverse sync isn't reported: it would read as the move's own.
	if flags.Parse(args) != nil || flags.NArg() != 0 || *file == "" || *every < time.Second || *rounds < 0 || *report && (*only || *dir == "" || *back) {
		return errConfig
	}
	m, err := loadMove(out, *file)
	if err != nil {
		return err
	}
	if *report && m.ID == "" {
		return errConfig
	}
	if !m.HasData() {
		fmt.Fprintln(out, "this move has no data to sync; zoctl cutover switches its DNS")
		return datasync.ErrNoData
	}
	if *back {
		return syncBack(m, *dir, *every, *rounds, *only, out, in)
	}
	s, err := datasync.Open(m, datasync.Options{Env: moveEnv(m, false), Client: syncClient()})
	if err != nil {
		return err
	}
	defer s.Close()
	ctx, stop := interruptContext()
	defer stop()
	if *only {
		st, err := s.Check(ctx)
		if err != nil {
			return err
		}
		printSyncStatus(out, st)
		if !st.Verified() {
			fmt.Fprintln(out, "the two sides differ")
			return errDiffer
		}
		fmt.Fprintln(out, "the two sides match")
		return nil
	}
	if *dir == "" {
		return errConfig
	}
	w, err := transfer.OpenWorkspace(*dir)
	if err != nil {
		return err
	}
	defer w.Close()
	if cut, err := datasync.CutOver(w); err != nil || cut {
		fmt.Fprintln(out, "this move has cut over; a sync would copy the source's old data over the target's")
		return datasync.ErrCutOver
	}
	send := func(datasync.Status) {}
	if *report {
		paired, err := runner.Load(w)
		if err != nil {
			return err
		}
		client := syncClient()
		warned := false
		// A console that's down never stops a sync; it's said once. The
		// last status goes out after an interrupt has ended ctx, so each
		// send has a context of its own.
		send = func(st datasync.Status) {
			sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), lastSend)
			defer cancel()
			if paired.Send(sctx, client, runner.FromSync(m, st)) != nil && !warned {
				warned = true
				fmt.Fprintln(out, "status not sent to the console; the sync goes on")
			}
		}
	}
	p, err := s.Plan(ctx)
	if err != nil {
		return err
	}
	printSyncPlan(out, p, false)
	send(planStatus(p, time.Now()))
	if p.Blocked() {
		return datasync.ErrBlocked
	}
	fmt.Fprintf(out, "nothing has been copied yet\nto start this sync, type its digest:\n%s\n", p.Digest)
	approved, err := readApproval(in)
	if err != nil || subtle.ConstantTimeCompare([]byte(approved), []byte(p.Digest)) != 1 {
		fmt.Fprintln(out, "not approved; nothing was copied")
		return errConfig
	}
	fmt.Fprintln(out, "syncing; interrupt once to stop it")
	st, err := s.Run(ctx, w, p, approved, datasync.RunOptions{Every: *every, Rounds: *rounds, Report: func(st datasync.Status) { printSyncStatus(out, st); send(st) }})
	switch {
	case errors.Is(err, transfer.ErrBusy):
		fmt.Fprintln(out, "another sync or a cutover is using this workspace; nothing was copied and the approval isn't spent")
		return err
	case errors.Is(err, datasync.ErrStop):
		fmt.Fprintln(out, "the target's subscription could not be disabled; it may still be replicating")
	case errors.Is(err, datasync.ErrCutOver) && st.Stage != "":
		fmt.Fprintln(out, "the move cut over while this ran; the sync stopped before its next round")
	}
	if st.Stage == "" {
		return err
	}
	fmt.Fprintf(out, "%s; this approval can't run again, plan again for more\n", st.Stage)
	return err
}

// syncBack is zoctl sync -back: after a cutover, the target's data copied
// to the old side. It is the one command that writes there, so it runs only
// while the workspace holds a finished cutover, reads the old side's keys
// from STANDBY_ names, and has a plan and an approval of its own.
func syncBack(m datasync.Move, dir string, every time.Duration, rounds int, only bool, out io.Writer, in io.Reader) error {
	if dir == "" {
		return errConfig
	}
	w, err := transfer.OpenWorkspace(dir)
	if err != nil {
		return err
	}
	defer w.Close()
	// Checked before a key is read: nothing opens the old side to be
	// written while the source is the side taking writes.
	if ok, err := datasync.Standing(w); err != nil || !ok {
		fmt.Fprintln(out, "this move hasn't cut over, or has switched back; a reverse sync writes to the old side only while the target is the one taking writes")
		return datasync.ErrNotCutOver
	}
	s, err := datasync.OpenBack(m, datasync.Options{Env: moveEnv(m, true), Client: syncClient()})
	if errors.Is(err, datasync.ErrNoWayBack) {
		fmt.Fprintln(out, "a database in the move file has no back_env, so its rows have no way back")
	}
	if err != nil {
		return err
	}
	defer s.Close()
	ctx, stop := interruptContext()
	defer stop()
	if only {
		st, err := s.Check(ctx)
		if err != nil {
			return err
		}
		printSyncStatus(out, st)
		if !st.Verified() {
			fmt.Fprintln(out, "the old side doesn't have everything the target has")
			return errDiffer
		}
		fmt.Fprintln(out, "the old side has everything the target has")
		return nil
	}
	p, err := s.Plan(ctx)
	if err != nil {
		return err
	}
	printSyncPlan(out, p, true)
	if p.Blocked() {
		return datasync.ErrBlocked
	}
	fmt.Fprintf(out, "this writes to the old side: objects are added or replaced and none is deleted,\nand a published table's rows follow the target's, deletes included\nnothing has been copied yet\nto start this reverse sync, type its digest:\n%s\n", p.Digest)
	approved, err := readApproval(in)
	if err != nil || subtle.ConstantTimeCompare([]byte(approved), []byte(p.Digest)) != 1 {
		fmt.Fprintln(out, "not approved; nothing was copied")
		return errConfig
	}
	fmt.Fprintln(out, "syncing back; interrupt once to stop it")
	st, err := s.Run(ctx, w, p, approved, datasync.RunOptions{Every: every, Rounds: rounds, Report: func(st datasync.Status) { printSyncStatus(out, st) }})
	switch {
	case errors.Is(err, transfer.ErrBusy):
		fmt.Fprintln(out, "another sync or a cutover is using this workspace; nothing was copied and the approval isn't spent")
		return err
	case errors.Is(err, datasync.ErrStop):
		fmt.Fprintln(out, "the old side's subscription could not be disabled; it may still be replicating")
	case errors.Is(err, datasync.ErrNotCutOver) && st.Stage != "":
		fmt.Fprintln(out, "the move switched back while this ran; the reverse sync stopped before its next round")
	}
	if st.Stage == "" {
		return err
	}
	fmt.Fprintf(out, "%s; this approval can't run again, plan again for more\n", st.Stage)
	return err
}

// planStatus is what a console sees of a plan: planned or blocked, and the
// findings.
func planStatus(p datasync.Plan, now time.Time) datasync.Status {
	st := datasync.Status{Stage: "planned", Observed: now}
	if p.Blocked() {
		st.Stage = "blocked"
	}
	for _, d := range p.Datasets {
		st.Datasets = append(st.Datasets, datasync.DatasetStatus{Kind: d.Kind, Findings: d.Findings})
	}
	return st
}

// pairRunner pairs this machine's workspace with a console. The code an
// owner made in the console is typed in, never a flag, and the token the
// console returns stays in the workspace.
func pairRunner(args []string, out io.Writer, in io.Reader) error {
	flags := flag.NewFlagSet("runner pair", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	console := flags.String("console", "", "console origin, https://...")
	contact := flags.String("contact", "", "an https page of yours the console links to")
	dir := flags.String("workspace", "", "the sync's private directory")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *console == "" || *contact == "" || *dir == "" {
		return errConfig
	}
	w, err := transfer.OpenWorkspace(*dir)
	if err != nil {
		return err
	}
	defer w.Close()
	fmt.Fprintln(out, "type the pairing code from the console:")
	code, err := readApproval(in)
	if err != nil {
		return errConfig
	}
	ctx, stop := interruptContext()
	defer stop()
	if _, err := runner.Pair(ctx, syncClient(), w, *console, code, *contact); err != nil {
		return err
	}
	fmt.Fprintln(out, "paired; zoctl sync -report sends this workspace's status")
	return nil
}

// interruptContext ends on the first Ctrl-C.
func interruptContext() (context.Context, func()) {
	ctx, cancel := context.WithCancel(context.Background())
	signals, stop := interrupts()
	go func() {
		select {
		case <-signals:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, func() { stop(); cancel() }
}

// loadMove reads a move file and, when it is refused, says which part in the
// fixed words datasync has for it. Nothing the file held is printed.
func loadMove(out io.Writer, file string) (datasync.Move, error) {
	m, err := datasync.LoadMove(file)
	if err != nil {
		if part := datasync.MovePart(err); part != "" {
			fmt.Fprintf(out, "the move file was refused at %s\n", part)
		} else {
			fmt.Fprintln(out, "the move file was refused: it has to be a readable file of at most 1 MB")
		}
	}
	return m, err
}
