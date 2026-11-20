package movecmd

// zoctl cutover points a move's DNS at the target, and with -back at the
// source again. It prints counts and finding codes, never a name or value.

import (
	"context"
	"crypto/subtle"
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/zoctl/cli/internal/datasync"
	"github.com/zoctl/cli/internal/runner"
	"github.com/zoctl/cli/internal/transfer"
)

// cutoverSleep replaces the settle wait in tests.
var cutoverSleep func(context.Context, time.Duration) error

func printCutPlan(out io.Writer, p datasync.CutPlan) {
	if p.Back {
		fmt.Fprintf(out, "switching back: %d DNS record sets go back to what they were before the cutover", p.Change)
		if !p.ReturnUntil.IsZero() {
			fmt.Fprintf(out, " (kept ready until %s)", p.ReturnUntil.Format("2 January 2006"))
		}
		fmt.Fprintln(out)
		if p.Standby > 0 {
			fmt.Fprintf(out, "  the way back: %d databases' sequences on the old side are set to the target's, then the target's slot and publication are dropped\n", p.Standby)
		}
	} else {
		fmt.Fprintf(out, "cutover: %d DNS record sets change (%d new), %d already point at the target\n", p.Change, p.Created, p.Already)
		if p.Standby > 0 && !p.Resume {
			fmt.Fprintf(out, "  the way back: %d databases get a publication and a slot on the target before DNS changes\n", p.Standby)
			fmt.Fprintln(out, "  the slot holds the target's WAL until zoctl sync -back reads it: run that, or switch back, or drop the slot")
		}
	}
	for _, f := range p.Findings {
		fmt.Fprintf(out, "  finding: %s\n", f)
	}
	if p.Blocked {
		fmt.Fprintln(out, "  blocked: fix the findings above, then plan again")
	}
}

// sayBlocked adds what two findings' codes can't say. partWay is a cutover
// that began and didn't finish.
func sayBlocked(out io.Writer, partWay bool, p datasync.CutPlan) {
	for _, f := range p.Findings {
		switch {
		case f == "standby_not_caught_up" && partWay:
			fmt.Fprintln(out, "  the old side doesn't have all the target took, and this cutover stopped part way: a reverse sync runs only under a finished one. Run zoctl cutover to finish it, then zoctl sync -back, then plan the return again")
		case f == "standby_not_caught_up":
			fmt.Fprintln(out, "  the old side doesn't have all the target took: run zoctl sync -back, then plan the return again")
		case f == "dns_changed":
			fmt.Fprintln(out, "  a record is neither as the cutover found it nor as it planned. Set it to one of the two by hand, then plan again; the workspace still holds what it was")
		}
	}
}

// cutover plans a cutover or a return, and with the plan's digest typed
// back carries it out.
func cutover(args []string, out io.Writer, in io.Reader) error {
	flags := flag.NewFlagSet("cutover", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	file := flags.String("move", "", "move file")
	dir := flags.String("workspace", "", "the sync's private directory")
	back := flags.Bool("back", false, "switch DNS back to the source")
	settle := flags.Duration("settle", time.Minute, "how long the source must stay quiet first")
	report := flags.Bool("report", false, "send status to the console the workspace is paired with")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *file == "" || *dir == "" || *settle < 10*time.Second || *settle > time.Hour {
		return errConfig
	}
	m, err := loadMove(out, *file)
	if err != nil {
		return err
	}
	if m.Cutover == nil || *report && m.ID == "" {
		return errConfig
	}
	w, err := transfer.OpenWorkspace(*dir)
	if err != nil {
		return err
	}
	defer w.Close()
	// Switching back reads each dataset the other way round too, and a
	// database with a way back has its old side's sequences set, so only
	// then are the STANDBY_ names read.
	open := datasync.OpenCut
	if *back {
		open = datasync.OpenReturn
	}
	c, err := open(m, datasync.Options{Env: moveEnv(m, *back), Client: syncClient()}, *settle)
	if err != nil {
		return err
	}
	c.Sleep = cutoverSleep
	defer c.Close()
	ctx, stop := interruptContext()
	defer stop()
	send := func(string, datasync.CutPlan) {}
	if *report {
		paired, err := runner.Load(w)
		if err != nil {
			return err
		}
		client := syncClient()
		// The last status goes out after an interrupt has ended ctx.
		send = func(stage string, p datasync.CutPlan) {
			sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), lastSend)
			defer cancel()
			if paired.Send(sctx, client, runner.FromCut(m, stage, p, time.Now())) != nil {
				fmt.Fprintln(out, "status not sent to the console")
			}
		}
	}
	p, err := c.Plan(ctx, w, *back)
	if err != nil {
		// Nothing to plan because it's done already. The console may have
		// been down when it was, and a move's 30 days there start with
		// this report, so it's sent again from what the workspace holds.
		if *report && (!*back && errors.Is(err, datasync.ErrCutOver) || *back && errors.Is(err, datasync.ErrNoCutover)) {
			if done, ok, derr := datasync.CutDone(w); derr == nil && ok && done.Back == *back {
				fmt.Fprintln(out, "already done; sending its status to the console again")
				stage := "verified"
				if done.Back {
					stage = "paused"
				}
				send(stage, done)
			}
		}
		return err
	}
	printCutPlan(out, p)
	if p.Blocked {
		under, _ := datasync.CutOver(w)
		_, done, _ := datasync.CutDone(w)
		sayBlocked(out, under && !done, p)
		send("blocked", p)
		return datasync.ErrBlocked
	}
	send("planned", p)
	switch {
	case p.Resume:
		fmt.Fprintln(out, "This finishes a cutover that stopped part way: some records already point at the target.")
		fmt.Fprintln(out, "The two sides aren't compared again, and anything written to the source since stays only there.")
		fmt.Fprintln(out, "To undo the part that happened instead, run zoctl cutover -back.")
	case !m.HasData():
		fmt.Fprintln(out, "This move has no data, so only DNS changes: nothing is copied or compared, and nothing waits.")
		fmt.Fprintln(out, "Whatever the apps keep on either side stays where it is.")
	case *back:
		fmt.Fprintln(out, "Switching back copies nothing: the old side already has what the target has, or this plan is blocked.")
		fmt.Fprintln(out, "Stop the target's writers before you approve: DNS can't stop a writer.")
		if p.Standby > 0 {
			fmt.Fprintf(out, "zoctl then waits %s for the target to stay quiet and checks the old side again.\n", *settle)
		}
	default:
		fmt.Fprintln(out, "Stop everything that writes to the source before you approve: DNS can't stop a writer.")
		fmt.Fprintf(out, "zoctl then waits %s for the source to stay quiet and checks the data again.\n", *settle)
	}
	fmt.Fprintf(out, "nothing has changed yet\nto go ahead, type the plan's digest:\n%s\n", p.Digest)
	approved, err := readApproval(in)
	if err != nil || subtle.ConstantTimeCompare([]byte(approved), []byte(p.Digest)) != 1 {
		fmt.Fprintln(out, "not approved; nothing changed")
		return errConfig
	}
	send("running", p)
	done, err := c.Apply(ctx, w, p, approved)
	if err != nil {
		for _, f := range done.Findings {
			fmt.Fprintf(out, "  finding: %s\n", f)
		}
		switch {
		case errors.Is(err, transfer.ErrBusy):
			fmt.Fprintln(out, "a sync is using this workspace; stop it, then plan again. Nothing changed and the approval isn't spent")
		case errors.Is(err, datasync.ErrCutPartial):
			fmt.Fprintln(out, "stopped part way: some records changed and could not be put back, so DNS points at both sides")
			if *back {
				fmt.Fprintln(out, "run zoctl cutover -back again to finish switching back")
			} else {
				fmt.Fprintln(out, "run zoctl cutover again to finish, or zoctl cutover -back to undo what changed")
			}
		default:
			fmt.Fprintln(out, "stopped; DNS is as it was")
		}
		send("failed", done)
		return err
	}
	if *back {
		fmt.Fprintln(out, "switched back; DNS points at the source")
		send("paused", done)
		return nil
	}
	fmt.Fprintf(out, "cut over; switching back stays ready until %s with zoctl cutover -back\n", done.ReturnUntil.Format("2 January 2006"))
	send("verified", done)
	return nil
}
