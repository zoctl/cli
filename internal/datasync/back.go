package datasync

// The way back. A move copies the source to the target and never writes to
// the source. Once it has cut over, the target is the side taking writes and
// the old side is the copy, so the same datasets can be synced the other way:
// what the target took goes to the old side, and a return can go ahead with
// nothing left behind. Run back and forth, that is a standby in a second
// cloud.
//
// A reverse sync is the one thing here that writes to the old side, on these
// terms (policy.ReturnRules):
//
//   - only while the workspace holds a finished cutover that hasn't been
//     switched back;
//   - only on its own command, after its own plan, with that plan's digest
//     typed back;
//   - only with keys from STANDBY_ names, which a sync and a cutover never
//     read, so the read-only keys a move runs on never write. A return
//     reads a database's, to set the old side's sequences;
//   - only data: an object is added or replaced and none the target lacks
//     is deleted, and a published table's rows follow the target's, deletes
//     included. The one thing made on the old side is the subscription.

import (
	"context"
	"errors"
	"io/fs"
	"time"

	"github.com/zoctl/cli/internal/transfer"
	"github.com/zoctl/cli/internal/transfer/objects"
)

var (
	// ErrNotCutOver means a reverse sync was asked for while the source is
	// still the side taking writes: before a cutover, part way through
	// one, or after a return.
	ErrNotCutOver = errors.New("datasync: the move hasn't cut over")
	// ErrNoWayBack means a database in the move file names no back_env, so
	// there is nothing to write its rows to the old side with.
	ErrNoWayBack = errors.New("datasync: the move file gives a database no way back")
)

// Finding codes the way back reports.
const (
	// findingReturnSetup means the way back can't be made: a server older
	// than Postgres 16, or a target that can't hold the slot.
	findingReturnSetup = "return_prerequisite"
	// findingStandbyBehind means the old side doesn't have what the target
	// took since the cutover. A reverse sync is what brings it.
	findingStandbyBehind = "standby_not_caught_up"
	// findingWayBackLeft means a return went through and the way back's
	// slot or subscription couldn't be ended. The slot holds the target's
	// WAL until someone drops it.
	findingWayBackLeft = "return_slot_left"
)

// standing reports whether w holds a finished cutover that hasn't been
// switched back, in whole or in part: the target is the side taking writes.
func standing(w *transfer.Workspace) (bool, error) {
	var st cutState
	err := w.Load(cutoverState, &st)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return st.Complete && st.Returned == nil && st.Returning == nil, nil
}

// Standing reports whether w holds a finished cutover that hasn't been
// switched back, which is when a reverse sync may run.
func Standing(w *transfer.Workspace) (bool, error) { return standing(w) }

// OpenBack opens every dataset of m the other way round, for a reverse
// sync: the target is read and the old side is written, with the old side's
// keys from STANDBY_ names. Plan and Run on what it returns work as a sync's
// do; Run refuses unless the workspace holds a finished cutover.
func OpenBack(m Move, o Options) (*Sync, error) {
	if o.Env == nil {
		return nil, ErrMove
	}
	if !m.HasData() {
		return nil, ErrNoData
	}
	if !m.Reversible() {
		return nil, ErrNoWayBack
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	s := &Sync{move: m, now: o.Now, back: true}
	for _, d := range m.Datasets {
		ds, err := openBack(d, o, true)
		if err != nil {
			s.Close()
			return nil, err
		}
		s.sets = append(s.sets, ds)
	}
	return s, nil
}

// openBack opens one dataset the other way round. With write unset it is
// for a return's check: a bucket's old side is opened with the source's
// read-only keys and the dataset refuses to copy. A database with no
// back_env has no reverse, and reads as nil.
func openBack(d Dataset, o Options, write bool) (dataset, error) {
	switch d.Kind {
	case "objects":
		side := "SOURCE"
		if write {
			side = "STANDBY"
		}
		src, err := objects.Open(d.To, "TARGET", o.Env, o.Client)
		if err != nil {
			return nil, err
		}
		old, err := objects.Open(d.From, side, o.Env, o.Client)
		if err != nil {
			return nil, err
		}
		set := newObjectSet(d.To+"\x00"+d.From+"\x00back", d.Prefix, src, old)
		set.back, set.checkOnly = true, !write
		return set, nil
	case "postgres":
		if d.BackEnv == "" {
			return nil, nil
		}
		return openPostgresBack(d, o)
	}
	return nil, ErrMove
}

// OpenReturn opens a move for switching back: its datasets as OpenCut does,
// and each one the other way round to check the old side has what the
// target took. A bucket's old side is read with the source's keys. A
// database with a way back needs its STANDBY_ URL, since a return sets the
// old side's sequences.
func OpenReturn(m Move, o Options, settle time.Duration) (*Cut, error) {
	c, err := OpenCut(m, o, settle)
	if err != nil {
		return nil, err
	}
	for _, d := range m.Datasets {
		ds, err := openBack(d, o, false)
		if err != nil {
			c.Close()
			return nil, err
		}
		c.back = append(c.back, ds)
	}
	return c, nil
}

// reverse is dataset i the other way round, or nil when the cut was opened
// without one or the dataset has none.
func (c *Cut) reverse(i int) dataset {
	if i < len(c.back) {
		return c.back[i]
	}
	return nil
}

// positioned is a dataset that can say where its writes are.
type positioned interface {
	Position(context.Context) (string, error)
}

// behind reads, a dataset at a time, whether switching back would leave
// something on the target: the finding codes for what would, none when the
// old side has it all.
//
// A database with a way back is asked the other way round, by the slot on
// the target: nothing in it the old side hasn't confirmed. A bucket, or a
// database with no way back, passes when the source's data is still on the
// target as it was and the target has nothing it didn't have at the
// cutover; a bucket that fails that is asked the other way round, which a
// reverse sync makes true.
func (c *Cut) behind(ctx context.Context, saved cutState) ([]string, error) {
	var codes []string
	add := func(code string) {
		for _, have := range codes {
			if have == code {
				return
			}
		}
		codes = append(codes, code)
	}
	for i, d := range c.Sync.sets {
		rev := c.reverse(i)
		if _, ok := rev.(positioned); ok {
			st, err := rev.Check(ctx)
			if err != nil {
				return nil, err
			}
			if !st.Verified {
				add(findingStandbyBehind)
			}
			continue
		}
		st, err := d.Check(ctx)
		if err != nil {
			return nil, err
		}
		// A cutover from before the workspace kept this can only be
		// told apart when the target has nothing of its own at all.
		same := st.Extra == 0
		if i < len(saved.Extras) {
			same = st.extras == saved.Extras[i]
		}
		if st.Verified && same {
			continue
		}
		if rev == nil {
			add(findingNoFailback)
			continue
		}
		if st, err = rev.Check(ctx); err != nil {
			return nil, err
		}
		if !st.Verified {
			add(findingStandbyBehind)
		}
	}
	return codes, nil
}

// backPositions reads where the target's writes are, for each dataset with
// a way back that can say. A return waits on these as a cutover waits on
// the source's.
func (c *Cut) backPositions(ctx context.Context) ([]string, error) {
	var out []string
	for i := range c.Sync.sets {
		p, ok := c.reverse(i).(positioned)
		if !ok {
			out = append(out, "")
			continue
		}
		pos, err := p.Position(ctx)
		if err != nil {
			return nil, err
		}
		out = append(out, pos)
	}
	return out, nil
}

// settleBack is a return's last look before DNS changes: the target has to
// stay quiet over Settle with the old side still holding all of it, and
// then the old side's sequences are set to the target's.
func (c *Cut) settleBack(ctx context.Context, p *CutPlan, saved cutState) error {
	for i := range c.Sync.sets {
		// Only a database can say where its writes are, so there is
		// nothing to wait on without one.
		if _, ok := c.reverse(i).(positioned); ok {
			if err := c.wait(ctx, c.Settle); err != nil {
				return err
			}
			break
		}
	}
	pos, err := c.backPositions(ctx)
	if err != nil {
		return err
	}
	for i := range pos {
		if i >= len(p.positions) || pos[i] != p.positions[i] {
			addFinding(p, findingWritesGoingOn, true)
			return ErrBlocked
		}
	}
	codes, err := c.behind(ctx, saved)
	if err != nil {
		return err
	}
	for _, code := range codes {
		addFinding(p, code, true)
	}
	if len(codes) > 0 {
		return ErrBlocked
	}
	for i := range c.Sync.sets {
		if s, ok := c.reverse(i).(interface {
			Sequences(context.Context) (int, error)
		}); ok {
			if _, err := s.Sequences(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

// ways is how many of the move's databases have a way back.
func (c *Cut) ways() int {
	n := 0
	for _, d := range c.Sync.sets {
		if set, ok := d.(*pgSet); ok && set.twoWay {
			n++
		}
	}
	return n
}

// wayBackMark is what backPending holds.
type wayBackMark struct {
	Pending bool `json:"pending"`
}

// pendingBack reports whether w says a way back was being made that its
// cutover never saved. A mark that can't be read says nothing.
func pendingBack(w *transfer.Workspace) bool {
	var m wayBackMark
	return w.Load(backPending, &m) == nil && m.Pending
}

// dropBack takes the way back off the target when a cutover didn't happen:
// no name points there, so nothing will ever read the slot, and a slot
// nobody reads holds the target's WAL until its disk is full. It touches
// the target only, since the source is still the live side, and a slot it
// can't drop is a finding. It runs on a context of its own, as endBack
// does.
func (c *Cut) dropBack(ctx context.Context, w *transfer.Workspace, p *CutPlan) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
	defer cancel()
	left := false
	for _, d := range c.Sync.sets {
		if set, ok := d.(*pgSet); ok && set.twoWay && set.endBack(ctx) != nil {
			addFinding(p, findingWayBackLeft, false)
			left = true
		}
	}
	if !left && w != nil {
		_ = w.Save(backPending, wayBackMark{})
	}
}

// endBack ends every database's way back after a return: the old side's
// subscription is stopped and the target's slot and publication dropped.
// It reports whether all of it went. It runs on a context of its own,
// since DNS has changed by now and an interrupt shouldn't leave a slot
// holding the target's WAL.
func (c *Cut) endBack(ctx context.Context) bool {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
	defer cancel()
	ok := true
	for i, d := range c.Sync.sets {
		set, is := d.(*pgSet)
		if !is || !set.twoWay {
			continue
		}
		if rev := c.reverse(i); rev != nil && rev.Stop(ctx) != nil {
			ok = false
			continue
		}
		if set.endBack(ctx) != nil {
			ok = false
		}
	}
	return ok
}
