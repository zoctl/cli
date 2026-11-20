package datasync

import (
	"context"
	"errors"
	"io"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/zoctl/cli/internal/transfer"
	"github.com/zoctl/cli/internal/transfer/dns"
	"github.com/zoctl/cli/internal/transfer/objects"
)

// A sync left running when the cutover finishes would copy the source over
// the target's new writes. It stops before its next round.
func TestSyncStopsWhenTheMoveCutsOver(t *testing.T) {
	src := &bucket{data: map[string]string{"a": "old"}}
	dst := &bucket{data: map[string]string{}}
	s := testSync(t, src, dst, time.Now)
	p, err := s.Plan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	w := workspace(t)
	done := false
	report := func(st Status) {
		if st.Stage == "running" && !done {
			done = true
			if err := w.Save(cutoverState, cutState{At: time.Now(), Complete: true}); err != nil {
				t.Fatal(err)
			}
			dst.mu.Lock()
			dst.data["a"] = "new"
			dst.mu.Unlock()
		}
	}
	st, err := s.Run(context.Background(), w, p, p.Digest, RunOptions{Every: time.Second, Rounds: 3, Report: report})
	if !errors.Is(err, ErrCutOver) || st.Stage != "paused" || st.Rounds != 0 {
		t.Fatalf("stage %q rounds %d err %v", st.Stage, st.Rounds, err)
	}
	if dst.data["a"] != "new" {
		t.Fatalf("the running sync put %q back over the target's write", dst.data["a"])
	}
}

// Every plan has a digest of its own, so an approval that was spent never
// stands in the way of the next plan, and a digest read off an earlier
// plan approves nothing.
func TestPlansNeverShareADigest(t *testing.T) {
	ctx := context.Background()
	s := testSync(t, &bucket{data: map[string]string{"a": "1"}}, &bucket{data: map[string]string{}}, time.Now)
	p1, _ := s.Plan(ctx)
	p2, _ := s.Plan(ctx)
	if p1.Digest == p2.Digest || !transfer.ValidDigest(p1.Digest) {
		t.Fatal("two sync plans share a digest")
	}
	if _, err := s.Run(ctx, workspace(t), p2, p1.Digest, RunOptions{Every: time.Second}); !errors.Is(err, ErrApproval) {
		t.Fatalf("an earlier plan's digest started a sync: %v", err)
	}
	f := newCut(t)
	c1, _ := f.cut.Plan(ctx, f.w, false)
	c2, _ := f.cut.Plan(ctx, f.w, false)
	if c1.Digest == c2.Digest || !transfer.ValidDigest(c1.Digest) {
		t.Fatal("two cutover plans share a digest")
	}
	if _, err := f.cut.Apply(ctx, f.w, c2, c1.Digest); !errors.Is(err, ErrApproval) || f.zone.changes != 0 {
		t.Fatalf("an earlier plan's digest cut over: %v", err)
	}
}

// failing is a source whose reads fail while fail is set.
type failing struct {
	*bucket
	fail bool
}

func (f *failing) Get(ctx context.Context, key string) (io.ReadCloser, objects.Meta, error) {
	if f.fail {
		return nil, objects.Meta{}, objects.ErrTransport
	}
	return f.bucket.Get(ctx, key)
}

// A run whose reads failed leaves nothing in the way of the next plan.
func TestFailedRunCanBePlannedAgain(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	src := &failing{bucket: &bucket{data: map[string]string{"a": "1"}}, fail: true}
	dst := &bucket{data: map[string]string{}}
	m, err := ParseMove([]byte(moveJSON))
	if err != nil {
		t.Fatal(err)
	}
	s := &Sync{move: m, now: time.Now, sets: []dataset{newObjectSet("pair", "", src, dst)}}
	w := workspace(t)
	p1, err := s.Plan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	st, err := s.Run(ctx, w, p1, p1.Digest, RunOptions{Every: time.Second, Rounds: 1})
	if err != nil || st.Stage != "paused" || st.Verified() || dst.puts != 0 {
		t.Fatalf("first run: stage %q err %v puts %d", st.Stage, err, dst.puts)
	}
	src.fail = false
	p2, err := s.Plan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	st, err = s.Run(ctx, w, p2, p2.Digest, RunOptions{Every: time.Second, Rounds: 1})
	if err != nil || st.Stage != "verified" || dst.data["a"] != "1" {
		t.Fatalf("second run: stage %q err %v", st.Stage, err)
	}
}

// A Postgres move is approved once for the copy and again for every window
// after it. The second and third plans describe the same state.
func TestPostgresApprovedThreeTimes(t *testing.T) {
	t.Parallel()
	src, dst := sourcePG(), newPG()
	for k := range src.tables {
		dst.tables[k] = 0
	}
	m := Move{Version: 1, Source: "aws", Target: "gcp", Limits: Limits{MaxObjects: 1, MaxBytes: 1, Hours: 1}}
	s := &Sync{move: m, sets: []dataset{pgSetFor(src, dst, false)}, now: time.Now}
	w := workspace(t)
	for i := 1; i <= 3; i++ {
		p, err := s.Plan(context.Background())
		if err != nil || p.Blocked() {
			t.Fatalf("plan %d: %v %+v", i, err, p.Datasets)
		}
		if _, err := s.Run(context.Background(), w, p, p.Digest, RunOptions{Every: time.Second, Rounds: 1}); err != nil {
			t.Fatalf("approval %d refused: %v", i, err)
		}
	}
}

// scripted is a dataset whose rounds end the way the test says.
type scripted struct {
	rounds  []error
	stopErr error
	n       int
	stops   int
}

func (d *scripted) Plan(context.Context) (DatasetPlan, error) {
	return DatasetPlan{Kind: "objects", Digest: transfer.Digest([]byte("scripted"))}, nil
}

func (d *scripted) Start(context.Context, DatasetPlan, *transfer.Budget) (DatasetStatus, error) {
	return DatasetStatus{Kind: "objects", Verified: true}, nil
}

func (d *scripted) Round(context.Context, *transfer.Budget) (DatasetStatus, error) {
	var err error
	if d.n < len(d.rounds) {
		err = d.rounds[d.n]
	}
	d.n++
	return DatasetStatus{Kind: "objects", Verified: err == nil}, err
}

func (d *scripted) Check(context.Context) (DatasetStatus, error) {
	return DatasetStatus{Kind: "objects", Verified: true}, nil
}

func (d *scripted) Stop(context.Context) error { d.stops++; return d.stopErr }
func (d *scripted) Close()                     {}

func runScripted(t *testing.T, d *scripted, rounds int) (Status, error) {
	t.Helper()
	m := Move{Version: 1, Source: "aws", Target: "gcp", Limits: Limits{MaxObjects: 1, MaxBytes: 1, Hours: 1}}
	s := &Sync{move: m, sets: []dataset{d}, now: time.Now}
	p, err := s.Plan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return s.Run(context.Background(), workspace(t), p, p.Digest, RunOptions{Every: time.Second, Rounds: rounds})
}

// A round that fails on a storage or database call is tried again. Only
// maxFailedRounds in a row end the run, and an error of another kind ends
// it at once.
func TestRoundsThatFail(t *testing.T) {
	blip := objects.ErrTransport
	t.Run("recovers", func(t *testing.T) {
		t.Parallel()
		d := &scripted{rounds: []error{blip, ErrPostgres, nil}}
		st, err := runScripted(t, d, 3)
		if err != nil || st.Stage != "verified" || st.Rounds != 3 || d.stops != 1 {
			t.Fatalf("stage %q rounds %d stops %d err %v", st.Stage, st.Rounds, d.stops, err)
		}
	})
	t.Run("three in a row", func(t *testing.T) {
		t.Parallel()
		d := &scripted{rounds: []error{nil, blip, blip, blip, nil}}
		st, err := runScripted(t, d, 9)
		if !errors.Is(err, blip) || st.Stage != "failed" || st.Rounds != 4 || d.stops != 1 {
			t.Fatalf("stage %q rounds %d stops %d err %v", st.Stage, st.Rounds, d.stops, err)
		}
	})
	t.Run("another error", func(t *testing.T) {
		t.Parallel()
		d := &scripted{rounds: []error{ErrMove, nil}}
		st, err := runScripted(t, d, 9)
		if !errors.Is(err, ErrMove) || st.Stage != "failed" || st.Rounds != 1 {
			t.Fatalf("stage %q rounds %d err %v", st.Stage, st.Rounds, err)
		}
	})
}

// A subscription that couldn't be disabled may still be writing to the
// target, so a run that ended well otherwise still fails.
func TestStopThatFailsIsNotACleanEnd(t *testing.T) {
	t.Parallel()
	d := &scripted{stopErr: ErrPostgres}
	st, err := runScripted(t, d, 1)
	if !errors.Is(err, ErrStop) || st.Stage != "failed" || d.stops != 1 {
		t.Fatalf("stage %q stops %d err %v", st.Stage, d.stops, err)
	}
}

// One workspace runs one sync or one cutover at a time. The one refused
// spends no approval.
func TestWorkspaceRunsOneThingAtATime(t *testing.T) {
	ctx := context.Background()
	f := newCut(t)
	s := f.cut.Sync
	p, err := s.Plan(ctx)
	if err != nil || p.Blocked() {
		t.Fatalf("plan: %v %+v", err, p.Datasets)
	}
	second, err := s.Plan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cp, err := f.cut.Plan(ctx, f.w, false)
	if err != nil || cp.Blocked {
		t.Fatalf("cutover plan: %v %v", err, cp.Findings)
	}
	tried := false
	report := func(st Status) {
		if st.Stage != "running" || tried {
			return
		}
		tried = true
		if _, err := s.Run(ctx, f.w, second, second.Digest, RunOptions{Every: time.Second}); !errors.Is(err, transfer.ErrBusy) {
			t.Errorf("a second sync in the workspace: %v", err)
		}
		if _, err := f.cut.Apply(ctx, f.w, cp, cp.Digest); !errors.Is(err, transfer.ErrBusy) {
			t.Errorf("a cutover under a running sync: %v", err)
		}
	}
	if _, err := s.Run(ctx, f.w, p, p.Digest, RunOptions{Every: time.Second, Rounds: 1, Report: report}); err != nil {
		t.Fatal(err)
	}
	if !tried || f.zone.changes != 0 {
		t.Fatalf("tried %v, DNS changes %d", tried, f.zone.changes)
	}
	// Neither refusal spent its approval.
	if _, err := f.cut.Apply(ctx, f.w, cp, cp.Digest); err != nil {
		t.Fatalf("the cutover's approval was spent by the refusal: %v", err)
	}
}

// stuck is a source whose first read hangs until its context ends.
type stuck struct {
	*bucket
	once sync.Once
}

func (s *stuck) Get(ctx context.Context, key string) (io.ReadCloser, objects.Meta, error) {
	hung := false
	s.once.Do(func() { hung = true })
	if hung {
		<-ctx.Done()
		return nil, objects.Meta{}, objects.ErrTransport
	}
	return s.bucket.Get(ctx, key)
}

// The approval's hours end a copy that is under way, not only the gap
// between two copies.
func TestCopyEndsWithTheApproval(t *testing.T) {
	src := &stuck{bucket: &bucket{data: map[string]string{"a": "1"}}}
	dst := &bucket{data: map[string]string{}}
	m, err := ParseMove([]byte(moveJSON))
	if err != nil {
		t.Fatal(err)
	}
	// The clock reads the start once, then 100ms short of the hour.
	base := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	calls := 0
	now := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		if calls++; calls == 1 {
			return base
		}
		return base.Add(time.Hour - 100*time.Millisecond)
	}
	s := &Sync{move: m, now: now, sets: []dataset{newObjectSet("pair", "", src, dst)}}
	p, err := s.Plan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	began := time.Now()
	st, err := s.Run(context.Background(), workspace(t), p, p.Digest, RunOptions{Every: time.Second})
	if err != nil || st.Stage != "paused" || st.Verified() || dst.puts != 0 {
		t.Fatalf("stage %q err %v puts %d", st.Stage, err, dst.puts)
	}
	if time.Since(began) > 5*time.Second {
		t.Fatalf("the hung read held the run for %v", time.Since(began))
	}
}

// flaky is a zone whose Replace fails after the first n calls, or kills the
// process there.
type flaky struct {
	*memZone
	n, calls int
	panics   bool
}

func (z *flaky) Replace(ctx context.Context, old, new *dns.RecordSet) error {
	z.calls++
	if z.calls > z.n {
		if z.panics {
			panic("the process dies here")
		}
		return dns.ErrProvider
	}
	return z.memZone.Replace(ctx, old, new)
}

func (f *cutFixture) at(t *testing.T, want dns.RecordSet) bool {
	t.Helper()
	got, ok, err := f.zone.Get(context.Background(), want.Name, want.Type)
	if err != nil {
		t.Fatal(err)
	}
	return ok && got.Equal(want)
}

// halfCut leaves www at the target and the AAAA not made: one change
// landed, the next failed, and so did the undo.
func halfCut(t *testing.T) *cutFixture {
	t.Helper()
	f := newCut(t)
	ctx := context.Background()
	f.cut.Zone = &flaky{memZone: f.zone, n: 1}
	p, err := f.cut.Plan(ctx, f.w, false)
	if err != nil || p.Blocked {
		t.Fatalf("plan: %v %v", err, p.Findings)
	}
	_, err = f.cut.Apply(ctx, f.w, p, p.Digest)
	if !errors.Is(err, ErrCutPartial) || !errors.Is(err, dns.ErrProvider) {
		t.Fatalf("a cutover that couldn't be undone: %v", err)
	}
	if !f.at(t, wwwNew) || f.at(t, v6New) {
		t.Fatal("the fixture isn't half cut over")
	}
	f.cut.Zone = f.zone
	return f
}

// A cutover that stopped part way and couldn't be put back is still a
// cutover to the workspace: a sync is refused and a return is planned.
func TestHalfCutoverIsNotRecordedAsReturned(t *testing.T) {
	f := halfCut(t)
	ctx := context.Background()
	if cut, err := CutOver(f.w); err != nil || !cut {
		t.Fatalf("the workspace says nothing is cut over: %v", err)
	}
	sp, err := f.cut.Sync.Plan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.cut.Sync.Run(ctx, f.w, sp, sp.Digest, RunOptions{Every: time.Second}); !errors.Is(err, ErrCutOver) {
		t.Fatalf("a sync under a half cutover: %v", err)
	}
	back, err := f.cut.Plan(ctx, f.w, true)
	if err != nil || back.Blocked || back.Change != 1 || back.Already != 1 {
		t.Fatalf("return plan: %v %+v", err, back)
	}
	if _, err := f.cut.Apply(ctx, f.w, back, back.Digest); err != nil {
		t.Fatal(err)
	}
	if !f.at(t, wwwOld) || f.at(t, v6New) {
		t.Fatal("the return didn't put the records back")
	}
	if cut, _ := CutOver(f.w); cut {
		t.Fatal("still cut over after the return")
	}
}

// The other way out of a half cutover is to finish it. That plan makes
// only the changes still missing, and doesn't set the sequences again.
func TestHalfCutoverCanBeFinished(t *testing.T) {
	f := halfCut(t)
	ctx := context.Background()
	f.dst.setvals[`"public"."orders_id_seq"`] = 99
	p, err := f.cut.Plan(ctx, f.w, false)
	if err != nil || !p.Resume || p.Blocked || p.Change != 1 || p.Created != 1 || p.Already != 2 {
		t.Fatalf("plan: %v %+v", err, p)
	}
	// It fails again: the zone is left half way, and says so.
	f.cut.Zone = &flaky{memZone: f.zone}
	if _, err := f.cut.Apply(ctx, f.w, p, p.Digest); !errors.Is(err, ErrCutPartial) {
		t.Fatalf("a finish that failed: %v", err)
	}
	f.cut.Zone = f.zone
	// Part way is not done: nothing for a runner to report as a switch.
	if _, ok, err := CutDone(f.w); err != nil || ok {
		t.Fatalf("a cutover part way reads as done: %v %v", ok, err)
	}
	if p, err = f.cut.Plan(ctx, f.w, false); err != nil || !p.Resume {
		t.Fatalf("plan: %v %+v", err, p)
	}
	done, err := f.cut.Apply(ctx, f.w, p, p.Digest)
	if err != nil {
		t.Fatal(err)
	}
	// Finished on a later run, it was made when its first record changed.
	if again, ok, _ := CutDone(f.w); !ok || done.At.IsZero() || !again.At.Equal(done.At) {
		t.Fatalf("finished at %v, kept as %+v", done.At, again)
	}
	if !f.at(t, wwwNew) || !f.at(t, v6New) || !f.at(t, apiNew) {
		t.Fatal("the records aren't all at the target")
	}
	if done.ReturnUntil.IsZero() || f.dst.setvals[`"public"."orders_id_seq"`] != 99 {
		t.Fatalf("return until %v, sequence %d", done.ReturnUntil, f.dst.setvals[`"public"."orders_id_seq"`])
	}
	if _, err := f.cut.Plan(ctx, f.w, false); !errors.Is(err, ErrCutOver) {
		t.Fatalf("a cutover after it finished: %v", err)
	}
	// And it can still be switched back, every record of it.
	back, err := f.cut.Plan(ctx, f.w, true)
	if err != nil || back.Blocked || back.Change != 2 {
		t.Fatalf("return plan: %v %+v", err, back)
	}
	if _, err := f.cut.Apply(ctx, f.w, back, back.Digest); err != nil {
		t.Fatal(err)
	}
	if !f.at(t, wwwOld) || f.at(t, v6New) {
		t.Fatal("the return didn't put the records back")
	}
}

// The process dies between two record changes. Both ways out stay open.
func TestCrashBetweenRecordChanges(t *testing.T) {
	f := newCut(t)
	ctx := context.Background()
	f.cut.Zone = &flaky{memZone: f.zone, n: 1, panics: true}
	p, err := f.cut.Plan(ctx, f.w, false)
	if err != nil {
		t.Fatal(err)
	}
	func() {
		defer func() { _ = recover() }()
		_, _ = f.cut.Apply(ctx, f.w, p, p.Digest)
	}()
	f.cut.Zone = f.zone
	if !f.at(t, wwwNew) || f.at(t, v6New) {
		t.Fatal("the fixture isn't half cut over")
	}
	fwd, err := f.cut.Plan(ctx, f.w, false)
	if err != nil || !fwd.Resume || fwd.Blocked || fwd.Change != 1 {
		t.Fatalf("finishing: %v %+v", err, fwd)
	}
	back, err := f.cut.Plan(ctx, f.w, true)
	if err != nil || back.Blocked || back.Change != 1 {
		t.Fatalf("returning: %v %+v", err, back)
	}
	if _, err := f.cut.Apply(ctx, f.w, back, back.Digest); err != nil {
		t.Fatal(err)
	}
	if !f.at(t, wwwOld) {
		t.Fatal("not back")
	}
}

// A crash after the old sets were saved and before any record changed is a
// cutover that didn't start: the next plan is a first one, with every check.
func TestCrashBeforeTheFirstChange(t *testing.T) {
	f := newCut(t)
	ctx := context.Background()
	f.cut.Zone = &flaky{memZone: f.zone, panics: true}
	p, _ := f.cut.Plan(ctx, f.w, false)
	func() {
		defer func() { _ = recover() }()
		_, _ = f.cut.Apply(ctx, f.w, p, p.Digest)
	}()
	f.cut.Zone = f.zone
	p, err := f.cut.Plan(ctx, f.w, false)
	if err != nil || p.Resume || p.Blocked || p.Change != 2 {
		t.Fatalf("plan: %v %+v", err, p)
	}
	f.slept = 0
	if _, err := f.cut.Apply(ctx, f.w, p, p.Digest); err != nil {
		t.Fatal(err)
	}
	if f.slept != time.Minute || !f.at(t, wwwNew) || !f.at(t, v6New) {
		t.Fatalf("slept %v", f.slept)
	}
}

// interrupted is a zone whose calls fail once their context is cancelled,
// and which cancels it after the first change, as Ctrl-C does.
type interrupted struct {
	*memZone
	cancel context.CancelFunc
}

func (z *interrupted) Replace(ctx context.Context, old, new *dns.RecordSet) error {
	if ctx.Err() != nil {
		return dns.ErrProvider
	}
	err := z.memZone.Replace(ctx, old, new)
	z.cancel()
	return err
}

func (z *interrupted) Get(ctx context.Context, name, typ string) (dns.RecordSet, bool, error) {
	if ctx.Err() != nil {
		return dns.RecordSet{}, false, dns.ErrProvider
	}
	return z.memZone.Get(ctx, name, typ)
}

// An interrupt between two changes fails the second one. The undo runs on
// a context of its own, so the first is put back and read back.
func TestInterruptedCutoverIsPutBack(t *testing.T) {
	f := newCut(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p, err := f.cut.Plan(ctx, f.w, false)
	if err != nil {
		t.Fatal(err)
	}
	f.cut.Zone = &interrupted{memZone: f.zone, cancel: cancel}
	_, err = f.cut.Apply(ctx, f.w, p, p.Digest)
	if err == nil || errors.Is(err, ErrCutPartial) {
		t.Fatalf("apply: %v", err)
	}
	if !f.at(t, wwwOld) || f.at(t, v6New) {
		t.Fatal("DNS isn't as it was")
	}
	if cut, err := CutOver(f.w); err != nil || cut {
		t.Fatalf("recorded as cut over: %v", err)
	}
}

// A cutover, a return, then the same cutover again.
func TestCutoverAgainAfterAReturn(t *testing.T) {
	f := newCut(t)
	ctx := context.Background()
	for i := 1; i <= 2; i++ {
		p, err := f.cut.Plan(ctx, f.w, false)
		if err != nil || p.Blocked || p.Resume {
			t.Fatalf("cutover %d plan: %v %+v", i, err, p)
		}
		if _, err := f.cut.Apply(ctx, f.w, p, p.Digest); err != nil {
			t.Fatalf("cutover %d: %v", i, err)
		}
		if !f.at(t, wwwNew) {
			t.Fatalf("cutover %d didn't change DNS", i)
		}
		if i == 2 {
			break
		}
		b, err := f.cut.Plan(ctx, f.w, true)
		if err != nil || b.Blocked {
			t.Fatalf("return plan: %v %v", err, b.Findings)
		}
		if _, err := f.cut.Apply(ctx, f.w, b, b.Digest); err != nil {
			t.Fatal(err)
		}
	}
}

// The first try is refused because a writer was still going. The customer
// stops it and plans again.
func TestRefusedCutoverCanBePlannedAgain(t *testing.T) {
	f := newCut(t)
	ctx := context.Background()
	p, err := f.cut.Plan(ctx, f.w, false)
	if err != nil {
		t.Fatal(err)
	}
	f.cut.Sleep = func(context.Context, time.Duration) error { f.src.lsn = "0/16B3999"; return nil }
	if _, err := f.cut.Apply(ctx, f.w, p, p.Digest); !errors.Is(err, ErrBlocked) || f.zone.changes != 0 {
		t.Fatalf("a cutover with a writer going: %v", err)
	}
	f.cut.Sleep = func(context.Context, time.Duration) error { return nil }
	p, err = f.cut.Plan(ctx, f.w, false)
	if err != nil || p.Blocked {
		t.Fatalf("second plan: %v %v", err, p.Findings)
	}
	if _, err := f.cut.Apply(ctx, f.w, p, p.Digest); err != nil {
		t.Fatalf("the writers stopped, yet the cutover can't run: %v", err)
	}
}

// twoCalls is a zone that changes a record's type in two calls, as Azure
// DNS has to, and stops between them: the old set gone, the new not there.
type twoCalls struct {
	*memZone
	stop bool
}

func (z *twoCalls) Replace(ctx context.Context, old, new *dns.RecordSet) error {
	if z.stop && old != nil && new != nil && old.Type != new.Type {
		if err := z.memZone.Replace(ctx, old, nil); err != nil {
			return err
		}
		return dns.ErrProvider
	}
	return z.memZone.Replace(ctx, old, new)
}

// A change of type that stopped half way leaves the name with nothing. That
// is not a zone no cutover has touched: a plan that took it for one would
// save "there was nothing here" over the address the first plan kept, and a
// return would then leave the name empty.
func TestHalfChangedTypeIsNotAFirstCutover(t *testing.T) {
	alias := dns.RecordSet{Name: wwwOld.Name, Type: "CNAME", TTL: 60, Values: []string{"lb.new.example.test."}}
	f := cutWith(t, []dns.RecordSet{wwwOld}, records(alias)...)
	ctx := context.Background()
	p, err := f.cut.Plan(ctx, f.w, false)
	if err != nil || p.Blocked || p.Change != 1 {
		t.Fatalf("plan: %v %+v", err, p)
	}
	f.cut.Zone = &twoCalls{memZone: f.zone, stop: true}
	if _, err := f.cut.Apply(ctx, f.w, p, p.Digest); !errors.Is(err, ErrCutPartial) {
		t.Fatalf("a type change that stopped half way: %v", err)
	}
	f.cut.Zone = f.zone
	if f.at(t, wwwOld) || f.at(t, alias) {
		t.Fatal("the test's zone didn't stop half way")
	}
	for _, back := range []bool{false, true} {
		again, err := f.cut.Plan(ctx, f.w, back)
		if err != nil || !again.Blocked || !slices.Contains(again.Findings, findingDNSChanged) {
			t.Fatalf("back %v: planned over a half-changed name: %v %+v", back, err, again)
		}
	}
	// Someone puts the address back by hand. Nothing changed after all, so
	// the next plan is a first one and the return still knows the address.
	f.zone.sets[wwwOld.Name+" A"] = wwwOld
	f.apply(t, false)
	if !f.at(t, alias) || f.at(t, wwwOld) {
		t.Fatal("the cutover didn't change the type")
	}
	f.apply(t, true)
	if !f.at(t, wwwOld) || f.at(t, alias) {
		t.Fatal("the return didn't bring the address back")
	}
}
