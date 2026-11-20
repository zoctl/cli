package datasync

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/zoctl/cli/internal/transfer"
	"github.com/zoctl/cli/internal/transfer/objects"
)

var (
	// ErrApproval means the approval typed doesn't match the plan.
	ErrApproval = errors.New("datasync: not approved")
	// ErrBlocked means a dataset has a blocking finding, so the plan can't
	// be approved until the customer fixes it.
	ErrBlocked = errors.New("datasync: a dataset is blocked")
	// ErrStop means a run ended and the target's writes couldn't be
	// stopped: a subscription may still be enabled.
	ErrStop = errors.New("datasync: the target's writes could not be stopped")
)

// maxFailedRounds is how many rounds in a row may end on a failed storage
// or database call before the run gives up.
const maxFailedRounds = 3

// retryable reports whether the next round can make up for err: a storage
// or database call that failed, not a refusal, a limit or a cancellation.
func retryable(err error) bool {
	return errors.Is(err, objects.ErrTransport) || errors.Is(err, objects.ErrStatus) || errors.Is(err, ErrPostgres)
}

// dataset is one kind of copy. Plan and Check read both sides; Start, Round
// and Stop write the target only. In a reverse sync (back.go) the side
// written is the old one and the side read is the target.
type dataset interface {
	// Plan reads both sides and says what Start would do.
	Plan(ctx context.Context) (DatasetPlan, error)
	// Start does the approved plan: the first copy.
	Start(ctx context.Context, p DatasetPlan, b *transfer.Budget) (DatasetStatus, error)
	// Round catches up what changed since.
	Round(ctx context.Context, b *transfer.Budget) (DatasetStatus, error)
	// Check compares the two sides, reading only.
	Check(ctx context.Context) (DatasetStatus, error)
	// Stop ends this approval's writes on the target.
	Stop(ctx context.Context) error
	Close()
}

// DatasetPlan is what one dataset's first copy would do. It holds counts and
// a digest; the object list behind it stays in memory.
type DatasetPlan struct {
	Kind      string   `json:"kind"`
	Copy      int64    `json:"copy"`
	CopyBytes int64    `json:"copy_bytes"`
	Same      int64    `json:"same"`
	Extra     int64    `json:"extra"`
	Tables    int64    `json:"tables,omitempty"`
	Findings  []string `json:"findings,omitempty"`
	Blocked   bool     `json:"blocked,omitempty"`
	Digest    string   `json:"digest"`

	work any
}

// Plan is every dataset's plan and the digest an approval must repeat.
type Plan struct {
	Datasets []DatasetPlan `json:"datasets"`
	Limits   Limits        `json:"limits"`
	Digest   string        `json:"digest"`
}

// Blocked reports whether any dataset can't start.
func (p Plan) Blocked() bool {
	for _, d := range p.Datasets {
		if d.Blocked {
			return true
		}
	}
	return false
}

// DatasetStatus is one dataset's progress: counts, never names.
type DatasetStatus struct {
	Kind    string `json:"kind"`
	Copied  int64  `json:"copied"`
	Bytes   int64  `json:"bytes"`
	Failed  int64  `json:"failed"`
	Pending int64  `json:"pending"`
	Extra   int64  `json:"extra"`
	// LagSeconds is how far a replicated database is behind its source.
	LagBytes   int64    `json:"lag_bytes"`
	LagSeconds int      `json:"lag_seconds"`
	Verified   bool     `json:"verified"`
	Findings   []string `json:"findings,omitempty"`

	// extras names what the target has that the source doesn't; see
	// objects.Verification.
	extras string
}

// Status is a sync's state as the workspace keeps it and a console sees it.
type Status struct {
	Stage    string          `json:"stage"`
	Plan     string          `json:"plan"`
	Rounds   int             `json:"rounds"`
	Datasets []DatasetStatus `json:"datasets"`
	Observed time.Time       `json:"observed"`
	Until    time.Time       `json:"until"`
}

// Lag is the largest lag of any dataset.
func (s Status) Lag() int {
	n := 0
	for _, d := range s.Datasets {
		n = max(n, d.LagSeconds)
	}
	return n
}

// Verified is true when every dataset checked out on its last check.
func (s Status) Verified() bool {
	for _, d := range s.Datasets {
		if !d.Verified {
			return false
		}
	}
	return len(s.Datasets) > 0
}

// Findings is every dataset's finding codes, each once.
func (s Status) Findings() []string {
	seen := map[string]bool{}
	var out []string
	for _, d := range s.Datasets {
		for _, f := range d.Findings {
			if !seen[f] {
				seen[f] = true
				out = append(out, f)
			}
		}
	}
	return out
}

// Sync is one move's datasets, opened with the credentials of both sides.
type Sync struct {
	move Move
	sets []dataset
	now  func() time.Time
	// back is a reverse sync: the target is read and the old side written.
	back bool
}

// Options are the parts a Sync takes from its caller.
type Options struct {
	// Env reads a credential. The caller decides where from; nothing here
	// keeps or prints what it returns.
	Env    func(string) string
	Client *http.Client
	Now    func() time.Time
}

// Open opens every dataset of m. A dataset's credential missing, or a spec
// refused, fails before any call. A move with no data has nothing to sync,
// and is ErrNoData.
func Open(m Move, o Options) (*Sync, error) {
	if !m.HasData() {
		return nil, ErrNoData
	}
	return open(m, o)
}

// open is Open for a cutover too, which a move with no data still has.
func open(m Move, o Options) (*Sync, error) {
	if o.Env == nil {
		return nil, ErrMove
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	s := &Sync{move: m, now: o.Now}
	for _, d := range m.Datasets {
		ds, err := openDataset(d, o)
		if err != nil {
			s.Close()
			return nil, err
		}
		s.sets = append(s.sets, ds)
	}
	return s, nil
}

// Close releases every dataset's connections.
func (s *Sync) Close() {
	for _, d := range s.sets {
		d.Close()
	}
}

// Plan reads both sides of every dataset. It writes nothing anywhere.
//
// Every plan has a digest of its own, even of the same data: an approval
// is for one plan, and a plan made again after a run that failed, or for a
// database's next window, has to be approvable.
func (s *Sync) Plan(ctx context.Context) (Plan, error) {
	p := Plan{Limits: s.move.Limits}
	parts := [][]byte{[]byte("datasync-plan-v2"), []byte(rand.Text()), []byte(s.move.digest()), []byte(strconv.FormatBool(s.back)),
		[]byte(strconv.FormatInt(p.Limits.MaxObjects, 10)), []byte(strconv.FormatInt(p.Limits.MaxBytes, 10)), []byte(strconv.Itoa(p.Limits.Hours))}
	for _, d := range s.sets {
		dp, err := d.Plan(ctx)
		if err != nil {
			return Plan{}, err
		}
		p.Datasets = append(p.Datasets, dp)
		parts = append(parts, []byte(dp.Digest))
	}
	p.Digest = transfer.Digest(parts...)
	return p, nil
}

// RunOptions shape the catch-up loop.
type RunOptions struct {
	// Every is the time between catch-up rounds, a second at least.
	Every time.Duration
	// Rounds stops after that many catch-up rounds; zero runs until the
	// approval runs out.
	Rounds int
	// Report, when set, gets the status after every step.
	Report func(Status)
}

// Run does an approved plan, then catches up every o.Every until the
// approval's time or limits run out, o.Rounds is reached or ctx ends, and
// stops the target's writes. The status after every step is saved in the
// workspace as status.json.
//
// approved must be the plan's digest, and the workspace spends it, so one
// approval runs once even across restarts. The run holds the workspace's
// lock: a second sync or a cutover gets transfer.ErrBusy until it ends.
//
// A failed storage or database call doesn't end the run. The object it hit
// counts as failed and the next round copies it; only maxFailedRounds
// rounds failing in a row do.
func (s *Sync) Run(ctx context.Context, w *transfer.Workspace, p Plan, approved string, o RunOptions) (Status, error) {
	every, report := o.Every, o.Report
	if p.Digest == "" || subtle.ConstantTimeCompare([]byte(approved), []byte(p.Digest)) != 1 {
		return Status{}, ErrApproval
	}
	if p.Blocked() {
		return Status{}, ErrBlocked
	}
	if len(p.Datasets) != len(s.sets) || every < time.Second {
		return Status{}, ErrMove
	}
	unlock, err := w.Lock()
	if err != nil {
		return Status{}, err
	}
	defer unlock()
	// After a cutover the target has writes the source doesn't, and a sync
	// would copy the source's old data over them. A reverse sync is the
	// other way round: it writes the old side, so it runs only while a
	// finished cutover stands.
	if err := s.allowed(w); err != nil {
		return Status{}, err
	}
	if err := w.Spend(p.Digest); err != nil {
		return Status{}, err
	}
	// Round(0) drops the monotonic reading, so the approval ends by the
	// wall clock even when the machine slept through part of it.
	start := s.now().Round(0)
	until := start.Add(time.Duration(s.move.Limits.Hours) * time.Hour)
	b := transfer.NewBudget(transfer.Limits{MaxObjects: s.move.Limits.MaxObjects, MaxBytes: s.move.Limits.MaxBytes, Until: until}, s.now)
	st := Status{Stage: "approved", Plan: p.Digest, Datasets: make([]DatasetStatus, len(s.sets)), Until: until}
	emit := func(stage string) {
		st.Stage = stage
		st.Observed = s.now()
		_ = w.Save(s.statusKey(), st)
		if report != nil {
			report(st)
		}
	}
	emit("approved")
	// One object can take hours, so the copies end with the approval too,
	// not only between two of them.
	run, cancelRun := context.WithTimeout(ctx, max(0, until.Sub(s.now())))
	defer cancelRun()

	// Stop runs however the loop ends, a panic included, and on a context
	// of its own, so a cancelled run still ends its writes on the target.
	stopped := false
	stop := func() error {
		stopped = true
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
		defer cancel()
		var first error
		for _, d := range s.sets {
			if err := d.Stop(sctx); err != nil && first == nil {
				first = err
			}
		}
		return first
	}
	defer func() {
		if !stopped {
			_ = stop()
		}
	}()

	var runErr error
	failed := 0
	for i, d := range s.sets {
		ds, err := d.Start(run, p.Datasets[i], b)
		st.Datasets[i] = ds
		if err == nil {
			continue
		}
		// A first copy that stopped on a failed storage call is picked up
		// by the next round, which lists both sides again. Anything else,
		// such as a subscription that couldn't be made, ends the run.
		if ds.Kind == "objects" && retryable(err) && run.Err() == nil {
			failed = 1
			continue
		}
		runErr = err
		break
	}
	emit("running")
	for runErr == nil {
		if b.Expired() || o.Rounds > 0 && st.Rounds >= o.Rounds {
			break
		}
		t := time.NewTimer(max(0, min(every, until.Sub(s.now())+time.Second)))
		select {
		case <-ctx.Done():
			runErr = ctx.Err()
		case <-t.C:
		}
		t.Stop()
		if runErr != nil || b.Expired() {
			break
		}
		// The lock keeps a cutover out while this runs. This is for a
		// workspace on a file system where a lock doesn't hold.
		if err := s.allowed(w); err != nil {
			runErr = err
			break
		}
		var roundErr error
		for i, d := range s.sets {
			ds, err := d.Round(run, b)
			st.Datasets[i] = merge(st.Datasets[i], ds, err)
			if err == nil {
				continue
			}
			if !retryable(err) || run.Err() != nil {
				runErr = err
				break
			}
			roundErr = err
		}
		st.Rounds++
		switch {
		case runErr != nil:
		case roundErr == nil:
			failed = 0
		default:
			if failed++; failed >= maxFailedRounds {
				runErr = roundErr
			}
		}
		emit("running")
	}
	switch {
	case runErr == nil:
	case ctx.Err() != nil:
		// A call the interrupt cut short is the interrupt, not a failure.
		runErr = ctx.Err()
	case run.Err() != nil:
		// A copy the approval's time cut short is the limit reached.
		runErr = transfer.ErrBudget
	}
	stopErr := stop()
	// The last check reads both sides, whatever ended the loop.
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Minute)
	for i, d := range s.sets {
		if ds, err := d.Check(cctx); err == nil {
			st.Datasets[i] = check(st.Datasets[i], ds)
		} else {
			st.Datasets[i].Verified = false
		}
	}
	cancel()
	switch {
	case stopErr != nil:
		// Whatever ended the loop, a subscription that may still be
		// enabled isn't a clean end.
		runErr = ErrStop
		emit("failed")
	case errors.Is(runErr, transfer.ErrBudget), runErr == nil:
		runErr = nil
		if st.Verified() {
			emit("verified")
		} else {
			emit("paused")
		}
	case errors.Is(runErr, ErrCutOver), errors.Is(runErr, ErrNotCutOver):
		emit("paused")
	case errors.Is(runErr, context.Canceled):
		emit("interrupted")
	default:
		emit("failed")
	}
	return st, runErr
}

// Check reads both sides of every dataset and compares them. A cutover
// takes one of these, taken while the source's writes are paused.
func (s *Sync) Check(ctx context.Context) (Status, error) {
	st := Status{Stage: "verified", Observed: s.now()}
	for _, d := range s.sets {
		ds, err := d.Check(ctx)
		if err != nil {
			return Status{}, err
		}
		st.Datasets = append(st.Datasets, ds)
	}
	if !st.Verified() {
		st.Stage = "running"
	}
	return st, nil
}

// merge adds a round's work to the running totals. A round that ended on an
// error counted only what it copied, so the rest keeps its last reading.
func merge(total, round DatasetStatus, err error) DatasetStatus {
	total.Copied += round.Copied
	total.Bytes += round.Bytes
	if err != nil {
		total.Verified = false
		return total
	}
	total.Kind = round.Kind
	total.Failed = round.Failed
	total.Pending = round.Pending
	total.Extra = round.Extra
	total.LagSeconds = round.LagSeconds
	total.LagBytes = round.LagBytes
	total.Verified = round.Verified
	total.Findings = round.Findings
	return total
}

// check puts a check's answer on the totals.
func check(total, c DatasetStatus) DatasetStatus {
	total.Kind = c.Kind
	total.Pending = c.Pending
	total.Extra = c.Extra
	total.LagSeconds = c.LagSeconds
	total.LagBytes = c.LagBytes
	total.Verified = c.Verified
	total.Findings = c.Findings
	return total
}

// allowed reports whether this sync may write now: a sync while the source
// is the side taking writes, a reverse sync while a finished cutover
// stands. A workspace that can't be read allows neither.
func (s *Sync) allowed(w *transfer.Workspace) error {
	if s.back {
		if ok, err := standing(w); err != nil || !ok {
			return ErrNotCutOver
		}
		return nil
	}
	if cut, err := CutOver(w); err != nil || cut {
		return ErrCutOver
	}
	return nil
}

// Back reports whether s is a reverse sync.
func (s *Sync) Back() bool { return s.back }

// statusKey is where the workspace keeps this sync's last status. A reverse
// sync has its own, so it doesn't stand in for the move's.
func (s *Sync) statusKey() string {
	if s.back {
		return "status-back"
	}
	return "status"
}
