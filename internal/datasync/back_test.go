package datasync

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/zoctl/cli/internal/transfer/dns"
	"github.com/zoctl/cli/internal/transfer/objects"
)

const backName_ = "zoctl_back_orders"

// newStandby is newCut with a way back: the move's database is two-way, and
// the cut can read it the other way round, the target first and the old
// side written.
func newStandby(t *testing.T) (*cutFixture, *pgSet) {
	f := newCut(t)
	fwd := f.cut.Sync.sets[0].(*pgSet)
	fwd.twoWay, fwd.backName, fwd.peer = true, backName_, backName_
	rev := &pgSet{src: readOnly{f.dst}, dst: f.src, sourceURL: "postgres://repl:synthetic@new.example.test/shop",
		publication: backName_, slot: backName_, sub: backName_,
		twoWay: true, back: true, backName: backName_, peer: "zoctl_orders"}
	f.cut.back = []dataset{rev}
	return f, rev
}

// reverseOf is the reverse sync of f's move.
func reverseOf(f *cutFixture, sets ...dataset) *Sync {
	return &Sync{move: f.cut.Sync.move, sets: sets, now: func() time.Time { return f.now }, back: true}
}

func cutOver(t *testing.T, f *cutFixture) CutPlan {
	t.Helper()
	ctx := context.Background()
	p, err := f.cut.Plan(ctx, f.w, false)
	if err != nil || p.Blocked {
		t.Fatalf("cutover plan: %+v %v", p, err)
	}
	done, err := f.cut.Apply(ctx, f.w, p, p.Digest)
	if err != nil {
		t.Fatalf("cutover: %v", err)
	}
	return done
}

// built is the statements without the session's own log settings, which
// change nothing on the server.
func built(execs []string) []string {
	var out []string
	for _, sql := range execs {
		if !slices.Contains(quiet, sql) && !slices.Contains(loud, sql) {
			out = append(out, sql)
		}
	}
	return out
}

// wroteOnly fails the test when the old side was sent anything but the
// subscription's own statements.
func wroteOnly(t *testing.T, old *fakePG, from int) {
	t.Helper()
	for _, sql := range built(old.execs[from:]) {
		if !strings.HasPrefix(sql, "CREATE SUBSCRIPTION ") && !strings.HasPrefix(sql, "ALTER SUBSCRIPTION ") {
			t.Fatalf("the old side was sent %.40q", sql)
		}
	}
}

func TestMoveFileWayBack(t *testing.T) {
	const head = `{"version":1,"source":"aws","target":"gcp","limits":{"max_objects":1,"max_bytes":1,"hours":1},"datasets":[`
	db := func(name, extra string) string {
		return `{"name":"` + name + `","kind":"postgres","from_env":"SOURCE_DB","to_env":"TARGET_DB","publication":"p","slot":"s"` + extra + `}`
	}
	const both = `,"back_env":"STANDBY_DB","back_from_env":"TARGET_DB_REPL"`
	for name, tc := range map[string]struct {
		datasets    string
		ok, reverse bool
	}{
		"no way back":                 {db("a", ""), true, false},
		"a way back":                  {db("a", both), true, true},
		"every database or none":      {db("a", both) + "," + db("b", ""), false, false},
		"every database":              {db("a", both) + "," + db("b", both), true, true},
		"the source's name":           {db("a", `,"back_env":"SOURCE_DB","back_from_env":"TARGET_DB"`), false, false},
		"the target's name":           {db("a", `,"back_env":"TARGET_DB","back_from_env":"TARGET_DB"`), false, false},
		"no URL for the old side":     {db("a", `,"back_from_env":"TARGET_DB"`), false, false},
		"no URL to reach the target":  {db("a", `,"back_env":"STANDBY_DB"`), false, false},
		"the old side's URL as reach": {db("a", `,"back_env":"STANDBY_DB","back_from_env":"STANDBY_DB"`), false, false},
		"a bucket names none":         {`{"name":"u","kind":"objects","from":"s3://a?region=us-east-1","to":"gs://b","back_env":"STANDBY_X"}`, false, false},
		"buckets need nothing":        {`{"name":"u","kind":"objects","from":"s3://a?region=us-east-1","to":"gs://b"}`, true, true},
	} {
		m, err := ParseMove([]byte(head + tc.datasets + "]}"))
		if (err == nil) != tc.ok {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if err == nil && m.Reversible() != tc.reverse {
			t.Errorf("%s: reversible %v", name, m.Reversible())
		}
	}
}

// A reverse sync writes to the old side, so it runs only while a finished
// cutover stands: not before one, not part way through one, not after the
// move switched back.
func TestReverseSyncOnlyWhileCutOver(t *testing.T) {
	f, rev := newStandby(t)
	ctx := context.Background()
	// Before a cutover there is no way back to read, and the plan says so.
	s := reverseOf(f, rev)
	p, err := s.Plan(ctx)
	if err != nil || !p.Blocked() {
		t.Fatalf("plan before a cutover: %+v %v", p, err)
	}
	// Even a plan that isn't blocked is refused by the workspace.
	f.dst.pubs[backName_], f.dst.slots[backName_] = true, [3]string{"pgoutput", "logical", "true"}
	f.dst.pubTabs = f.src.pubTabs
	if p, err = s.Plan(ctx); err != nil || p.Blocked() {
		t.Fatalf("plan: %+v %v", p, err)
	}
	if _, err := s.Run(ctx, f.w, p, p.Digest, RunOptions{Every: time.Second, Rounds: 1}); !errors.Is(err, ErrNotCutOver) {
		t.Fatalf("a reverse sync before the cutover: %v", err)
	}
	if len(f.src.execs) != 0 {
		t.Fatalf("the source was written before the cutover: %q", f.src.execs)
	}
	// The refusal didn't spend the approval or mark the workspace.
	if ok, err := Standing(f.w); err != nil || ok {
		t.Fatalf("standing before a cutover: %v %v", ok, err)
	}
	delete(f.dst.pubs, backName_)
	delete(f.dst.slots, backName_)

	// A cutover that stopped part way isn't one that stands.
	f.zone.failOn = v6New.Name + " AAAA"
	cp, err := f.cut.Plan(ctx, f.w, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.cut.Apply(ctx, f.w, cp, cp.Digest); err == nil {
		t.Fatal("the cutover didn't stop")
	}
	if ok, _ := Standing(f.w); ok {
		t.Fatal("a cutover that stopped part way stands")
	}
}

func TestStandbyRoundTrip(t *testing.T) {
	f, rev := newStandby(t)
	ctx := context.Background()
	p, err := f.cut.Plan(ctx, f.w, false)
	if err != nil || p.Blocked || p.Standby != 1 {
		t.Fatalf("cutover plan: %+v %v", p, err)
	}
	if len(f.dst.made) != 0 || f.dst.pubs[backName_] {
		t.Fatal("the plan made the way back")
	}
	if _, err := f.cut.Apply(ctx, f.w, p, p.Digest); err != nil {
		t.Fatal(err)
	}
	// The cutover left a publication of the move's tables and a slot on
	// the target, and nothing on the old side.
	if !f.dst.pubs[backName_] || len(f.dst.made) != 1 || f.dst.made[0] != backName_ || len(f.dst.pubTabs) != 2 {
		t.Fatalf("the way back: pubs %v, slots made %v, tables %v", f.dst.pubs, f.dst.made, f.dst.pubTabs)
	}
	if len(f.src.execs) != 0 {
		t.Fatalf("the cutover wrote to the source: %q", f.src.execs)
	}

	// The target takes writes. Switching back would lose them, and the
	// finding says a reverse sync is what's missing.
	f.dst.tables[`"public"."orders"`] = 5
	f.dst.lag = 4096
	back, err := f.cut.Plan(ctx, f.w, true)
	if err != nil || !back.Blocked || !contains(back.Findings, findingStandbyBehind) || contains(back.Findings, findingNoFailback) {
		t.Fatalf("back with the old side behind: %+v %v", back, err)
	}
	if _, err := f.cut.Apply(ctx, f.w, back, back.Digest); err == nil {
		t.Fatal("a blocked return was applied")
	}

	// The reverse sync: a subscription on the old side that copies
	// nothing, reads the slot the cutover made and takes only what was
	// written on the target.
	s := reverseOf(f, rev)
	sp, err := s.Plan(ctx)
	if err != nil || sp.Blocked() || sp.Datasets[0].Copy != 0 || sp.Datasets[0].Tables != 2 {
		t.Fatalf("reverse plan: %+v %v", sp, err)
	}
	if len(f.src.execs) != 0 {
		t.Fatal("the reverse plan wrote to the old side")
	}
	if _, err := s.Run(ctx, f.w, sp, strings.Repeat("0", 64), RunOptions{Every: time.Second, Rounds: 1}); !errors.Is(err, ErrApproval) {
		t.Fatalf("wrong digest: %v", err)
	}
	// What the subscription does once it's made.
	f.src.tables[`"public"."orders"`] = 5
	f.src.ready, f.dst.lag = 2, 0
	st, err := s.Run(ctx, f.w, sp, sp.Digest, RunOptions{Every: time.Second, Rounds: 1})
	if err != nil || !st.Verified() {
		t.Fatalf("reverse sync: %+v %v", st, err)
	}
	made := built(f.src.execs)
	if len(made) == 0 || !strings.HasPrefix(made[0], `CREATE SUBSCRIPTION "zoctl_back_orders" CONNECTION 'postgres://repl:synthetic@new.example.test/shop' PUBLICATION "zoctl_back_orders" WITH (`) {
		t.Fatalf("the old side's first statement: %.80q", f.src.execs)
	}
	for _, want := range []string{"create_slot = false", "copy_data = false", "origin = none", "slot_name = 'zoctl_back_orders'"} {
		if !strings.Contains(made[0], want) {
			t.Fatalf("the subscription lacks %s", want)
		}
	}
	wroteOnly(t, f.src, 0)
	if f.src.subs[backName_] {
		t.Fatal("the old side's subscription is still enabled after the run")
	}
	// The approval is spent, and the move's own sync is still refused.
	if _, err := s.Run(ctx, f.w, sp, sp.Digest, RunOptions{Every: time.Second, Rounds: 1}); err == nil {
		t.Fatal("a reverse approval ran twice")
	}
	fp, err := f.cut.Sync.Plan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.cut.Sync.Run(ctx, f.w, fp, fp.Digest, RunOptions{Every: time.Second, Rounds: 1}); !errors.Is(err, ErrCutOver) {
		t.Fatalf("the move's sync after a cutover: %v", err)
	}
	// A second window enables the subscription it has.
	if sp, err = s.Plan(ctx); err != nil || sp.Blocked() {
		t.Fatalf("second reverse plan: %+v %v", sp, err)
	}
	if st, err = s.Run(ctx, f.w, sp, sp.Digest, RunOptions{Every: time.Second, Rounds: 1}); err != nil || !st.Verified() {
		t.Fatalf("second reverse sync: %+v %v", st, err)
	}
	wroteOnly(t, f.src, 0)

	// Switching back: the old side has it all, the target has to stay
	// quiet over the wait, and the old side's sequences follow.
	f.dst.seqs = [][]string{{"public", "orders_id_seq", "57"}}
	f.src.setvals[`"public"."orders_id_seq"`] = 42
	back, err = f.cut.Plan(ctx, f.w, true)
	if err != nil || back.Blocked || back.Standby != 1 || len(back.Findings) != 0 {
		t.Fatalf("back: %+v %v", back, err)
	}
	if f.src.setvals[`"public"."orders_id_seq"`] != 42 {
		t.Fatal("a return's plan set a sequence")
	}
	slept := f.slept
	done, err := f.cut.Apply(ctx, f.w, back, back.Digest)
	if err != nil || len(done.Findings) != 0 {
		t.Fatalf("return: %+v %v", done, err)
	}
	if f.slept-slept != time.Minute {
		t.Fatalf("the return waited %v for the target to stay quiet", f.slept-slept)
	}
	if f.src.setvals[`"public"."orders_id_seq"`] != 57 {
		t.Fatalf("the old side's sequence is at %d", f.src.setvals[`"public"."orders_id_seq"`])
	}
	if got, _, _ := f.zone.Get(ctx, wwwOld.Name, "A"); !got.Equal(wwwOld) {
		t.Fatalf("www after the return: %+v", got)
	}
	// The way back is gone from the target, so nothing holds its WAL.
	if _, ok := f.dst.slots[backName_]; ok || f.dst.pubs[backName_] {
		t.Fatal("the return left the way back's slot or publication on the target")
	}
	// After the return the source takes writes again, and nothing writes
	// to it.
	if ok, _ := Standing(f.w); ok {
		t.Fatal("a returned cutover stands")
	}
	if _, err := s.Run(ctx, f.w, sp, sp.Digest, RunOptions{Every: time.Second, Rounds: 1}); err == nil {
		t.Fatal("a reverse sync ran after the return")
	}
}

// Writes on the target during a return's wait stop it before DNS changes.
func TestReturnWaitsForTheTargetToGoQuiet(t *testing.T) {
	f, _ := newStandby(t)
	ctx := context.Background()
	cutOver(t, f)
	changes := f.zone.changes
	back, err := f.cut.Plan(ctx, f.w, true)
	if err != nil || back.Blocked {
		t.Fatalf("back: %+v %v", back, err)
	}
	f.cut.Sleep = func(context.Context, time.Duration) error { f.dst.lsn = "writes 9"; return nil }
	done, err := f.cut.Apply(ctx, f.w, back, back.Digest)
	if !errors.Is(err, ErrBlocked) || !contains(done.Findings, findingWritesGoingOn) {
		t.Fatalf("a return with the target still written: %+v %v", done, err)
	}
	if f.zone.changes != changes {
		t.Fatal("DNS changed")
	}
	if _, ok := f.dst.slots[backName_]; !ok {
		t.Fatal("a return that didn't happen dropped the way back")
	}
	// Rows that reach the target in the wait and aren't on the old side.
	f.dst.lsn = ""
	if back, err = f.cut.Plan(ctx, f.w, true); err != nil || back.Blocked {
		t.Fatalf("back again: %+v %v", back, err)
	}
	f.cut.Sleep = func(context.Context, time.Duration) error { f.dst.lag = 8; return nil }
	f.dst.lsn = ""
	done, err = f.cut.Apply(ctx, f.w, back, back.Digest)
	if !errors.Is(err, ErrBlocked) || !contains(done.Findings, findingStandbyBehind) || f.zone.changes != changes {
		t.Fatalf("a return with the old side behind: %+v %v", done, err)
	}
}

// A move file that asks for a way back gets one or doesn't cut over.
func TestCutoverNeedsTheWayBackItWasAskedFor(t *testing.T) {
	for name, spoil := range map[string]func(*cutFixture){
		"the target's WAL isn't logical":    func(f *cutFixture) { f.dst.wal = "replica" },
		"the target has no slot to spare":   func(f *cutFixture) { f.dst.noFreeSlot = true },
		"the target is before Postgres 16":  func(f *cutFixture) { f.dst.version = "150008" },
		"the source is before Postgres 16":  func(f *cutFixture) { f.src.version = "150008" },
		"the target subscribes elsewhere":   func(f *cutFixture) { f.dst.subs["theirs"] = true },
		"the old side subscribes elsewhere": func(f *cutFixture) { f.src.subs["theirs"] = true },
	} {
		f, _ := newStandby(t)
		spoil(f)
		p, err := f.cut.Plan(context.Background(), f.w, false)
		// A source with no slot to spare only matters to a first copy.
		if err != nil || !p.Blocked || !contains(p.Findings, findingReturnSetup) {
			t.Errorf("%s: %+v %v", name, p, err)
		}
	}
	// One that can't be made at the last moment stops the cutover with DNS
	// as it was.
	f, _ := newStandby(t)
	ctx := context.Background()
	p, err := f.cut.Plan(ctx, f.w, false)
	if err != nil || p.Blocked {
		t.Fatalf("plan: %+v %v", p, err)
	}
	f.dst.failExec = "CREATE PUBLICATION"
	if _, err := f.cut.Apply(ctx, f.w, p, p.Digest); err == nil {
		t.Fatal("cut over without the way back")
	}
	if f.zone.changes != 0 {
		t.Fatal("DNS changed before the way back was made")
	}
	if cut, _ := CutOver(f.w); cut {
		t.Fatal("the workspace says it cut over")
	}
}

// A database without a way back keeps the old rule: the return is refused
// once the target differs, and no reverse sync opens.
func TestNoWayBackWithoutBackEnv(t *testing.T) {
	m, err := ParseMove([]byte(`{"version":1,"source":"aws","target":"gcp","limits":{"max_objects":1,"max_bytes":1,"hours":1},
	 "datasets":[{"name":"orders","kind":"postgres","from_env":"SOURCE_DB","to_env":"TARGET_DB","publication":"p","slot":"s"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	asked := map[string]bool{}
	_, err = OpenBack(m, Options{Env: func(k string) string { asked[k] = true; return "" }})
	if !errors.Is(err, ErrNoWayBack) {
		t.Fatalf("a reverse sync of a database with no back_env: %v", err)
	}
	if len(asked) != 0 {
		t.Fatalf("credentials were read for a reverse sync that can't run: %v", asked)
	}
}

// bucketCut is newCut with a bucket for its dataset: three objects on both
// sides, and one the target had of its own before the cutover.
func bucketCut(t *testing.T) (*cutFixture, *bucket, *bucket) {
	f := newCut(t)
	old := &bucket{data: map[string]string{"a": "one", "b": "two", "c": "three"}}
	target := &bucket{data: map[string]string{"a": "one", "b": "two", "c": "three", "theirs": "kept"}}
	f.cut.Sync.sets = []dataset{newObjectSet("pair", "", old, target)}
	check := newObjectSet("pair-back", "", target, old)
	check.back, check.checkOnly = true, true
	f.cut.back = []dataset{check}
	return f, old, target
}

func TestBucketsGoBack(t *testing.T) {
	f, old, target := bucketCut(t)
	ctx := context.Background()
	cutOver(t, f)
	// Nothing new on the target: the return goes ahead with no reverse
	// sync, as it did before there was one.
	back, err := f.cut.Plan(ctx, f.w, true)
	if err != nil || back.Blocked {
		t.Fatalf("back with nothing new: %+v %v", back, err)
	}
	// The set opened for a return's check can't write.
	check := f.cut.back[0]
	if _, err := check.Round(ctx, nil); err == nil {
		t.Fatal("a return's check copied")
	}
	if _, err := check.Start(ctx, DatasetPlan{}, nil); err == nil {
		t.Fatal("a return's check copied")
	}

	for name, write := range map[string]func(){
		"a new object":                       func() { target.data["new"] = "four" },
		"one changed":                        func() { target.data["b"] = "two, edited" },
		"one of its own swapped for another": func() { delete(target.data, "theirs"); target.data["ours"] = "kept" },
	} {
		saved := map[string]string{}
		for k, v := range target.data {
			saved[k] = v
		}
		write()
		back, err := f.cut.Plan(ctx, f.w, true)
		if err != nil || !back.Blocked || !contains(back.Findings, findingStandbyBehind) {
			t.Errorf("back with %s on the target: %+v %v", name, back, err)
		}
		target.data = saved
	}
	if old.puts != 0 {
		t.Fatal("a return's plan wrote to the old side")
	}

	// The reverse sync copies what the target took and deletes nothing.
	target.data["new"] = "four"
	target.data["b"] = "two, edited"
	delete(target.data, "c")
	f.cut.Sync.move.Limits = Limits{MaxObjects: 100, MaxBytes: 1000, Hours: 1}
	s := reverseOf(f, func() dataset { d := newObjectSet("pair-back", "", target, old); d.back = true; return d }())
	sp, err := s.Plan(ctx)
	if err != nil || sp.Blocked() || sp.Datasets[0].Copy != 3 || sp.Datasets[0].Extra != 1 {
		t.Fatalf("reverse plan: %+v %v", sp, err)
	}
	if old.puts != 0 {
		t.Fatal("the reverse plan wrote to the old side")
	}
	st, err := s.Run(ctx, f.w, sp, sp.Digest, RunOptions{Every: time.Second, Rounds: 1})
	if err != nil || !st.Verified() {
		t.Fatalf("reverse sync: %+v %v", st, err)
	}
	if old.data["new"] != "four" || old.data["b"] != "two, edited" || old.data["theirs"] != "kept" {
		t.Fatalf("the old side after the reverse sync: %v", old.data)
	}
	if old.data["c"] != "three" {
		t.Fatal("the reverse sync deleted an object from the old side")
	}
	if back, err = f.cut.Plan(ctx, f.w, true); err != nil || back.Blocked {
		t.Fatalf("back after the reverse sync: %+v %v", back, err)
	}
	if _, err := f.cut.Apply(ctx, f.w, back, back.Digest); err != nil {
		t.Fatal(err)
	}
}

// The old side's bucket opens for writing only on STANDBY_ keys.
func TestReverseSyncReadsStandbyKeys(t *testing.T) {
	m, err := ParseMove([]byte(moveJSON))
	if err != nil {
		t.Fatal(err)
	}
	keys := map[string]string{
		"SOURCE_AWS_ACCESS_KEY_ID": "synthetic-read", "SOURCE_AWS_SECRET_ACCESS_KEY": "synthetic-read-secret",
		"TARGET_GOOGLE_OAUTH_ACCESS_TOKEN": "synthetic-token",
	}
	var asked []string
	env := func(k string) string { asked = append(asked, k); return keys[k] }
	if _, err := OpenBack(m, Options{Env: env}); err == nil {
		t.Fatal("a reverse sync opened on the source's read-only keys")
	}
	keys["STANDBY_AWS_ACCESS_KEY_ID"], keys["STANDBY_AWS_SECRET_ACCESS_KEY"] = "synthetic-write", "synthetic-write-secret"
	asked = nil
	s, err := OpenBack(m, Options{Env: env})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, k := range asked {
		if strings.HasPrefix(k, "SOURCE_") {
			t.Fatalf("a reverse sync read %s", k)
		}
	}
	if !s.Back() {
		t.Fatal("not a reverse sync")
	}
	// The move's own sync and a cutover never ask for them.
	asked = nil
	fwd, err := Open(m, Options{Env: env})
	if err != nil {
		t.Fatal(err)
	}
	fwd.Close()
	for _, k := range asked {
		if strings.HasPrefix(k, "STANDBY_") {
			t.Fatalf("the move's sync read %s", k)
		}
	}
	// A return reads the old side with the source's keys.
	delete(keys, "STANDBY_AWS_ACCESS_KEY_ID")
	delete(keys, "STANDBY_AWS_SECRET_ACCESS_KEY")
	for _, name := range objects.CredentialNames("s3://src-bucket?region=us-east-1", "STANDBY") {
		if !strings.HasPrefix(name, "STANDBY_") {
			t.Fatalf("the old side's write key is named %s", name)
		}
	}
}

// stopZone makes the changes it has left and fails the rest, undo included.
type stopZone struct {
	*memZone
	left int
}

func (z *stopZone) Replace(ctx context.Context, old, new *dns.RecordSet) error {
	if z.left == 0 {
		return dns.ErrProvider
	}
	z.left--
	return z.memZone.Replace(ctx, old, new)
}

// A return that stops part way leaves some names the old side's, which takes
// writes again. No reverse sync runs over that, though the cutover was
// finished and the return isn't.
func TestNoReverseSyncOverAReturnPartWay(t *testing.T) {
	f, rev := newStandby(t)
	ctx := context.Background()
	cutOver(t, f)
	if ok, err := Standing(f.w); err != nil || !ok {
		t.Fatalf("a finished cutover stands: %t %v", ok, err)
	}
	back, err := f.cut.Plan(ctx, f.w, true)
	if err != nil || back.Blocked || back.Change != 2 {
		t.Fatalf("back: %+v %v", back, err)
	}
	// The AAAA goes, then the A fails and the AAAA can't be put back.
	f.cut.Zone = &stopZone{memZone: f.zone, left: 1}
	if _, err := f.cut.Apply(ctx, f.w, back, back.Digest); !errors.Is(err, ErrCutPartial) {
		t.Fatalf("a return stopped part way: %v", err)
	}
	if _, ok, _ := f.zone.Get(ctx, v6New.Name, "AAAA"); ok {
		t.Fatal("the fixture changed nothing, so this shows nothing")
	}
	if ok, err := Standing(f.w); err != nil || ok {
		t.Fatalf("a cutover part way back stands: %t %v", ok, err)
	}
	s := reverseOf(f, rev)
	sp, err := s.Plan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	from := len(f.src.execs)
	if _, err := s.Run(ctx, f.w, sp, sp.Digest, RunOptions{Every: time.Second, Rounds: 1}); !errors.Is(err, ErrNotCutOver) {
		t.Fatalf("a reverse sync over a return part way: %v", err)
	}
	for _, sql := range f.src.execs[from:] {
		if strings.HasPrefix(sql, "CREATE SUBSCRIPTION ") || strings.Contains(sql, " ENABLE") {
			t.Fatalf("the old side was sent %.40q", sql)
		}
	}
	// The next return finishes the job, and it stays that way.
	f.cut.Zone = f.zone
	back, err = f.cut.Plan(ctx, f.w, true)
	if err != nil || back.Blocked || back.Change != 1 || back.Already != 1 {
		t.Fatalf("the second return's plan: %+v %v", back, err)
	}
	if _, err := f.cut.Apply(ctx, f.w, back, back.Digest); err != nil {
		t.Fatal(err)
	}
	if got, _, _ := f.zone.Get(ctx, wwwOld.Name, "A"); !got.Equal(wwwOld) {
		t.Fatalf("www after the return: %+v", got)
	}
	if ok, _ := Standing(f.w); ok {
		t.Fatal("a returned cutover stands")
	}
}

// A return whose first change was put back didn't happen: the cutover stands
// as it did, and a reverse sync may run.
func TestAReturnPutBackLeavesTheCutoverStanding(t *testing.T) {
	f, _ := newStandby(t)
	ctx := context.Background()
	cutOver(t, f)
	back, err := f.cut.Plan(ctx, f.w, true)
	if err != nil || back.Blocked {
		t.Fatalf("back: %+v %v", back, err)
	}
	f.zone.failOn = wwwOld.Name + " A"
	if _, err := f.cut.Apply(ctx, f.w, back, back.Digest); err == nil || errors.Is(err, ErrCutPartial) {
		t.Fatalf("a return put back: %v", err)
	}
	if got, ok, _ := f.zone.Get(ctx, v6New.Name, "AAAA"); !ok || !got.Equal(v6New) {
		t.Fatal("the AAAA was not put back")
	}
	if ok, err := Standing(f.w); err != nil || !ok {
		t.Fatalf("a cutover whose return was put back: %t %v", ok, err)
	}
}

// A plan made before another cut over finds nothing to change. Applying it
// doesn't set the target's sequences again or replace the way back.
func TestASecondCutoverPlanIsRefusedOnceCutOver(t *testing.T) {
	f := newCut(t)
	ctx := context.Background()
	f.zone.sets = map[string]dns.RecordSet{wwwNew.Name + " A": wwwNew, apiNew.Name + " CNAME": apiNew, v6New.Name + " AAAA": v6New}
	first, err := f.cut.Plan(ctx, f.w, false)
	if err != nil || first.Blocked || first.Change != 0 {
		t.Fatalf("a plan with nothing to change: %+v %v", first, err)
	}
	f.now = f.now.Add(time.Hour)
	f.cut.Sync.move.Cutover.ReturnDays = 7
	second, err := f.cut.Plan(ctx, f.w, false)
	if err != nil || second.Blocked || second.Digest == first.Digest {
		t.Fatalf("a second plan: %+v %v", second, err)
	}
	f.cut.Sync.move.Cutover.ReturnDays = 0
	if _, err := f.cut.Apply(ctx, f.w, first, first.Digest); err != nil {
		t.Fatal(err)
	}
	f.cut.Sync.move.Cutover.ReturnDays = 7
	sets := len(f.dst.setvals)
	f.dst.setvals = map[string]int64{}
	if _, err := f.cut.Apply(ctx, f.w, second, second.Digest); !errors.Is(err, ErrCutOver) {
		t.Fatalf("a second cutover: %v (the first set %d sequences)", err, sets)
	}
	if len(f.dst.setvals) != 0 {
		t.Fatalf("a second cutover set the target's sequences: %v", f.dst.setvals)
	}
}

// A cutover that dies between making the way back and saving its state
// leaves a slot nothing else knows of. The next plan says so without writing
// anything, and the next cutover drops it before making its own.
func TestAWayBackLeftByACrashIsReported(t *testing.T) {
	f, _ := newStandby(t)
	ctx := context.Background()
	if err := f.w.Save(backPending, wayBackMark{Pending: true}); err != nil {
		t.Fatal(err)
	}
	f.dst.slots[backName_] = [3]string{"pgoutput", "logical", "true"}
	f.dst.pubs[backName_] = true
	p, err := f.cut.Plan(ctx, f.w, false)
	if err != nil || p.Blocked || !slices.Contains(p.Findings, findingWayBackLeft) {
		t.Fatalf("plan after a crash: %v %+v", err, p)
	}
	if len(f.dst.execs) != 0 || len(f.src.execs) != 0 {
		t.Fatalf("a plan wrote: %q %q", f.dst.execs, f.src.execs)
	}
	if _, err := f.cut.Apply(ctx, f.w, p, p.Digest); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if pendingBack(f.w) {
		t.Fatal("a cutover that saved its state left the mark")
	}
	if len(f.dst.made) != 1 {
		t.Fatalf("the way back was made %d times", len(f.dst.made))
	}
}

// A cutover that didn't happen leaves no way back on the target: a slot
// nobody reads holds the target's WAL until its disk is full. The source is
// still the live side, so nothing is written there.
func TestACutoverThatDidNotHappenLeavesNoSlot(t *testing.T) {
	f, _ := newStandby(t)
	ctx := context.Background()
	f.zone.failOn = v6New.Name + " AAAA"
	p, err := f.cut.Plan(ctx, f.w, false)
	if err != nil || p.Blocked {
		t.Fatalf("plan: %v %+v", err, p)
	}
	if _, err := f.cut.Apply(ctx, f.w, p, p.Digest); err == nil || errors.Is(err, ErrCutPartial) {
		t.Fatalf("a cutover put back: %v", err)
	}
	if !f.at(t, wwwOld) {
		t.Fatal("the records weren't put back")
	}
	if len(f.dst.made) != 1 {
		t.Fatalf("the test never made the way back: %v", f.dst.made)
	}
	if _, left := f.dst.slots[backName_]; left || f.dst.pubs[backName_] {
		t.Fatalf("the way back was left on the target: slots %v, pubs %v", f.dst.slots, f.dst.pubs)
	}
	if len(f.src.execs) != 0 {
		t.Fatalf("a cutover that didn't happen wrote to the source: %q", f.src.execs)
	}

	if pendingBack(f.w) {
		t.Fatal("a way back that was dropped is still marked")
	}

	// One left part way keeps it: the target may be taking writes.
	g, _ := newStandby(t)
	p, err = g.cut.Plan(ctx, g.w, false)
	if err != nil || p.Blocked {
		t.Fatalf("plan: %v %+v", err, p)
	}
	g.cut.Zone = &flaky{memZone: g.zone, n: 1}
	if _, err := g.cut.Apply(ctx, g.w, p, p.Digest); !errors.Is(err, ErrCutPartial) {
		t.Fatalf("a cutover left part way: %v", err)
	}
	if _, kept := g.dst.slots[backName_]; !kept || !g.dst.pubs[backName_] {
		t.Fatalf("a cutover part way lost its way back: slots %v, pubs %v", g.dst.slots, g.dst.pubs)
	}
}
