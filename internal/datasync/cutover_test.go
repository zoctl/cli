package datasync

import (
	"context"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zoctl/cli/internal/transfer"
	"github.com/zoctl/cli/internal/transfer/dns"
)

// memZone is a DNS zone in memory. failOn makes a replace of that name fail.
type memZone struct {
	mu      sync.Mutex
	sets    map[string]dns.RecordSet
	failOn  string
	changes int
}

func (z *memZone) Get(_ context.Context, name, typ string) (dns.RecordSet, bool, error) {
	z.mu.Lock()
	defer z.mu.Unlock()
	s, ok := z.sets[name+" "+typ]
	return s, ok, nil
}

// Replace is one change, of the type too, as Route 53, Cloud DNS and
// Cloudflare make it. Like them it refuses a CNAME beside another record.
func (z *memZone) Replace(_ context.Context, old, new *dns.RecordSet) error {
	z.mu.Lock()
	defer z.mu.Unlock()
	set := new
	if set == nil {
		set = old
	}
	k := set.Name + " " + set.Type
	if old != nil {
		if cur, ok := z.sets[old.Name+" "+old.Type]; !ok || !cur.Equal(*old) {
			return dns.ErrChanged
		}
	}
	if _, ok := z.sets[k]; ok && (old == nil || old.Type != set.Type) {
		return dns.ErrChanged
	}
	if k == z.failOn {
		return dns.ErrProvider
	}
	after := map[string]dns.RecordSet{}
	for key, s := range z.sets {
		after[key] = s
	}
	if old != nil {
		delete(after, old.Name+" "+old.Type)
	}
	if new != nil {
		after[k] = *new
	}
	for key, s := range after {
		if s.Type != "CNAME" {
			continue
		}
		for _, t := range []string{"A", "AAAA"} {
			if _, ok := after[strings.TrimSuffix(key, "CNAME")+t]; ok {
				return dns.ErrProvider
			}
		}
	}
	z.sets = after
	z.changes++
	return nil
}

// records is a move file's records for sets that say nothing of
// Cloudflare's setting.
func records(sets ...dns.RecordSet) []Record {
	var out []Record
	for _, s := range sets {
		out = append(out, Record{Name: s.Name, Type: s.Type, TTL: s.TTL, Values: s.Values})
	}
	return out
}

type healthCheck int

func (h healthCheck) RoundTrip(*http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: int(h), Header: http.Header{}, Body: io.NopCloser(strings.NewReader(""))}, nil
}

var (
	wwwOld = dns.RecordSet{Name: "www.shop.example.test.", Type: "A", TTL: 300, Values: []string{"198.51.100.7"}}
	wwwNew = dns.RecordSet{Name: "www.shop.example.test.", Type: "A", TTL: 60, Values: []string{"203.0.113.9"}}
	apiNew = dns.RecordSet{Name: "api.shop.example.test.", Type: "CNAME", TTL: 60, Values: []string{"lb.new.example.test."}}
	v6New  = dns.RecordSet{Name: "www.shop.example.test.", Type: "AAAA", TTL: 60, Values: []string{"2001:db8::9"}}
)

type cutFixture struct {
	src, dst *fakePG
	zone     *memZone
	cut      *Cut
	w        *transfer.Workspace
	now      time.Time
	slept    time.Duration
}

// newCut is a move whose one Postgres dataset has caught up, and whose
// zone has www pointing at the source and api already on the target.
func newCut(t *testing.T) *cutFixture {
	f := &cutFixture{src: sourcePG(), dst: newPG(), now: time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)}
	f.src.lsn = "0/16B3748"
	f.src.seqs = [][]string{{"public", "orders_id_seq", "42"}}
	f.dst.setvals[`"public"."orders_id_seq"`] = 1
	for k, v := range f.src.tables {
		f.dst.tables[k] = v
	}
	f.dst.subs["zoctl_orders"] = false
	f.dst.ready = 2
	f.zone = &memZone{sets: map[string]dns.RecordSet{
		wwwOld.Name + " A":     wwwOld,
		apiNew.Name + " CNAME": apiNew,
	}}
	m := Move{Version: 1, Source: "aws", Target: "gcp", Limits: Limits{MaxObjects: 1, MaxBytes: 1, Hours: 1},
		Cutover: &Cutover{Zone: "cloudflare://0123456789abcdef0123456789abcdef", Credentials: "DNS", Health: "https://new.shop.example.test/healthz",
			Records: records(wwwNew, apiNew, v6New)}}
	s := &Sync{move: m, sets: []dataset{pgSetFor(f.src, f.dst, false)}, now: func() time.Time { return f.now }}
	f.cut = &Cut{Sync: s, Zone: f.zone, Client: &http.Client{Transport: healthCheck(200)}, Settle: time.Minute,
		Sleep: func(_ context.Context, d time.Duration) error { f.slept += d; return nil }}
	w, err := transfer.OpenWorkspace(filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	f.w = w
	return f
}

func TestCutoverAndBack(t *testing.T) {
	f := newCut(t)
	ctx := context.Background()
	p, err := f.cut.Plan(ctx, f.w, false)
	if err != nil {
		t.Fatal(err)
	}
	if p.Blocked || p.Change != 2 || p.Already != 1 || p.Created != 1 || len(p.Findings) != 0 {
		t.Fatalf("plan %+v", p)
	}
	if f.zone.changes != 0 {
		t.Fatal("the plan changed DNS")
	}
	if _, err := f.cut.Apply(ctx, f.w, p, strings.Repeat("0", 64)); !errors.Is(err, ErrApproval) {
		t.Fatalf("wrong digest: %v", err)
	}
	done, err := f.cut.Apply(ctx, f.w, p, p.Digest)
	if err != nil {
		t.Fatal(err)
	}
	if f.slept != time.Minute || f.dst.setvals[`"public"."orders_id_seq"`] != 42 {
		t.Fatalf("slept %v, sequence %d", f.slept, f.dst.setvals[`"public"."orders_id_seq"`])
	}
	for _, want := range []dns.RecordSet{wwwNew, apiNew, v6New} {
		if got, ok, _ := f.zone.Get(ctx, want.Name, want.Type); !ok || !got.Equal(want) {
			t.Fatalf("%s %s: %+v", want.Name, want.Type, got)
		}
	}
	if !done.ReturnUntil.Equal(f.now.AddDate(0, 0, 30)) {
		t.Fatal(done.ReturnUntil)
	}
	if cut, err := CutOver(f.w); err != nil || !cut {
		t.Fatalf("not cut over: %v", err)
	}
	// The finished cutover says when it was made, and the workspace says
	// it again to a runner that asks later.
	cutAt := f.now
	if again, ok, err := CutDone(f.w); err != nil || !ok || again.Back || !done.At.Equal(cutAt) || !again.At.Equal(cutAt) || !again.ReturnUntil.Equal(done.ReturnUntil) {
		t.Fatalf("cutover done at %v, kept as %+v %v %v", done.At, again, ok, err)
	}
	// The approval is spent, a second cutover is refused, and so is a
	// sync that would copy the source's old rows over the target's.
	if _, err := f.cut.Apply(ctx, f.w, p, p.Digest); err == nil {
		t.Fatal("approval ran twice")
	}
	if _, err := f.cut.Plan(ctx, f.w, false); !errors.Is(err, ErrCutOver) {
		t.Fatalf("second cutover: %v", err)
	}
	sp, err := f.cut.Sync.Plan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.cut.Sync.Run(ctx, f.w, sp, sp.Digest, RunOptions{Every: time.Second, Rounds: 1}); !errors.Is(err, ErrCutOver) {
		t.Fatalf("sync after cutover: %v", err)
	}

	// The target took a write: switching back would lose it.
	f.dst.tables[`"public"."orders"`] = 4
	back, err := f.cut.Plan(ctx, f.w, true)
	if err != nil || !back.Blocked || !contains(back.Findings, findingNoFailback) {
		t.Fatalf("back with target writes: %+v %v", back, err)
	}
	f.dst.tables[`"public"."orders"`] = 3
	f.now = f.now.AddDate(0, 0, 31)
	back, err = f.cut.Plan(ctx, f.w, true)
	if err != nil || back.Blocked || back.Change != 2 || !contains(back.Findings, findingReturnOver) {
		t.Fatalf("back: %+v %v", back, err)
	}
	if _, ok, _ := CutDone(f.w); !ok {
		t.Fatal("a standing cutover isn't done")
	}
	returned, err := f.cut.Apply(ctx, f.w, back, back.Digest)
	if err != nil {
		t.Fatal(err)
	}
	if again, ok, err := CutDone(f.w); err != nil || !ok || !again.Back || !returned.At.Equal(f.now) || !again.At.Equal(f.now) {
		t.Fatalf("return done at %v, kept as %+v %v %v", returned.At, again, ok, err)
	}
	if got, _, _ := f.zone.Get(ctx, wwwOld.Name, "A"); !got.Equal(wwwOld) {
		t.Fatalf("www back: %+v", got)
	}
	if _, ok, _ := f.zone.Get(ctx, v6New.Name, "AAAA"); ok {
		t.Fatal("the AAAA zoctl made is still there")
	}
	if got, _, _ := f.zone.Get(ctx, apiNew.Name, "CNAME"); !got.Equal(apiNew) {
		t.Fatal("touched a record that was already right")
	}
	if cut, _ := CutOver(f.w); cut {
		t.Fatal("still cut over")
	}
	if _, err := f.cut.Plan(ctx, f.w, true); !errors.Is(err, ErrNoCutover) {
		t.Fatalf("second return: %v", err)
	}
	// The source only ever got reads.
	for _, q := range f.src.queries {
		if !strings.HasPrefix(q, "SELECT ") {
			t.Fatalf("source got %q", q)
		}
	}
}

func TestCutoverWaitsForTheData(t *testing.T) {
	f := newCut(t)
	f.src.lag = 100
	p, err := f.cut.Plan(context.Background(), f.w, false)
	if err != nil || !p.Blocked || !contains(p.Findings, findingNotCaughtUp) {
		t.Fatalf("%+v %v", p, err)
	}
	if _, err := f.cut.Apply(context.Background(), f.w, p, p.Digest); !errors.Is(err, ErrBlocked) || f.zone.changes != 0 {
		t.Fatalf("%v, %d changes", err, f.zone.changes)
	}
}

func TestCutoverNeedsAHealthyTarget(t *testing.T) {
	f := newCut(t)
	f.cut.Client = &http.Client{Transport: healthCheck(503)}
	p, err := f.cut.Plan(context.Background(), f.w, false)
	if err != nil || !p.Blocked || !contains(p.Findings, findingUnhealthy) {
		t.Fatalf("%+v %v", p, err)
	}
}

func TestCutoverRefusesWhileTheSourceIsWritten(t *testing.T) {
	f := newCut(t)
	ctx := context.Background()
	p, err := f.cut.Plan(ctx, f.w, false)
	if err != nil {
		t.Fatal(err)
	}
	f.cut.Sleep = func(context.Context, time.Duration) error { f.src.lsn = "0/16B37A0"; return nil }
	done, err := f.cut.Apply(ctx, f.w, p, p.Digest)
	if !errors.Is(err, ErrBlocked) || !contains(done.Findings, findingWritesGoingOn) || f.zone.changes != 0 {
		t.Fatalf("%+v %v", done, err)
	}
	if cut, _ := CutOver(f.w); cut {
		t.Fatal("cut over anyway")
	}
}

func TestCutoverRefusesARecordChangedSinceThePlan(t *testing.T) {
	f := newCut(t)
	ctx := context.Background()
	p, err := f.cut.Plan(ctx, f.w, false)
	if err != nil {
		t.Fatal(err)
	}
	moved := wwwOld
	moved.Values = []string{"198.51.100.99"}
	f.zone.sets[wwwOld.Name+" A"] = moved
	if _, err := f.cut.Apply(ctx, f.w, p, p.Digest); !errors.Is(err, dns.ErrChanged) || f.zone.changes != 0 {
		t.Fatalf("%v", err)
	}
}

func TestCutoverPutsRecordsBackWhenOneFails(t *testing.T) {
	f := newCut(t)
	ctx := context.Background()
	f.zone.failOn = v6New.Name + " AAAA"
	// www's A goes first and succeeds, then the AAAA on the same name fails.
	p, err := f.cut.Plan(ctx, f.w, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.cut.Apply(ctx, f.w, p, p.Digest); err == nil {
		t.Fatal("no error")
	}
	if f.zone.changes != 2 {
		t.Fatalf("%d changes, want the A and its undo", f.zone.changes)
	}
	if got, _, _ := f.zone.Get(ctx, wwwOld.Name, "A"); !got.Equal(wwwOld) {
		t.Fatalf("not put back: %+v", got)
	}
	if cut, _ := CutOver(f.w); cut {
		t.Fatal("recorded as cut over")
	}
}

func TestCutoverMoveFile(t *testing.T) {
	base := `{"version":1,"source":"aws","target":"gcp","datasets":[{"name":"media","kind":"objects","from":"s3://a","to":"s3://b"}],"limits":{"max_objects":1,"max_bytes":1,"hours":1},"cutover":`
	good := `{"zone":"route53://Z0123","credentials":"DNS","health":"https://new.example.test/","records":[{"name":"www.example.test.","type":"A","ttl":60,"values":["203.0.113.1"]}]}}`
	if _, err := ParseMove([]byte(base + good)); err != nil {
		t.Fatalf("good: %v", err)
	}
	for _, bad := range []string{
		strings.Replace(good, `"DNS"`, `"SOURCE"`, 1),
		strings.Replace(good, `https://new`, `http://new`, 1),
		strings.Replace(good, `"A"`, `"TXT"`, 1),
		strings.Replace(good, `route53://Z0123`, `route53://z0123`, 1),
		strings.Replace(good, `"records":[`, `"records":[{"name":"www.example.test.","type":"A","ttl":60,"values":["203.0.113.2"]},`, 1),
	} {
		if _, err := ParseMove([]byte(base + bad)); !errors.Is(err, ErrMove) {
			t.Errorf("accepted %s", bad)
		}
	}
}

// A move file with no data needs a cutover, and may leave its limits out.
func TestMoveFileWithNoData(t *testing.T) {
	cut := `"cutover":{"zone":"route53://Z0123","credentials":"DNS","records":[{"name":"www.example.test.","type":"CNAME","ttl":60,"values":["app.new.example.test."]}]}`
	m, err := ParseMove([]byte(`{"version":1,"source":"vercel","target":"azure","datasets":[],` + cut + `}`))
	if err != nil || m.HasData() {
		t.Fatalf("%v %v", err, m.HasData())
	}
	if _, err := ParseMove([]byte(`{"version":1,"source":"vercel","target":"azure",` + cut + `}`)); err != nil {
		t.Fatalf("no datasets field: %v", err)
	}
	for _, bad := range []string{
		`{"version":1,"source":"vercel","target":"azure","datasets":[]}`,
		`{"version":1,"source":"vercel","target":"azure","datasets":[],"limits":{"max_objects":1,"max_bytes":0,"hours":1},` + cut + `}`,
	} {
		if _, err := ParseMove([]byte(bad)); !errors.Is(err, ErrMove) {
			t.Errorf("accepted %s", bad)
		}
	}
	if _, err := Open(m, Options{Env: func(string) string { return "x" }}); !errors.Is(err, ErrNoData) {
		t.Fatalf("opened a sync of no data: %v", err)
	}
	if _, err := OpenBack(m, Options{Env: func(string) string { return "x" }}); !errors.Is(err, ErrNoData) {
		t.Fatalf("opened a reverse sync of no data: %v", err)
	}
}

// A cutover with no data changes DNS without waiting or comparing, and its
// return puts the records back.
func TestCutoverWithNoData(t *testing.T) {
	f := newCut(t)
	f.cut.Sync.sets = nil
	f.cut.Sync.move.Limits = Limits{}
	f.src.lsn = ""
	ctx := context.Background()
	done := f.apply(t, false)
	if f.slept != 0 || done.Standby != 0 {
		t.Fatalf("slept %v, standby %d", f.slept, done.Standby)
	}
	if got, ok := f.has(wwwNew.Name, "A"); !ok || !got.Equal(wwwNew) {
		t.Fatalf("www: %+v", got)
	}
	if cut, err := CutOver(f.w); err != nil || !cut {
		t.Fatalf("not cut over: %v", err)
	}
	f.apply(t, true)
	if got, ok := f.has(wwwOld.Name, "A"); !ok || !got.Equal(wwwOld) {
		t.Fatalf("www back: %+v", got)
	}
	if _, ok, _ := f.zone.Get(ctx, v6New.Name, "AAAA"); ok {
		t.Fatal("the AAAA zoctl made is still there")
	}
}

// cutWith is newCut with another zone and other records.
func cutWith(t *testing.T, zone []dns.RecordSet, want ...Record) *cutFixture {
	f := newCut(t)
	f.zone.sets = map[string]dns.RecordSet{}
	for _, s := range zone {
		f.zone.sets[s.Name+" "+s.Type] = s
	}
	f.cut.Sync.move.Cutover.Records = want
	return f
}

func (f *cutFixture) apply(t *testing.T, back bool) CutPlan {
	t.Helper()
	p, err := f.cut.Plan(context.Background(), f.w, back)
	if err != nil || p.Blocked {
		t.Fatalf("plan: %v %+v", err, p)
	}
	done, err := f.cut.Apply(context.Background(), f.w, p, p.Digest)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	return done
}

func (f *cutFixture) has(name, typ string) (dns.RecordSet, bool) {
	s, ok, _ := f.zone.Get(context.Background(), name, typ)
	return s, ok
}

// A record that says nothing of Cloudflare's proxy keeps what the name has:
// a cutover doesn't take a name out from behind Cloudflare by leaving a
// word out of the move file.
func TestCutoverKeepsCloudflaresProxy(t *testing.T) {
	proxied := dns.RecordSet{Name: wwwOld.Name, Type: "A", TTL: 1, Values: wwwOld.Values, Proxied: true}
	f := cutWith(t, []dns.RecordSet{proxied}, records(wwwNew, v6New)...)
	f.apply(t, false)
	for _, typ := range []string{"A", "AAAA"} {
		// The AAAA is new, and takes the setting of the name's A.
		if got, ok := f.has(wwwNew.Name, typ); !ok || !got.Proxied || got.TTL != 1 {
			t.Fatalf("%s after the cutover: %+v", typ, got)
		}
	}
	if got, _ := f.has(wwwNew.Name, "A"); got.Values[0] != wwwNew.Values[0] {
		t.Fatalf("%+v", got)
	}
	f.apply(t, true)
	if got, _ := f.has(wwwOld.Name, "A"); !got.Equal(proxied) {
		t.Fatalf("after the return: %+v", got)
	}

	// The move file can say otherwise, either way.
	off, on := false, true
	plain := Record{Name: wwwNew.Name, Type: "A", TTL: 60, Values: wwwNew.Values, Proxied: &off}
	f = cutWith(t, []dns.RecordSet{proxied}, plain)
	f.apply(t, false)
	if got, _ := f.has(wwwNew.Name, "A"); got.Proxied || got.TTL != 60 {
		t.Fatalf("asked for no proxy: %+v", got)
	}
	f = cutWith(t, []dns.RecordSet{wwwOld}, Record{Name: wwwNew.Name, Type: "A", Values: wwwNew.Values, Proxied: &on})
	f.apply(t, false)
	if got, _ := f.has(wwwNew.Name, "A"); !got.Proxied || got.TTL != 1 {
		t.Fatalf("asked for the proxy: %+v", got)
	}
	// Nothing to change is still nothing to change.
	f = cutWith(t, []dns.RecordSet{proxied}, Record{Name: proxied.Name, Type: "A", TTL: 60, Values: proxied.Values})
	if p, err := f.cut.Plan(context.Background(), f.w, false); err != nil || p.Change != 0 || p.Already != 1 {
		t.Fatalf("%v %+v", err, p)
	}
}

// A name answers with every address it has. One the move file doesn't
// list would go on sending IPv6 to the source after the cutover.
func TestCutoverRefusesAnAddressItDoesNotList(t *testing.T) {
	v6Old := dns.RecordSet{Name: wwwOld.Name, Type: "AAAA", TTL: 300, Values: []string{"2001:db8::7"}}
	f := cutWith(t, []dns.RecordSet{wwwOld, v6Old}, records(wwwNew)...)
	p, err := f.cut.Plan(context.Background(), f.w, false)
	if err != nil || !p.Blocked || !contains(p.Findings, findingUnlisted) {
		t.Fatalf("%v %+v", err, p)
	}
	if _, err := f.cut.Apply(context.Background(), f.w, p, p.Digest); !errors.Is(err, ErrBlocked) || f.zone.changes != 0 {
		t.Fatalf("%v, %d changes", err, f.zone.changes)
	}

	// Listed with no values, it goes, and a return puts it back.
	f = cutWith(t, []dns.RecordSet{wwwOld, v6Old}, append(records(wwwNew), Record{Name: v6Old.Name, Type: "AAAA"})...)
	done := f.apply(t, false)
	if _, ok := f.has(v6Old.Name, "AAAA"); ok || done.Change != 2 || done.Created != 0 {
		t.Fatalf("the AAAA is still there: %+v", done)
	}
	f.apply(t, true)
	if got, _ := f.has(v6Old.Name, "AAAA"); !got.Equal(v6Old) {
		t.Fatalf("after the return: %+v", got)
	}
	if got, _ := f.has(wwwOld.Name, "A"); !got.Equal(wwwOld) {
		t.Fatalf("after the return: %+v", got)
	}
	// Listed as gone and already gone is nothing to do.
	f = cutWith(t, []dns.RecordSet{wwwNew}, append(records(wwwNew), Record{Name: v6Old.Name, Type: "AAAA"})...)
	if p, err := f.cut.Plan(context.Background(), f.w, false); err != nil || p.Change != 0 || p.Already != 2 {
		t.Fatalf("%v %+v", err, p)
	}
}

// A name that is a CNAME today can end with addresses, and one with
// addresses can end as a CNAME. No DNS host keeps both, so the CNAME is
// swapped for an address in one change, and a return undoes the changes
// last first.
func TestCutoverChangesARecordsType(t *testing.T) {
	alias := dns.RecordSet{Name: wwwOld.Name, Type: "CNAME", TTL: 300, Values: []string{"lb.old.example.test."}}
	v6Old := dns.RecordSet{Name: wwwOld.Name, Type: "AAAA", TTL: 300, Values: []string{"2001:db8::7"}}
	target := dns.RecordSet{Name: wwwOld.Name, Type: "CNAME", TTL: 60, Values: []string{"lb.new.example.test."}}

	f := cutWith(t, []dns.RecordSet{alias}, records(wwwNew, v6New)...)
	done := f.apply(t, false)
	if done.Change != 2 || done.Created != 1 || f.zone.changes != 2 {
		t.Fatalf("%+v, %d changes", done, f.zone.changes)
	}
	if _, ok := f.has(alias.Name, "CNAME"); ok {
		t.Fatal("the CNAME is still there")
	}
	for _, want := range []dns.RecordSet{wwwNew, v6New} {
		if got, _ := f.has(want.Name, want.Type); !got.Equal(want) {
			t.Fatalf("%s: %+v", want.Type, got)
		}
	}
	f.apply(t, true)
	if got, _ := f.has(alias.Name, "CNAME"); !got.Equal(alias) || len(f.zone.sets) != 1 {
		t.Fatalf("after the return: %+v", f.zone.sets)
	}

	f = cutWith(t, []dns.RecordSet{wwwOld, v6Old}, records(target)...)
	done = f.apply(t, false)
	if got, _ := f.has(target.Name, "CNAME"); !got.Equal(target) || len(f.zone.sets) != 1 || done.Change != 2 {
		t.Fatalf("to a CNAME: %+v %+v", f.zone.sets, done)
	}
	f.apply(t, true)
	a, _ := f.has(wwwOld.Name, "A")
	b, _ := f.has(v6Old.Name, "AAAA")
	if !a.Equal(wwwOld) || !b.Equal(v6Old) || len(f.zone.sets) != 2 {
		t.Fatalf("after the return: %+v", f.zone.sets)
	}

	// A CNAME nothing replaces, and nothing says goes, is refused.
	f = cutWith(t, []dns.RecordSet{alias}, Record{Name: alias.Name, Type: "A"})
	if p, err := f.cut.Plan(context.Background(), f.w, false); err != nil || !p.Blocked || !contains(p.Findings, findingUnlisted) {
		t.Fatalf("%v %+v", err, p)
	}

	// The second change fails: the first, which swapped the type, is undone.
	f = cutWith(t, []dns.RecordSet{alias}, records(wwwNew, v6New)...)
	f.zone.failOn = v6New.Name + " AAAA"
	p, err := f.cut.Plan(context.Background(), f.w, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.cut.Apply(context.Background(), f.w, p, p.Digest); err == nil {
		t.Fatal("no error")
	}
	if got, _ := f.has(alias.Name, "CNAME"); !got.Equal(alias) || len(f.zone.sets) != 1 {
		t.Fatalf("not put back: %+v", f.zone.sets)
	}
	if cut, _ := CutOver(f.w); cut {
		t.Fatal("recorded as cut over")
	}
}

func TestCutoverMoveFileRecords(t *testing.T) {
	base := `{"version":1,"source":"aws","target":"gcp","datasets":[{"name":"media","kind":"objects","from":"s3://a","to":"s3://b"}],"limits":{"max_objects":1,"max_bytes":1,"hours":1},"cutover":{"zone":"ZONE","credentials":"DNS","records":[RECORDS]}}`
	file := func(zone, records string) string {
		return strings.Replace(strings.Replace(base, "ZONE", zone, 1), "RECORDS", records, 1)
	}
	const cf, r53 = "cloudflare://0123456789abcdef0123456789abcdef", "route53://Z0123"
	a := `{"name":"www.example.test.","type":"A","ttl":60,"values":["203.0.113.1"]}`
	for _, good := range []string{
		file(r53, a+`,{"name":"www.example.test.","type":"AAAA"}`),
		file(r53, a+`,{"name":"www.example.test.","type":"CNAME"}`),
		file(cf, `{"name":"www.example.test.","type":"A","values":["203.0.113.1"],"proxied":true}`),
		file(cf, `{"name":"www.example.test.","type":"A","ttl":60,"values":["203.0.113.1"],"proxied":false}`),
	} {
		if _, err := ParseMove([]byte(good)); err != nil {
			t.Errorf("refused %s: %v", good, err)
		}
	}
	for _, bad := range []string{
		// Only Cloudflare has a proxy.
		file(r53, `{"name":"www.example.test.","type":"A","ttl":60,"values":["203.0.113.1"],"proxied":false}`),
		// A CNAME beside an address.
		file(r53, a+`,{"name":"WWW.example.test.","type":"CNAME","ttl":60,"values":["lb.example.test."]}`),
		// A record that goes has nothing else to say.
		file(r53, `{"name":"www.example.test.","type":"AAAA","ttl":60}`),
		file(cf, `{"name":"www.example.test.","type":"AAAA","proxied":true}`),
		file(r53, `{"name":"www.example.test.","type":"TXT"}`),
		// A record that stays needs a TTL, unless Cloudflare sets it.
		file(r53, `{"name":"www.example.test.","type":"A","values":["203.0.113.1"]}`),
	} {
		if _, err := ParseMove([]byte(bad)); !errors.Is(err, ErrMove) {
			t.Errorf("accepted %s", bad)
		}
	}
}
