package datasync

// A cutover points the move's DNS records at the target, and switching back
// points them at the source again. DNS can't stop anyone writing, so a
// cutover first waits for the source to go quiet and the data to check out,
// and switching back only goes ahead while the old side has everything the
// target has: either the target took nothing since, or a reverse sync
// (back.go) carried it there. A return never copies data itself.

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/zoctl/cli/internal/transfer"
	"github.com/zoctl/cli/internal/transfer/dns"
)

// Finding codes a cutover reports.
const (
	findingNotCaughtUp   = "data_not_caught_up"
	findingWritesGoingOn = "writers_not_stopped"
	findingUnhealthy     = "target_unhealthy"
	findingDNSChanged    = "dns_changed"
	findingNoFailback    = "source_failback_unqualified"
	findingReturnOver    = "return_window_over"
	findingUnlisted      = "dns_unlisted_record"
)

// FindingCodes is every finding code a sync or a cutover reports. A console
// takes only the codes it knows, so it checks itself against this list.
func FindingCodes() []string {
	return []string{
		findingPrerequisite, findingSchema, findingNotEmpty, findingTablesOnly, findingNoIdentity, findingNoSequence,
		findingSameDatabase, findingSchemaLeft,
		findingNotCaughtUp, findingWritesGoingOn, findingUnhealthy, findingDNSChanged, findingNoFailback,
		findingReturnOver, findingUnlisted, findingReturnSetup, findingStandbyBehind, findingWayBackLeft,
	}
}

// Stages is every stage a sync or a cutover reports.
func Stages() []string {
	return []string{"planned", "blocked", "running", "paused", "interrupted", "verified", "failed"}
}

var (
	// ErrCutOver means the move has cut over, so a sync would copy the
	// source's old data over the target's new writes.
	ErrCutOver = errors.New("datasync: the move has cut over")
	// ErrNoCutover means there's nothing to switch back.
	ErrNoCutover = errors.New("datasync: no cutover to switch back")
	// ErrNoData means the move names no data, so there's nothing to sync
	// either way; its cutover only switches DNS.
	ErrNoData = errors.New("datasync: the move has no data to sync")
	// ErrCutPartial means a cutover or a return stopped part way: some
	// records point at the target and some at the source. The workspace
	// still holds every old set: the next plan finishes the job, and a
	// return puts back the ones that changed.
	ErrCutPartial = errors.New("datasync: the cutover stopped part way")
)

const (
	maxRecords   = 50
	cutoverState = "cutover"
	// backPending says a cutover is making the way back on the target and
	// hasn't saved its state yet. A process that dies in between leaves a
	// slot nothing in the workspace knows of, and the next plan says so.
	backPending       = "way-back-pending"
	defaultReturnDays = 30
)

// Cutover names the DNS zone and the record sets it ends with. The zone is
// on the target or with a DNS host the customer controls, never in the
// source account, so its keys come from TARGET_ or DNS_ names.
type Cutover struct {
	Zone        string   `json:"zone"`
	Credentials string   `json:"credentials"`
	Health      string   `json:"health,omitempty"`
	Records     []Record `json:"records"`
	ReturnDays  int      `json:"return_days,omitempty"`
}

// Record is what one name and type end a cutover with: its values, or with
// no values and no TTL, no record of that type at all.
//
// A name answers with every address record it has, so a cutover has to
// say what becomes of each: a name given an A while it has an AAAA the
// move file doesn't list is refused, since IPv6 would go on reaching the
// source. A CNAME can't share a name, so a name that ends as one loses its
// address records, and a CNAME is replaced by the addresses a name ends
// with.
//
// Proxied is Cloudflare's setting. Left out, the record keeps what the
// name has now, so a cutover doesn't take a name out from behind
// Cloudflare, and show its address, unless the move file says so.
type Record struct {
	Name    string   `json:"name"`
	Type    string   `json:"type"`
	TTL     int      `json:"ttl,omitempty"`
	Values  []string `json:"values,omitempty"`
	Proxied *bool    `json:"proxied,omitempty"`
}

// gone reports a record that asks for no set of its type.
func (r Record) gone() bool { return len(r.Values) == 0 }

func (r Record) key() string { return strings.ToLower(r.Name) + " " + r.Type }

// set is the record set r asks for, with Cloudflare's setting taken from
// now, what the name has today, when r doesn't say.
func (r Record) set(now *dns.RecordSet) *dns.RecordSet {
	s := dns.RecordSet{Name: r.Name, Type: r.Type, TTL: r.TTL, Values: r.Values}
	switch {
	case r.Proxied != nil:
		s.Proxied = *r.Proxied
	case now != nil:
		s.Proxied = now.Proxied
	}
	if s.Proxied {
		// Cloudflare sets a proxied record's TTL itself and reports 1.
		s.TTL = 1
	}
	return &s
}

// recordRule is what every record in a move file has to be. The dot is the
// likeliest slip, so it comes first.
const recordRule = "cutover record: names and CNAME values are lower case and end with a dot, as in app.example.com.; type A, AAAA or CNAME; ttl 1 to 86400 with values, or neither to say the name ends without one"

func (c *Cutover) check() error {
	if c.Credentials != "TARGET" && c.Credentials != "DNS" || len(c.Records) == 0 || len(c.Records) > maxRecords || c.ReturnDays < 0 || c.ReturnDays > 365 {
		return refused("cutover: credentials of TARGET or DNS, some records, return_days up to 365")
	}
	if dns.Check(c.Zone, c.Credentials) != nil {
		return refused("cutover zone: a DNS host this version knows, and never one in the source account")
	}
	if c.Health != "" {
		u, err := url.Parse(c.Health)
		if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Fragment != "" {
			return refused("cutover health: an https URL")
		}
	}
	seen := map[string]bool{}
	for _, r := range c.Records {
		switch {
		case seen[r.key()]:
			return refused("cutover record: a name and type listed twice")
		case r.Proxied != nil && !strings.HasPrefix(c.Zone, "cloudflare://"):
			// Only Cloudflare has the setting; elsewhere a record that
			// asked for it would never read back as asked.
			return refused("cutover record: proxied is Cloudflare's setting")
		case r.gone():
			if !dns.ValidName(r.Name, r.Type) || r.TTL != 0 || r.Proxied != nil {
				return refused(recordRule)
			}
		case !r.set(nil).Valid():
			return refused(recordRule)
		}
		seen[r.key()] = true
	}
	for _, r := range c.Records {
		name := strings.ToLower(r.Name)
		if r.Type == "CNAME" && !r.gone() && (seen[name+" A"] || seen[name+" AAAA"]) {
			for _, o := range c.Records {
				if strings.ToLower(o.Name) == name && o.Type != "CNAME" && !o.gone() {
					return refused("cutover record: a name that ends as a CNAME ends with no address")
				}
			}
		}
	}
	return nil
}

// cutState is what the workspace keeps about a cutover: every record set
// before and after, so switching back needs nothing from memory.
type cutState struct {
	Zone        string     `json:"zone"`
	At          time.Time  `json:"at"`
	ReturnUntil time.Time  `json:"return_until"`
	Changes     []cutPair  `json:"changes"`
	Complete    bool       `json:"complete"`
	Returned    *time.Time `json:"returned,omitempty"`
	// Returning is when a return began changing records. Until it is
	// done, or was put back whole, some names may be the old side's
	// again, so the old side takes writes and no reverse sync may run.
	Returning *time.Time `json:"returning,omitempty"`
	// Extras is, for each dataset, what its target had at the cutover
	// that the source didn't (objects.Verification.Extras). A return reads
	// it to tell whether the target has taken anything since.
	Extras []string `json:"extras,omitempty"`
}

type cutPair struct {
	Old *dns.RecordSet `json:"old,omitempty"`
	New *dns.RecordSet `json:"new,omitempty"`
}

// CutOver reports whether w holds a cutover that hasn't been switched back.
func CutOver(w *transfer.Workspace) (bool, error) {
	var st cutState
	err := w.Load(cutoverState, &st)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return st.Returned == nil, nil
}

// CutDone is what w holds of a finished cutover, for a runner to tell a
// console again that missed it: the cutover that stands, with when it was
// made and how long switching back is kept ready, or the return that undid
// it. ok is false when no cutover finished here, or one is part way.
func CutDone(w *transfer.Workspace) (p CutPlan, ok bool, err error) {
	var st cutState
	err = w.Load(cutoverState, &st)
	if errors.Is(err, fs.ErrNotExist) {
		return CutPlan{}, false, nil
	}
	if err != nil || !st.Complete {
		return CutPlan{}, false, err
	}
	if st.Returned != nil {
		return CutPlan{Back: true, At: *st.Returned}, true, nil
	}
	return CutPlan{At: st.At, ReturnUntil: st.ReturnUntil}, true, nil
}

// CutPlan is a cutover's or a return's plan: counts and finding codes, no
// names.
type CutPlan struct {
	Back bool `json:"back"`
	// Resume means this finishes a cutover that stopped part way.
	Resume      bool      `json:"resume,omitempty"`
	Change      int       `json:"change"`
	Already     int       `json:"already"`
	Created     int       `json:"created"`
	Findings    []string  `json:"findings,omitempty"`
	Blocked     bool      `json:"blocked"`
	Digest      string    `json:"digest"`
	ReturnUntil time.Time `json:"return_until,omitempty"`
	// At is when it was made, once it's done: a cutover's first change, or
	// the return. A runner reports it, and the console counts a move's 30
	// days from a cutover's.
	At time.Time `json:"at,omitzero"`
	// Standby is how many databases have a way back. A cutover makes each
	// one's publication and slot on the target; a return sets the old
	// side's sequences and drops them again.
	Standby int `json:"standby,omitempty"`

	changes   []cutPair
	positions []string
}

// Cut runs one move's cutover or return.
type Cut struct {
	Sync   *Sync
	Zone   dns.Zone
	Client *http.Client
	// Settle is how long the source must stay quiet before a cutover.
	Settle time.Duration
	// Sleep waits out Settle; tests replace it.
	Sleep func(context.Context, time.Duration) error

	// back is each dataset the other way round, for a return's check
	// (OpenReturn). An entry is nil where a dataset has no way back.
	back []dataset
}

// OpenCut opens the move's datasets and its DNS zone. The zone's keys come
// from the cutover's own prefix, never a SOURCE_ one.
func OpenCut(m Move, o Options, settle time.Duration) (*Cut, error) {
	if m.Cutover == nil {
		return nil, ErrMove
	}
	s, err := open(m, o)
	if err != nil {
		return nil, err
	}
	z, err := dns.Open(m.Cutover.Zone, m.Cutover.Credentials, o.Env, o.Client)
	if err != nil {
		s.Close()
		return nil, err
	}
	return &Cut{Sync: s, Zone: z, Client: o.Client, Settle: settle}, nil
}

func (c *Cut) Close() {
	c.Sync.Close()
	for _, d := range c.back {
		if d != nil {
			d.Close()
		}
	}
}

func (c *Cut) wait(ctx context.Context, d time.Duration) error {
	if c.Sleep != nil {
		return c.Sleep(ctx, d)
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// healthy asks the target's health URL, and wants a 2xx without a redirect.
func (c *Cut) healthy(ctx context.Context, u string) bool {
	if u == "" {
		return true
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return false
	}
	cc := http.Client{}
	if c.Client != nil {
		cc = *c.Client
	}
	cc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := cc.Do(req)
	if err != nil {
		return false
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
	resp.Body.Close()
	return resp.StatusCode/100 == 2
}

// positions reads where each dataset's source is: for Postgres, its WAL
// position. A dataset with none reads as "".
func (c *Cut) positions(ctx context.Context) ([]string, error) {
	var out []string
	for _, d := range c.Sync.sets {
		p, ok := d.(interface {
			Position(context.Context) (string, error)
		})
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

func addFinding(p *CutPlan, code string, block bool) {
	for _, f := range p.Findings {
		if f == code {
			p.Blocked = p.Blocked || block
			return
		}
	}
	p.Findings = append(p.Findings, code)
	p.Blocked = p.Blocked || block
}

// setDigest gives every plan a digest of its own, even of the same records:
// a cutover refused because a writer was still going, or made again after a
// return, is planned and approved again.
func setDigest(p *CutPlan, m Move) {
	parts := [][]byte{[]byte("cutover-plan-v3"), []byte(rand.Text()), []byte(m.digest()), []byte(strconv.FormatBool(p.Back)), []byte(strconv.FormatBool(p.Resume)), []byte(strconv.Itoa(p.Standby))}
	for _, ch := range p.changes {
		for _, s := range []*dns.RecordSet{ch.Old, ch.New} {
			if s == nil {
				parts = append(parts, []byte("-"))
				continue
			}
			parts = append(parts, []byte(s.Name), []byte(s.Type), []byte(strconv.Itoa(s.TTL)), []byte(strconv.FormatBool(s.Proxied)))
			for _, v := range s.Values {
				parts = append(parts, []byte(v))
			}
		}
	}
	parts = append(parts, []byte(strings.Join(p.Findings, ",")))
	p.Digest = transfer.Digest(parts...)
}

// Plan reads the records and the data and writes nothing. A cutover is
// blocked until the data checks out and the target answers its health
// check; a return is blocked once the target's data differs from the
// source's, or someone else changed the records.
//
// A cutover that stopped part way, on a failed call it couldn't undo or a
// process that died, leaves some records changed. The next plan finishes
// it (Resume) with the changes still to make, and a return puts back the
// ones that were made; either counts the rest as already there.
func (c *Cut) Plan(ctx context.Context, w *transfer.Workspace, back bool) (CutPlan, error) {
	m := c.Sync.move
	p := CutPlan{Back: back}
	var saved cutState
	err := w.Load(cutoverState, &saved)
	switch {
	case err == nil:
	case errors.Is(err, fs.ErrNotExist):
		if back {
			return p, ErrNoCutover
		}
		// A cutover died between making the way back and saving it.
		// The slot holds the target's WAL; this cutover's makeBack drops
		// it before making its own, and until then the plan says so. A
		// plan writes nothing, so it doesn't drop it itself.
		if pendingBack(w) {
			addFinding(&p, findingWayBackLeft, false)
		}
	default:
		return p, err
	}
	if !back && err == nil && saved.Returned == nil {
		if saved.Complete {
			return p, ErrCutOver
		}
		// Stopped part way. If a record isn't as the first plan found it,
		// this plan finishes the cutover. If every one is, nothing
		// happened yet and the plan is a first one, with every check.
		// A record that is neither as found nor as planned counts as
		// changed: a host that changes a type in two calls can stop
		// between them, and a first plan made then would save an empty
		// name over the set a return has to put back. Finishing blocks
		// on it below instead, and the saved sets stay.
		for _, ch := range saved.Changes {
			before, _, err := c.stands(ctx, ch)
			if err != nil {
				return p, err
			}
			if !before {
				p.Resume = true
			}
		}
	}
	if back && saved.Returned != nil {
		return p, ErrNoCutover
	}

	if p.Resume {
		// The data checked out and the source was quiet before the first
		// record changed, and the target has been taking traffic on the
		// changed names since. So the two sides aren't compared again:
		// they're expected to differ, with the target ahead.
		p.ReturnUntil = saved.ReturnUntil
		planned := map[string]bool{}
		for _, ch := range saved.Changes {
			for _, s := range []*dns.RecordSet{ch.Old, ch.New} {
				if s != nil {
					planned[strings.ToLower(s.Name)+" "+s.Type] = true
				}
			}
		}
		// A record the first plan found at the target has to be there still.
		for _, want := range m.Cutover.Records {
			if planned[want.key()] {
				continue
			}
			cur, ok, err := c.Zone.Get(ctx, want.Name, want.Type)
			if err != nil {
				return p, err
			}
			if want.gone() && !ok || !want.gone() && ok && cur.Equal(*want.set(&cur)) {
				p.Already++
			} else {
				addFinding(&p, findingDNSChanged, true)
			}
		}
		for _, ch := range saved.Changes {
			before, after, err := c.stands(ctx, ch)
			if err != nil {
				return p, err
			}
			switch {
			case after:
				p.Already++
				continue
			case before:
			default:
				addFinding(&p, findingDNSChanged, true)
			}
			p.changes = append(p.changes, ch)
			if ch.Old == nil {
				p.Created++
			}
			p.Change++
		}
		if !c.healthy(ctx, m.Cutover.Health) {
			addFinding(&p, findingUnhealthy, true)
		}
		setDigest(&p, m)
		return p, nil
	}

	if back {
		p.ReturnUntil = saved.ReturnUntil
		// Last change first: a name that became a CNAME has to stop being
		// one before it gets its second address record back.
		for i := len(saved.Changes) - 1; i >= 0; i-- {
			ch := saved.Changes[i]
			before, after, err := c.stands(ctx, ch)
			if err != nil {
				return p, err
			}
			switch {
			case after:
			case before:
				// The cutover stopped before this one changed, or an
				// earlier return already put it back.
				p.Already++
				continue
			default:
				addFinding(&p, findingDNSChanged, true)
			}
			p.changes = append(p.changes, cutPair{Old: ch.New, New: ch.Old})
			if ch.Old == nil {
				p.Created++
			}
			p.Change++
		}
		if !c.Sync.now().Before(saved.ReturnUntil) {
			addFinding(&p, findingReturnOver, false)
		}
	} else {
		if err := c.forward(ctx, &p); err != nil {
			return p, err
		}
		if !c.healthy(ctx, m.Cutover.Health) {
			addFinding(&p, findingUnhealthy, true)
		}
	}

	// Nothing may be left behind either way: a cutover waits for the
	// target to have the source's data, and a return for the old side to
	// have whatever the target took since.
	if back {
		codes, err := c.behind(ctx, saved)
		if err != nil {
			return p, err
		}
		for _, code := range codes {
			addFinding(&p, code, true)
		}
		if p.positions, err = c.backPositions(ctx); err != nil {
			return p, err
		}
		p.Standby = c.ways()
		setDigest(&p, m)
		return p, nil
	}
	st, err := c.Sync.Check(ctx)
	if err != nil {
		return p, err
	}
	if len(c.Sync.sets) > 0 && !st.Verified() {
		addFinding(&p, findingNotCaughtUp, true)
	}
	// A move file that asks for a way back gets one or doesn't cut over:
	// a target that can't hold it is said now, not at the return.
	for _, d := range c.Sync.sets {
		set, ok := d.(*pgSet)
		if !ok || !set.twoWay {
			continue
		}
		ready, err := set.backReady(ctx)
		if err != nil {
			return p, err
		}
		if !ready {
			addFinding(&p, findingReturnSetup, true)
		}
		p.Standby++
	}
	if p.positions, err = c.positions(ctx); err != nil {
		return p, err
	}
	setDigest(&p, m)
	return p, nil
}

// addressTypes are the types that can share a name.
var addressTypes = []string{"A", "AAAA"}

// forward works out a first cutover's changes, a name at a time, from what
// each name has now and what the move file says it ends with.
func (c *Cut) forward(ctx context.Context, p *CutPlan) error {
	records := c.Sync.move.Cutover.Records
	add := func(old, new *dns.RecordSet) {
		p.changes = append(p.changes, cutPair{Old: old, New: new})
		p.Change++
		if old == nil {
			p.Created++
		}
	}
	done := map[string]bool{}
	for _, first := range records {
		name := strings.ToLower(first.Name)
		if done[name] {
			continue
		}
		done[name] = true
		listed := map[string]Record{}
		for _, r := range records {
			if strings.ToLower(r.Name) == name {
				listed[r.Type] = r
			}
		}
		now := map[string]*dns.RecordSet{}
		// any is a set the name has today, for Cloudflare's setting on a
		// record of a type the name doesn't have yet.
		var any *dns.RecordSet
		for _, typ := range []string{"CNAME", "A", "AAAA"} {
			cur, ok, err := c.Zone.Get(ctx, first.Name, typ)
			if err != nil {
				return err
			}
			if ok {
				now[typ] = &cur
				if any == nil {
					any = &cur
				}
			}
		}
		want := func(r Record) *dns.RecordSet {
			if cur := now[r.Type]; cur != nil {
				return r.set(cur)
			}
			return r.set(any)
		}

		if alias, ok := listed["CNAME"]; ok && !alias.gone() {
			set := want(alias)
			var addresses []*dns.RecordSet
			for _, typ := range []string{"AAAA", "A"} {
				if now[typ] != nil {
					addresses = append(addresses, now[typ])
				} else if _, ok := listed[typ]; ok {
					p.Already++
				}
			}
			switch {
			case now["CNAME"] != nil && now["CNAME"].Equal(*set):
				p.Already++
			case now["CNAME"] != nil:
				add(now["CNAME"], set)
			case len(addresses) == 0:
				add(nil, set)
			default:
				// The address records go, and the last of them is
				// replaced by the CNAME in one change.
				for _, a := range addresses[:len(addresses)-1] {
					add(a, nil)
				}
				add(addresses[len(addresses)-1], set)
			}
			continue
		}

		if alias := now["CNAME"]; alias != nil {
			// The name is a CNAME today. The first address it ends with
			// replaces it in one change, and the rest are new.
			for _, typ := range addressTypes {
				r, ok := listed[typ]
				switch {
				case !ok:
				case r.gone():
					p.Already++
				case alias != nil:
					add(alias, want(r))
					alias = nil
				default:
					add(nil, want(r))
				}
			}
			if alias != nil {
				if _, ok := listed["CNAME"]; ok {
					add(alias, nil)
				} else {
					// Nothing replaces it and nothing says it goes.
					addFinding(p, findingUnlisted, true)
				}
			}
			continue
		}

		if _, ok := listed["CNAME"]; ok {
			p.Already++
		}
		for _, typ := range addressTypes {
			r, ok := listed[typ]
			cur := now[typ]
			switch {
			case !ok && cur != nil:
				// The name would go on answering with this one.
				addFinding(p, findingUnlisted, true)
			case !ok:
			case r.gone() && cur == nil:
				p.Already++
			case r.gone():
				add(cur, nil)
			case cur != nil && cur.Equal(*want(r)):
				p.Already++
			default:
				add(cur, want(r))
			}
		}
	}
	return nil
}

// stands reads where one change is: before it, with the zone as the plan
// found it, or after it. A change of type reads both types, since a name
// with the old set gone and the new one not there yet is neither.
func (c *Cut) stands(ctx context.Context, ch cutPair) (before, after bool, err error) {
	read := func(s *dns.RecordSet) (there, same bool, err error) {
		cur, ok, err := c.Zone.Get(ctx, s.Name, s.Type)
		return ok, ok && cur.Equal(*s), err
	}
	switch {
	case ch.Old == nil:
		there, same, err := read(ch.New)
		return !there, same, err
	case ch.New == nil:
		there, same, err := read(ch.Old)
		return same, !there, err
	case ch.Old.Type == ch.New.Type:
		cur, ok, err := c.Zone.Get(ctx, ch.New.Name, ch.New.Type)
		return ok && cur.Equal(*ch.Old), ok && cur.Equal(*ch.New), err
	}
	oldThere, oldSame, err := read(ch.Old)
	if err != nil {
		return false, false, err
	}
	newThere, newSame, err := read(ch.New)
	return oldSame && !newThere, newSame && !oldThere, err
}

// Apply carries out an approved plan. A cutover checks again that nothing
// moved on the source over Settle and the data still checks out, sets the
// target's sequences, keeps every record's old set in w, and only then
// changes DNS. If one change fails, the ones before it are put back and
// read back; when that can't be done either the error is ErrCutPartial and
// the workspace keeps the cutover as under way, so a return can finish the
// job. Apply holds the workspace's lock, so it is refused with
// transfer.ErrBusy while a sync runs there.
func (c *Cut) Apply(ctx context.Context, w *transfer.Workspace, p CutPlan, approved string) (CutPlan, error) {
	m := c.Sync.move
	if p.Digest == "" || subtle.ConstantTimeCompare([]byte(approved), []byte(p.Digest)) != 1 {
		return p, ErrApproval
	}
	if p.Blocked {
		return p, ErrBlocked
	}
	// A sync still running would copy the source's old data over what the
	// target takes after the cutover, so it has to end first.
	unlock, err := w.Lock()
	if err != nil {
		return p, err
	}
	defer unlock()
	// One approval, one go: the digest is spent before anything changes.
	if err := w.Spend(p.Digest); err != nil {
		return p, err
	}
	for _, ch := range p.changes {
		before, _, err := c.stands(ctx, ch)
		if err != nil {
			return p, err
		}
		if !before {
			addFinding(&p, findingDNSChanged, true)
			return p, dns.ErrChanged
		}
	}

	now := c.Sync.now().UTC()
	state := cutState{Zone: m.Cutover.Zone, At: now}
	if p.Back || p.Resume {
		// The sequences were set before the first change and the target
		// has used them since, so finishing a cutover never sets them
		// again.
		if err := w.Load(cutoverState, &state); err != nil {
			return p, err
		}
		if state.Returned != nil || state.Complete && p.Resume {
			return p, ErrApproval
		}
		if p.Resume {
			p.ReturnUntil = state.ReturnUntil
		}
		if p.Back {
			// The target goes quiet and the old side has all of it, or
			// DNS stays where it is.
			if err := c.settleBack(ctx, &p, state); err != nil {
				return p, err
			}
		}
	} else {
		// A plan made before another one cut over finds nothing left to
		// change. It must not set the target's sequences back to the
		// source's, or write a new way back over the one that stands.
		var was cutState
		switch err := w.Load(cutoverState, &was); {
		case errors.Is(err, fs.ErrNotExist):
		case err != nil:
			return p, err
		case was.Complete && was.Returned == nil:
			return p, ErrCutOver
		}
		// With no data there's nothing for the source to finish writing
		// and nothing to compare: DNS is all that changes.
		if len(c.Sync.sets) > 0 {
			if err := c.wait(ctx, c.Settle); err != nil {
				return p, err
			}
		}
		pos, err := c.positions(ctx)
		if err != nil {
			return p, err
		}
		for i := range pos {
			if i >= len(p.positions) || pos[i] != p.positions[i] {
				addFinding(&p, findingWritesGoingOn, true)
				return p, ErrBlocked
			}
		}
		st, err := c.Sync.Check(ctx)
		if err != nil {
			return p, err
		}
		if len(c.Sync.sets) > 0 && !st.Verified() {
			addFinding(&p, findingNotCaughtUp, true)
			return p, ErrBlocked
		}
		for _, d := range c.Sync.sets {
			if s, ok := d.(interface {
				Sequences(context.Context) (int, error)
			}); ok {
				if _, err := s.Sequences(ctx); err != nil {
					return p, err
				}
			}
		}
		// The way back starts here, with both sides the same and before
		// a name points at the target: the slot holds everything the
		// target takes from now on. Said on disk first, so a process
		// that dies before the state is saved leaves a mark of the slot.
		if c.ways() > 0 {
			if err := w.Save(backPending, wayBackMark{Pending: true}); err != nil {
				return p, err
			}
		}
		for _, d := range c.Sync.sets {
			if set, ok := d.(*pgSet); ok && set.twoWay {
				if err := set.makeBack(ctx); err != nil {
					c.dropBack(ctx, w, &p)
					return p, err
				}
			}
		}
		for _, d := range st.Datasets {
			state.Extras = append(state.Extras, d.extras)
		}
		days := m.Cutover.ReturnDays
		if days == 0 {
			days = defaultReturnDays
		}
		state.ReturnUntil = now.AddDate(0, 0, days)
		state.Changes = p.changes
		p.ReturnUntil = state.ReturnUntil
		// The way back is on disk before the first record changes.
		if err := w.Save(cutoverState, state); err != nil {
			c.dropBack(ctx, w, &p)
			return p, err
		}
		// The state knows of the slot now. A mark left standing only
		// matters with no state, so failing to clear it is harmless.
		if c.ways() > 0 {
			_ = w.Save(backPending, wayBackMark{})
		}
	}

	// A return that an earlier try left part way stays one whatever this
	// try manages to put back.
	returning := state.Returning != nil
	if p.Back && !returning && len(p.changes) > 0 {
		// Said on disk before the first record goes back: from here a
		// name may be the old side's, and a reverse sync would write
		// the target's rows over what the old side takes.
		state.Returning = &now
		if err := w.Save(cutoverState, state); err != nil {
			return p, err
		}
	}

	for i, ch := range p.changes {
		err := c.Zone.Replace(ctx, ch.Old, ch.New)
		if err == nil {
			continue
		}
		if !c.putBack(ctx, p.changes[:i+1]) {
			// Nothing more is saved: the workspace still says this is
			// under way and still has every old set.
			return p, fmt.Errorf("%w: %w", ErrCutPartial, err)
		}
		if p.Back && !returning {
			// Every name is the target's again, so the cutover stands
			// as it did.
			state.Returning = nil
			if w.Save(cutoverState, state) != nil {
				return p, fmt.Errorf("%w: %w", ErrCutPartial, err)
			}
		}
		// A first try put back is a cutover that didn't happen. One that
		// was being finished, or a return, is where it was before.
		if !p.Back && !p.Resume {
			state.Returned = &now
			if w.Save(cutoverState, state) != nil {
				return p, fmt.Errorf("%w: %w", ErrCutPartial, err)
			}
			c.dropBack(ctx, w, &p)
		}
		if p.Resume {
			// Back to where this run found it, which is part way.
			return p, fmt.Errorf("%w: %w", ErrCutPartial, err)
		}
		return p, err
	}
	if p.Back {
		state.Returned = &now
		p.At = now
		// The way back ends with the return. DNS has changed, so one
		// that can't be ended is said and the return stands.
		if !c.endBack(ctx) {
			addFinding(&p, findingWayBackLeft, false)
		}
	} else {
		state.Complete = true
		// A cutover finished on a second run began at its first.
		p.At = state.At
	}
	// Every record has changed by now, so a workspace that can't take the
	// new state is a cutover left under way, not one that didn't happen.
	if err := w.Save(cutoverState, state); err != nil {
		return p, fmt.Errorf("%w: %w", ErrCutPartial, err)
	}
	for _, ch := range p.changes {
		// The host took every change, so a read that fails here isn't a
		// reason to call the cutover failed.
		if _, after, err := c.stands(ctx, ch); err == nil && !after {
			addFinding(&p, findingDNSChanged, false)
		}
	}
	return p, nil
}

// putBack undoes the changes before the last of done, the one that failed,
// and reads every one of them back. It reports whether the zone is as it
// was before Apply. It runs on a context of its own: an interrupt that
// failed a change would fail the undo too.
func (c *Cut) putBack(ctx context.Context, done []cutPair) bool {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
	defer cancel()
	back := true
	for j := len(done) - 2; j >= 0; j-- {
		if c.Zone.Replace(ctx, done[j].New, done[j].Old) != nil {
			back = false
		}
	}
	for _, ch := range done {
		if before, _, err := c.stands(ctx, ch); err != nil || !before {
			back = false
		}
	}
	return back
}
