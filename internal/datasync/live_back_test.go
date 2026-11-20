//go:build live

package datasync

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"github.com/zoctl/cli/internal/transfer"
	"github.com/zoctl/cli/internal/transfer/dns"
)

// twoWay is live's move with a way back, a cutover over a zone in memory,
// and everything the three commands open: the move's sync, the cut a
// return runs on, and the reverse sync.
type twoWay struct {
	*live
	move Move
	env  func(string) string
	zone *memZone
	w    *transfer.Workspace
}

func newTwoWay(t *testing.T) *twoWay {
	l := newLive(t)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		// The old side's subscription first, let go of its slot, or its
		// database can't be dropped.
		if c, err := liveConnect(ctx, l.src); err == nil {
			for _, sql := range []string{"ALTER SUBSCRIPTION zoctl_back_orders DISABLE", "ALTER SUBSCRIPTION zoctl_back_orders SET (slot_name = NONE)", "DROP SUBSCRIPTION zoctl_back_orders"} {
				c.Exec(ctx, sql)
			}
			c.Close(ctx)
		}
		if c, err := liveConnect(ctx, l.dst); err == nil {
			c.Exec(ctx, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'zoctl_back_orders' AND NOT active AND database = current_database()")
			c.Close(ctx)
		}
	})
	m, err := ParseMove([]byte(fmt.Sprintf(`{"version":1,"source":"aws","target":"gcp","datasets":[{"name":"orders","kind":"postgres",
	 "from_env":"SOURCE_DB","to_env":"TARGET_DB","publication":"zoctl_live","slot":%q,"copy_schema":true,
	 "back_env":"STANDBY_DB","back_from_env":"TARGET_DB_REPL"}],"limits":{"max_objects":1,"max_bytes":1,"hours":1},
	 "cutover":{"zone":"cloudflare://0123456789abcdef0123456789abcdef","credentials":"DNS","records":[{"name":"www.shop.example.test.","type":"A","ttl":60,"values":["203.0.113.9"]}]}}`, l.slot)))
	if err != nil {
		t.Fatal(err)
	}
	// What the old side connects to the target as: a role on the target
	// that can replicate and nothing more. The reverse copies no table,
	// so it reads none.
	secret := make([]byte, 16)
	rand.Read(secret)
	l.exec(l.dst, "CREATE ROLE "+l.slot+" LOGIN REPLICATION PASSWORD '"+hex.EncodeToString(secret)+"'")
	reach, _ := url.Parse(l.dst)
	reach.User = url.UserPassword(l.slot, hex.EncodeToString(secret))
	keys := map[string]string{"SOURCE_DB": l.mover, "TARGET_DB": l.dst, "STANDBY_DB": l.src, "TARGET_DB_REPL": reach.String()}
	w, err := transfer.OpenWorkspace(filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	return &twoWay{live: l, move: m, env: func(k string) string { return keys[k] }, w: w,
		zone: &memZone{sets: map[string]dns.RecordSet{wwwOld.Name + " A": wwwOld}}}
}

// forward is the move's own sync.
func (l *twoWay) forward() *Sync {
	l.t.Helper()
	s, err := Open(l.move, Options{Env: l.env})
	if err != nil {
		l.t.Fatal(err)
	}
	l.t.Cleanup(s.Close)
	return s
}

// reverse is what zoctl sync -back opens.
func (l *twoWay) reverse() *Sync {
	l.t.Helper()
	s, err := OpenBack(l.move, Options{Env: l.env})
	if err != nil {
		l.t.Fatal(err)
	}
	l.t.Cleanup(s.Close)
	return s
}

// cut is what zoctl cutover opens, and with back, zoctl cutover -back. The
// zone is the one thing that isn't real.
func (l *twoWay) cut(back bool) *Cut {
	l.t.Helper()
	c := &Cut{Sync: l.forward(), Zone: l.zone, Client: &http.Client{Transport: healthCheck(200)}, Settle: 2 * time.Second}
	if back {
		for _, d := range l.move.Datasets {
			ds, err := openBack(d, Options{Env: l.env}, false)
			if err != nil {
				l.t.Fatal(err)
			}
			c.back = append(c.back, ds)
			l.t.Cleanup(ds.Close)
		}
	}
	return c
}

// apply plans and approves a cutover or a return. A session reports its
// writes to the server's counters up to ten seconds after it goes idle, so
// the plan waits that out, as a person stopping their writers would.
func (l *twoWay) apply(back bool) CutPlan {
	l.t.Helper()
	time.Sleep(12 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	c := l.cut(back)
	p, err := c.Plan(ctx, l.w, back)
	if err != nil || p.Blocked {
		l.t.Fatalf("plan (back %v): %+v %v", back, p, err)
	}
	done, err := c.Apply(ctx, l.w, p, p.Digest)
	if err != nil {
		l.t.Fatalf("apply (back %v): %+v %v", back, done, err)
	}
	return done
}

func (l *twoWay) same(what string) {
	l.t.Helper()
	for _, q := range []string{
		`SELECT count(*)::text || ' ' || coalesce(md5(string_agg(id::text || note, ',' ORDER BY id)), '') FROM orders`,
		`SELECT count(*)::text || ' ' || coalesce(md5(string_agg(id::text || order_id::text || coalesce(price::text, ''), ',' ORDER BY id)), '') FROM items`,
	} {
		if a, b := l.value(l.src, q), l.value(l.dst, q); a != b {
			l.t.Fatalf("%s: the two sides differ (%s on the old side, %s on the target)", what, a, b)
		}
	}
}

// A move there and back on two real servers: sync, cutover, writes on the
// target, the reverse sync, the return, then all of it a second time. The
// DNS zone is in memory.
func TestLivePostgresBothWays(t *testing.T) {
	l := newTwoWay(t)
	ctx := context.Background()
	const here = `subdbid = (SELECT oid FROM pg_database WHERE datname = current_database())`

	fwd := l.forward()
	if st := l.run(fwd, l.w, 6); !st.Verified() {
		t.Fatalf("first run: %+v", st)
	}
	if got := l.value(l.dst, `SELECT suborigin FROM pg_subscription WHERE subname = 'zoctl_orders' AND `+here); got != "none" {
		t.Fatalf("the move's subscription takes changes of origin %s", got)
	}
	// copy_schema made the target's sequences; the cutover sets them.
	l.exec(l.src, "GRANT SELECT ON SEQUENCE orders_id_seq, items_id_seq TO "+l.slot)

	// Before a cutover nothing writes to the source: the reverse sync has
	// no way back to read, and the workspace refuses it anyway.
	rev := l.reverse()
	if p, err := rev.Plan(ctx); err != nil || !p.Blocked() {
		t.Fatalf("a reverse plan before the cutover: %+v %v", p, err)
	}
	if got := l.value(l.src, `SELECT count(*)::text FROM pg_subscription WHERE `+here); got != "0" {
		t.Fatalf("%s subscriptions on the source before the cutover", got)
	}

	l.apply(false)
	if got := l.value(l.dst, `SELECT count(*)::text FROM pg_replication_slots WHERE slot_name = 'zoctl_back_orders' AND plugin = 'pgoutput'`); got != "1" {
		t.Fatalf("%s slots for the way back on the target", got)
	}
	if got := l.value(l.dst, `SELECT count(*)::text FROM pg_publication_tables WHERE pubname = 'zoctl_back_orders'`); got != "2" {
		t.Fatalf("the way back publishes %s tables", got)
	}
	if got, want := l.value(l.dst, `SELECT last_value::text FROM orders_id_seq`), l.value(l.src, `SELECT last_value::text FROM orders_id_seq`); got != want {
		t.Fatalf("the target's sequence at %s, the source's at %s", got, want)
	}
	// Nothing written on the target yet: a return could go now, with no
	// reverse sync. WAL that isn't the move's doesn't change that.
	l.exec(l.dst, `CREATE TABLE elsewhere (i int)`)
	l.exec(l.dst, `INSERT INTO elsewhere SELECT generate_series(1, 1000)`)
	back := l.cut(true)
	if p, err := back.Plan(ctx, l.w, true); err != nil || p.Blocked {
		t.Fatalf("back with nothing new on the target: %+v %v", p, err)
	}

	// The target takes writes: a row, a change and a delete.
	l.exec(l.dst, `INSERT INTO orders (id, note) VALUES (nextval('orders_id_seq'), 'on the target')`)
	l.exec(l.dst, `UPDATE items SET price = 2 WHERE id = 9`)
	l.exec(l.dst, `DELETE FROM items WHERE id = 11`)
	if p, err := back.Plan(ctx, l.w, true); err != nil || !p.Blocked || !contains(p.Findings, findingStandbyBehind) {
		t.Fatalf("back with the old side behind: %+v %v", p, err)
	}
	// An update leaves the row counts alike, and still counts.
	if st, err := rev.Check(ctx); err != nil || st.Verified() {
		t.Fatalf("the reverse check with the old side behind: %+v %v", st, err)
	}

	if st := l.run(rev, l.w, 6); !st.Verified() {
		t.Fatalf("reverse sync: %+v", st)
	}
	l.same("after the reverse sync")
	if got := l.value(l.src, `SELECT note FROM orders ORDER BY id DESC LIMIT 1`); got != "on the target" {
		t.Fatalf("the target's row didn't reach the old side: %s", got)
	}
	if got := l.value(l.src, `SELECT subenabled::text || ' ' || suborigin FROM pg_subscription WHERE subname = 'zoctl_back_orders' AND `+here); got != "false none" {
		t.Fatalf("the old side's subscription after the run: %s", got)
	}
	// More writes, and a second window takes the subscription up again.
	l.exec(l.dst, `INSERT INTO orders (id, note) VALUES (nextval('orders_id_seq'), 'second window')`)
	if st := l.run(rev, l.w, 6); !st.Verified() {
		t.Fatalf("second reverse sync: %+v", st)
	}
	l.same("after the second reverse sync")

	// Switching back: the old side's sequence follows, and the way back
	// leaves the target.
	done := l.apply(true)
	if len(done.Findings) != 0 {
		t.Fatalf("return: %+v", done)
	}
	if got, want := l.value(l.src, `SELECT last_value::text FROM orders_id_seq`), l.value(l.dst, `SELECT last_value::text FROM orders_id_seq`); got != want {
		t.Fatalf("the old side's sequence at %s, the target's at %s", got, want)
	}
	if got := l.value(l.dst, `SELECT (SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'zoctl_back_orders')::text || ' ' || (SELECT count(*) FROM pg_publication WHERE pubname = 'zoctl_back_orders')::text`); got != "0 0" {
		t.Fatalf("slots and publications the return left on the target: %s", got)
	}
	if got, _, _ := l.zone.Get(ctx, wwwOld.Name, "A"); !got.Equal(wwwOld) {
		t.Fatalf("www after the return: %+v", got)
	}

	// The source takes writes again, and the move's sync carries them
	// without sending the target's own rows round a second time: one
	// that came back would collide with itself and stop the subscription.
	l.exec(l.src, `INSERT INTO orders (note) VALUES ('back on the source')`)
	if st := l.run(fwd, l.w, 6); !st.Verified() {
		t.Fatalf("the move's sync after the return: %+v", st)
	}
	l.same("after the return and a sync")

	// And round again: a second cutover makes the way back anew, and the
	// old side's subscription, which remembers the first, reads it.
	l.apply(false)
	l.exec(l.dst, `INSERT INTO orders (id, note) VALUES (nextval('orders_id_seq'), 'second time on the target')`)
	l.exec(l.dst, `DELETE FROM orders WHERE note = 'second window'`)
	if st := l.run(rev, l.w, 6); !st.Verified() {
		t.Fatalf("reverse sync after the second cutover: %+v", st)
	}
	l.same("after the second cutover's reverse sync")
	l.apply(true)
	l.exec(l.src, `INSERT INTO orders (note) VALUES ('the last')`)
	if st := l.run(fwd, l.w, 6); !st.Verified() {
		t.Fatalf("the move's sync after the second return: %+v", st)
	}
	l.same("at the end")
}
