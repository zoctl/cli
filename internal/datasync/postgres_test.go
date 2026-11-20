package datasync

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zoctl/cli/internal/transfer"
)

// fakePG is one Postgres server in memory, enough of its catalog for a
// dataset: answers are picked by what the query starts with.
type fakePG struct {
	mu       sync.Mutex
	noSystem bool // the role may not ask which server this is
	// rels are the tables the subscription has ready, when they aren't
	// the first `ready` of the two sourcePG publishes.
	rels     []table
	wal      string
	pubs     map[string]bool
	slots    map[string][3]string // plugin, type, this database
	pubTabs  []table
	noID     int
	tables   map[string]int // ident -> rows
	columns  map[string][][]string
	pk       map[string][]string
	subs     map[string]bool // name -> enabled
	ready    int
	lag      int64
	seqs     [][]string
	setvals  map[string]int64
	execs    []string
	queries  []string
	conninfo string
	lsn      string // what Position reads

	active     int // reads until a walsender lets go of the slot
	version    string
	othersOnly bool // the WAL past the slot has nothing of the publication's
	noPeek     bool
	noFreeSlot bool
	peeks      int
	hidden     [][]string      // sequences the role can't read
	failExec   string          // a statement starting with this fails
	theirs     map[string]bool // subscriptions on another slot or publication
	asked      [][2]string     // the slot and publication a subscription was held to
	made       []string        // slots made here, in order

	// waiting are tables the subscription lists as not yet started (i),
	// and stuck is whether its apply worker has run a while with no
	// table-sync worker.
	waiting []table
	stuck   bool

	owned      map[string][][]string // ident -> sequences its columns own
	cons       map[string][][]string // ident -> constraints
	indexes    map[string][][]string // ident -> index definitions
	searchPath string
}

func newPG() *fakePG {
	return &fakePG{wal: "logical", pubs: map[string]bool{}, slots: map[string][3]string{}, tables: map[string]int{},
		columns: map[string][][]string{}, pk: map[string][]string{}, subs: map[string]bool{}, setvals: map[string]int64{},
		owned: map[string][][]string{}, cons: map[string][][]string{}, indexes: map[string][][]string{}, searchPath: `"$user", public`}
}

func (f *fakePG) Close() {}

func (f *fakePG) Rows(_ context.Context, sql string, args ...any) ([][]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queries = append(f.queries, sql)
	arg := ""
	if len(args) > 0 {
		arg, _ = args[0].(string)
	}
	row := func(v ...string) [][]string { return [][]string{v} }
	switch {
	case strings.HasPrefix(sql, "SELECT system_identifier::text"):
		if f.noSystem {
			return nil, ErrPostgres
		}
		return row(fmt.Sprintf("%p shop", f)), nil
	case strings.HasPrefix(sql, "SELECT current_setting('wal_level')"):
		return row(f.wal), nil
	case strings.HasPrefix(sql, "SELECT pubname::text FROM pg_publication"):
		if f.pubs[arg] {
			return row(arg), nil
		}
		return nil, nil
	case strings.HasPrefix(sql, "SELECT plugin::text"):
		if s, ok := f.slots[arg]; ok {
			return row(s[0], s[1], s[2]), nil
		}
		return nil, nil
	case strings.HasPrefix(sql, "SELECT schemaname::text, tablename::text FROM pg_publication_tables"):
		var out [][]string
		for _, t := range f.pubTabs {
			out = append(out, []string{t.schema, t.name})
		}
		return out, nil
	case strings.HasPrefix(sql, "SELECT count(*)::text FROM pg_publication_tables"):
		return row(strconv.Itoa(f.noID)), nil
	case strings.HasPrefix(sql, "SELECT subenabled::text FROM pg_subscription"):
		if e, ok := f.subs[arg]; ok {
			return row(strconv.FormatBool(e)), nil
		}
		return nil, nil
	case strings.HasPrefix(sql, "SELECT (coalesce(subslotname::text, '') = $2 AND subpublications::text[] = ARRAY[$3]::text[])::text FROM pg_subscription"):
		if _, ok := f.subs[arg]; !ok {
			return nil, nil
		}
		f.asked = append(f.asked, [2]string{args[1].(string), args[2].(string)})
		return row(strconv.FormatBool(!f.theirs[arg])), nil
	case strings.HasPrefix(sql, "SELECT subname::text FROM pg_subscription"):
		if _, ok := f.subs[arg]; ok {
			return row(arg), nil
		}
		return nil, nil
	case strings.HasPrefix(sql, "SELECT coalesce(to_regclass($1)::text, '')"):
		if _, ok := f.tables[arg]; ok {
			return row(arg), nil
		}
		if _, ok := f.setvals[arg]; ok {
			return row(arg), nil
		}
		return row(""), nil
	case strings.HasPrefix(sql, "SELECT EXISTS (SELECT 1 FROM "):
		t := strings.TrimSuffix(strings.TrimPrefix(sql, "SELECT EXISTS (SELECT 1 FROM "), ")::text")
		return row(strconv.FormatBool(f.tables[t] > 0)), nil
	case strings.HasPrefix(sql, "SELECT a.attname::text, format_type"):
		return f.columns[arg], nil
	case strings.HasPrefix(sql, "SELECT current_setting('search_path')"):
		return row(f.searchPath), nil
	case strings.HasPrefix(sql, "SELECT set_config('search_path', 'pg_catalog', false)"):
		f.searchPath = "pg_catalog"
		return row(f.searchPath), nil
	case strings.HasPrefix(sql, "SELECT set_config('search_path', $1, false)"):
		f.searchPath = arg
		return row(f.searchPath), nil
	case strings.HasPrefix(sql, "SELECT n.nspname::text, c.relname::text, format_type(s.seqtypid"):
		return f.owned[arg], nil
	case strings.HasPrefix(sql, "SELECT conname::text, contype::text, pg_get_constraintdef"):
		return f.cons[arg], nil
	case strings.HasPrefix(sql, "SELECT pg_get_indexdef("):
		return f.indexes[arg], nil
	case strings.HasPrefix(sql, "SELECT a.attname::text FROM pg_index"):
		var out [][]string
		for _, k := range f.pk[arg] {
			out = append(out, []string{k})
		}
		return out, nil
	case strings.HasPrefix(sql, "SELECT r.srsubstate::text"):
		var out [][]string
		rels := f.rels
		if rels == nil {
			rels = []table{{"public", "orders"}, {"shop", "it's"}}[:f.ready]
		}
		for _, t := range rels {
			out = append(out, []string{"r", t.schema, t.name})
		}
		for _, t := range f.waiting {
			out = append(out, []string{"i", t.schema, t.name})
		}
		return out, nil
	case strings.HasPrefix(sql, "SELECT (count(*) > 0)::text FROM pg_stat_subscription s JOIN pg_stat_activity"):
		return row(strconv.FormatBool(f.stuck)), nil
	case strings.HasPrefix(sql, "SELECT coalesce(pg_wal_lsn_diff"):
		if _, ok := f.slots[arg]; !ok {
			return nil, nil
		}
		return row(strconv.FormatInt(f.lag, 10), strconv.FormatBool(f.active > 0)), nil
	case strings.HasPrefix(sql, "SELECT active::text FROM pg_replication_slots"):
		if _, ok := f.slots[arg]; !ok {
			return nil, nil
		}
		// A walsender lets go of the slot a few reads after its
		// subscription stops.
		if f.active > 0 {
			f.active--
		}
		return row(strconv.FormatBool(f.active > 0)), nil
	case strings.HasPrefix(sql, "SELECT current_setting('server_version_num')"):
		if f.version == "" {
			return row("170004"), nil
		}
		return row(f.version), nil
	case strings.HasPrefix(sql, "SELECT count(*)::text FROM pg_logical_slot_peek_binary_changes("):
		f.peeks++
		if f.active > 0 || f.noPeek {
			return nil, ErrPostgres
		}
		// What's past the slot is the move's unless a test says it's
		// only the server's own.
		if f.lag > 0 && !f.othersOnly {
			return row("1"), nil
		}
		return row("0"), nil
	case strings.HasPrefix(sql, "SELECT (current_setting('max_replication_slots')"):
		if f.noFreeSlot {
			return row("0"), nil
		}
		return row("9"), nil
	case strings.HasPrefix(sql, "SELECT count(*)::text FROM pg_subscription WHERE subname <> $1"):
		n := 0
		for name := range f.subs {
			if name != arg {
				n++
			}
		}
		return row(strconv.Itoa(n)), nil
	case strings.HasPrefix(sql, "SELECT lsn::text FROM pg_create_logical_replication_slot("):
		// A slot made before its publication can't be read.
		if _, ok := f.slots[arg]; ok || !f.pubs[arg] {
			return nil, ErrPostgres
		}
		f.slots[arg] = [3]string{"pgoutput", "logical", "true"}
		f.made = append(f.made, arg)
		return row("0/1"), nil
	case strings.HasPrefix(sql, "SELECT pg_drop_replication_slot("):
		if _, ok := f.slots[arg]; !ok || f.active > 0 {
			return nil, ErrPostgres
		}
		delete(f.slots, arg)
		return row(""), nil
	case strings.HasPrefix(sql, "SELECT coalesce(extract(epoch"):
		return row("7"), nil
	case strings.HasPrefix(sql, "SELECT count(*)::text FROM "):
		// A server refuses a table it doesn't have (42P01).
		n, ok := f.tables[strings.TrimPrefix(sql, "SELECT count(*)::text FROM ")]
		if !ok {
			return nil, errors.New("relation does not exist")
		}
		return row(strconv.Itoa(n)), nil
	case strings.HasPrefix(sql, "SELECT schemaname::text, sequencename::text, last_value::text"):
		return f.seqs, nil
	case strings.HasPrefix(sql, "SELECT schemaname::text, sequencename::text FROM pg_sequences\nWHERE NOT has_sequence_privilege"):
		return f.hidden, nil
	case strings.HasPrefix(sql, "SELECT 'writes ' || "):
		return row(f.lsn), nil
	case strings.HasPrefix(sql, "SELECT setval("):
		f.setvals[arg] = args[1].(int64)
		return row("1"), nil
	}
	return nil, errors.New("fakePG: unknown query " + sql)
}

func (f *fakePG) Exec(_ context.Context, sql string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.execs = append(f.execs, sql)
	if f.failExec != "" && strings.HasPrefix(sql, f.failExec) {
		return ErrPostgres
	}
	switch {
	case strings.HasPrefix(sql, "CREATE SCHEMA"), sql == "BEGIN", sql == "COMMIT", sql == "ROLLBACK",
		strings.HasPrefix(sql, "SAVEPOINT "), strings.HasPrefix(sql, "RELEASE SAVEPOINT "), strings.HasPrefix(sql, "ROLLBACK TO SAVEPOINT "),
		strings.HasPrefix(sql, "CREATE SEQUENCE "), strings.HasPrefix(sql, "ALTER SEQUENCE "), strings.HasPrefix(sql, "ALTER TABLE "),
		strings.HasPrefix(sql, "CREATE INDEX "), strings.HasPrefix(sql, "CREATE UNIQUE INDEX "):
	case strings.HasPrefix(sql, "CREATE TABLE "):
		name := strings.Fields(sql)[2]
		f.tables[name] = 0
	case strings.HasPrefix(sql, "CREATE SUBSCRIPTION "):
		name := strings.Trim(strings.Fields(sql)[2], `"`)
		f.subs[name] = true
		i := strings.Index(sql, " CONNECTION ")
		f.conninfo = sql[i+len(" CONNECTION ") : strings.Index(sql, " PUBLICATION ")]
	case strings.HasPrefix(sql, "ALTER SUBSCRIPTION ") && strings.HasSuffix(sql, " SET (origin = none)"):
	case strings.HasPrefix(sql, "ALTER SUBSCRIPTION "):
		name := strings.Trim(strings.Fields(sql)[2], `"`)
		f.subs[name] = strings.HasSuffix(sql, " ENABLE")
	case strings.HasPrefix(sql, "CREATE PUBLICATION "):
		name := strings.Trim(strings.Fields(sql)[2], `"`)
		if f.pubs[name] {
			return errors.New("fakePG: the publication exists")
		}
		f.pubs[name] = true
		f.pubTabs = nil
		for _, ident := range strings.Split(sql[strings.Index(sql, " FOR TABLE ")+len(" FOR TABLE "):], ", ") {
			parts := strings.SplitN(strings.Trim(ident, `"`), `"."`, 2)
			f.pubTabs = append(f.pubTabs, table{parts[0], parts[1]})
		}
	case strings.HasPrefix(sql, "DROP PUBLICATION IF EXISTS "):
		delete(f.pubs, strings.Trim(strings.Fields(sql)[4], `"`))
	default:
		return errors.New("fakePG: unknown statement")
	}
	return nil
}

// sourcePG is a source the customer set up: wal_level logical, a
// publication of two tables and a pgoutput slot.
func sourcePG() *fakePG {
	f := newPG()
	f.pubs["zoctl_pub"] = true
	f.slots["zoctl_slot"] = [3]string{"pgoutput", "logical", "true"}
	f.pubTabs = []table{{"public", "orders"}, {"shop", "it's"}}
	f.tables[`"public"."orders"`] = 3
	f.tables[`"shop"."it's"`] = 1
	f.columns[`"public"."orders"`] = [][]string{{"id", "bigint", "true", "", "", ""}, {"note", "character varying(80)", "false", "", "", ""}}
	f.columns[`"shop"."it's"`] = [][]string{{"weird \"col\"", "numeric(10,2)", "false", "", "", ""}}
	f.pk[`"public"."orders"`] = []string{"id"}
	return f
}

func pgSetFor(src, dst *fakePG, copySchema bool) *pgSet {
	return &pgSet{src: readOnly{src}, dst: dst, sourceURL: "postgres://repl:pa'ss@old.example.test/shop",
		publication: "zoctl_pub", slot: "zoctl_slot", sub: "zoctl_orders", copySchema: copySchema}
}

func TestPostgresPlanNeedsTheSourceSetUp(t *testing.T) {
	cases := map[string]func(f *fakePG){
		"wal_level":       func(f *fakePG) { f.wal = "replica" },
		"no publication":  func(f *fakePG) { delete(f.pubs, "zoctl_pub") },
		"no slot":         func(f *fakePG) { delete(f.slots, "zoctl_slot") },
		"wrong plugin":    func(f *fakePG) { f.slots["zoctl_slot"] = [3]string{"wal2json", "logical", "true"} },
		"other database":  func(f *fakePG) { f.slots["zoctl_slot"] = [3]string{"pgoutput", "logical", "false"} },
		"nothing in it":   func(f *fakePG) { f.pubTabs = nil },
		"physical slot":   func(f *fakePG) { f.slots["zoctl_slot"] = [3]string{"", "physical", "false"} },
		"no target table": nil,
	}
	for name, breakIt := range cases {
		src, dst := sourcePG(), newPG()
		want := findingSchema
		if breakIt != nil {
			breakIt(src)
			want = findingPrerequisite
		}
		p, err := pgSetFor(src, dst, false).Plan(context.Background())
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !p.Blocked || len(p.Findings) == 0 || p.Findings[0] != want {
			t.Errorf("%s: %+v", name, p)
		}
	}
}

func TestPostgresPlanRefusesATargetWithRows(t *testing.T) {
	src, dst := sourcePG(), newPG()
	dst.tables[`"public"."orders"`] = 5
	dst.tables[`"shop"."it's"`] = 0
	p, err := pgSetFor(src, dst, false).Plan(context.Background())
	if err != nil || !p.Blocked || p.Findings[0] != findingNotEmpty {
		t.Fatalf("%+v %v", p, err)
	}
}

func TestPostgresReplicates(t *testing.T) {
	src, dst := sourcePG(), newPG()
	src.noID = 1
	set := pgSetFor(src, dst, true)
	s := &Sync{move: Move{Limits: Limits{MaxObjects: 1, MaxBytes: 1, Hours: 1}}, sets: []dataset{set}, now: time.Now}
	plan, err := s.Plan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	d := plan.Datasets[0]
	if d.Blocked || d.Tables != 2 || d.Copy != 2 || !contains(d.Findings, findingTablesOnly) || !contains(d.Findings, findingNoIdentity) {
		t.Fatalf("plan %+v", d)
	}
	w, err := transfer.OpenWorkspace(filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	rounds := 0
	st, err := s.Run(context.Background(), w, plan, plan.Digest, RunOptions{Every: time.Second, Rounds: 2, Report: func(st Status) {
		if st.Stage != "running" {
			return
		}
		// The first round sees the copy still going, then it catches up.
		rounds++
		dst.mu.Lock()
		if rounds == 1 {
			dst.ready = 1
			src.lag = 4096
		} else {
			dst.ready = 2
			src.lag = 0
			dst.tables[`"public"."orders"`] = 3
			dst.tables[`"shop"."it's"`] = 1
		}
		dst.mu.Unlock()
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !st.Verified() || st.Stage != "verified" || st.Datasets[0].LagBytes != 0 {
		t.Fatalf("%+v", st)
	}
	// The source got reads and nothing else.
	for _, q := range src.queries {
		if !strings.HasPrefix(q, "SELECT ") {
			t.Fatalf("source got %q", q)
		}
	}
	if len(src.execs) != 0 {
		t.Fatalf("source execs %v", src.execs)
	}
	// The target got the tables, then a subscription on the customer's slot,
	// which it neither makes nor drops, and was disabled at the end.
	ddl := strings.Join(dst.execs, "\n")
	for _, want := range []string{
		`CREATE SCHEMA IF NOT EXISTS "shop"`,
		`CREATE TABLE "public"."orders" (` + "\n" + `  "id" bigint NOT NULL,` + "\n" + `  "note" character varying(80),` + "\n" + `  PRIMARY KEY ("id")`,
		`CREATE TABLE "shop"."it's" (` + "\n" + `  "weird ""col""" numeric(10,2)`,
		`PUBLICATION "zoctl_pub" WITH (slot_name = 'zoctl_slot', create_slot = false, copy_data = true, enabled = true)`,
		`ALTER SUBSCRIPTION "zoctl_orders" DISABLE`,
	} {
		if !strings.Contains(ddl, want) {
			t.Fatalf("missing %q in\n%s", want, ddl)
		}
	}
	if dst.conninfo != `'postgres://repl:pa''ss@old.example.test/shop'` || dst.subs["zoctl_orders"] {
		t.Fatalf("conninfo %s subs %v", dst.conninfo, dst.subs)
	}

	// A new approval enables the same subscription again instead of
	// copying twice.
	again, err := s.Plan(context.Background())
	if err != nil || again.Digest == plan.Digest || again.Datasets[0].Blocked {
		t.Fatalf("%+v %v", again, err)
	}
	dst.execs = nil
	if _, err := s.Run(context.Background(), w, again, again.Digest, RunOptions{Every: time.Second, Rounds: 1}); err != nil {
		t.Fatal(err)
	}
	if dst.execs[0] != `ALTER SUBSCRIPTION "zoctl_orders" ENABLE` || strings.Contains(strings.Join(dst.execs, ""), "CREATE") {
		t.Fatalf("second run %v", dst.execs)
	}
}

func TestPostgresCheckCountsRows(t *testing.T) {
	src, dst := sourcePG(), newPG()
	set := pgSetFor(src, dst, false)
	dst.tables[`"public"."orders"`] = 3
	dst.tables[`"shop"."it's"`] = 0
	set.tables = src.pubTabs
	dst.ready = 2
	st, err := set.Check(context.Background())
	if err != nil || st.Verified || st.Extra != 1 {
		t.Fatalf("%+v %v", st, err)
	}
	dst.tables[`"shop"."it's"`] = 1
	if st, _ := set.Check(context.Background()); !st.Verified {
		t.Fatalf("%+v", st)
	}
	src.lag = 1
	if st, _ := set.Check(context.Background()); st.Verified || st.LagSeconds != 7 {
		t.Fatalf("lagging %+v", st)
	}
}

// A server's WAL goes on past a stopped subscription with nothing of the
// move's in it. A check asks the slot, and takes the distance for lag only
// when the slot has a change or can't be asked.
func TestPostgresCheckTellsTheMovesWALFromTheServers(t *testing.T) {
	fresh := func() (*fakePG, *fakePG, *pgSet) {
		src, dst := sourcePG(), newPG()
		dst.tables[`"public"."orders"`], dst.tables[`"shop"."it's"`] = 3, 1
		dst.subs["zoctl_orders"] = false
		dst.ready = 2
		src.lag, src.othersOnly = 1<<20, true
		set := pgSetFor(src, dst, false)
		set.sleep = func(context.Context) error { return nil }
		return src, dst, set
	}
	ctx := context.Background()

	src, _, set := fresh()
	if st, err := set.Check(ctx); err != nil || !st.Verified || st.LagBytes != 0 || st.LagSeconds != 0 {
		t.Fatalf("WAL that isn't the move's: %+v %v", st, err)
	}
	// A round reports the distance as it is, and doesn't ask the slot.
	peeks := src.peeks
	if st, err := set.Round(ctx, nil); err != nil || st.LagBytes != 1<<20 || src.peeks != peeks {
		t.Fatalf("round: %+v %v", st, err)
	}

	// The subscription has just stopped: the slot is free a moment later.
	src, _, set = fresh()
	src.active = 3
	if st, _ := set.Check(ctx); !st.Verified {
		t.Fatalf("after the walsender let go: %+v", st)
	}

	for name, change := range map[string]func(src, dst *fakePG){
		"a change of the move's":    func(src, _ *fakePG) { src.othersOnly = false },
		"a server before 15":        func(src, _ *fakePG) { src.version = "140012" },
		"a role that can't peek":    func(src, _ *fakePG) { src.noPeek = true },
		"a running subscription":    func(src, dst *fakePG) { src.active, dst.subs["zoctl_orders"] = 1000, true },
		"a slot something else has": func(src, _ *fakePG) { src.active = 1000 },
	} {
		src, dst, set := fresh()
		change(src, dst)
		if st, err := set.Check(ctx); err != nil || st.Verified || st.LagBytes != 1<<20 || st.LagSeconds == 0 {
			t.Errorf("%s: %+v %v", name, st, err)
		}
	}

	// The first copy needs a slot of its own on the source.
	src, dst, set := fresh()
	delete(dst.subs, "zoctl_orders")
	dst.tables[`"public"."orders"`], dst.tables[`"shop"."it's"`] = 0, 0
	src.noFreeSlot = true
	if p, err := set.Plan(ctx); err != nil || !p.Blocked || !contains(p.Findings, findingPrerequisite) {
		t.Fatalf("no slot to copy through: %+v %v", p, err)
	}
	src.noFreeSlot = false
	if p, err := set.Plan(ctx); err != nil || p.Blocked {
		t.Fatalf("%+v %v", p, err)
	}
}

func TestPostgresSequences(t *testing.T) {
	src, dst := sourcePG(), newPG()
	src.seqs = [][]string{{"public", "orders_id_seq", "42"}, {"public", "gone_seq", "9"}}
	dst.setvals[`"public"."orders_id_seq"`] = 1
	n, err := pgSetFor(src, dst, false).Sequences(context.Background())
	if err != nil || n != 1 || dst.setvals[`"public"."orders_id_seq"`] != 42 {
		t.Fatalf("%d %v %v", n, err, dst.setvals)
	}
	if _, ok := dst.setvals[`"public"."gone_seq"`]; ok {
		t.Fatal("made a sequence")
	}

	// A sequence the role can't read shows with no value, like one never
	// used. The plan says so, and a cutover stops over one the target has
	// before it sets any.
	src.hidden = [][]string{{"public", "audit_id_seq"}}
	set := pgSetFor(src, dst, false)
	dst.subs["zoctl_orders"] = false
	if p, err := set.Plan(context.Background()); err != nil || p.Blocked || !contains(p.Findings, findingNoSequence) {
		t.Fatalf("plan: %+v %v", p, err)
	}
	if n, err := set.Sequences(context.Background()); err != nil || n != 1 {
		t.Fatalf("a hidden sequence the target doesn't have: %d %v", n, err)
	}
	dst.setvals[`"public"."audit_id_seq"`] = 5
	dst.setvals[`"public"."orders_id_seq"`] = 1
	if _, err := set.Sequences(context.Background()); !errors.Is(err, ErrSequence) || dst.setvals[`"public"."orders_id_seq"`] != 1 {
		t.Fatalf("%v %v", err, dst.setvals)
	}
}

func TestCreateTableRefusesOddTypes(t *testing.T) {
	src := sourcePG()
	src.columns[`"public"."orders"`] = [][]string{{"id", "int); DROP TABLE x; --", "true", "", "", ""}}
	if _, err := pgSetFor(src, newPG(), true).createTable(context.Background(), table{"public", "orders"}); !errors.Is(err, ErrPostgres) {
		t.Fatalf("accepted: %v", err)
	}
}

// copy_schema makes what the first console move had to add by hand: the
// sequences, defaults and foreign key, plus constraints and indexes, and
// the cutover then has a sequence to set.
func TestCopySchemaMakesSequencesDefaultsAndKeys(t *testing.T) {
	src, dst := sourcePG(), newPG()
	orders, other := `"public"."orders"`, `"shop"."it's"`
	src.columns[orders] = [][]string{
		{"id", "bigint", "true", "", "", "nextval('public.orders_id_seq'::regclass)"},
		{"note", "character varying(80)", "false", "", "", "'it''s new; really'::character varying"},
		{"made", "timestamp with time zone", "true", "", "", "now()"},
		{"total", "numeric(10,2)", "false", "", "s", "(price * (2)::numeric)"},
		{"bad", "integer", "false", "", "", "1); DROP TABLE x; --"},
	}
	src.owned[orders] = [][]string{{"public", "orders_id_seq", "bigint", "1", "1", "1", "9223372036854775807", "1", "false", "id", "a"}}
	src.cons[orders] = [][]string{{"note_short", "c", "CHECK ((length((note)::text) < 70))"}}
	src.indexes[orders] = [][]string{{"CREATE INDEX orders_made ON public.orders USING btree (made)"}}
	src.columns[other] = [][]string{{"n", "integer", "true", "d", "", ""}, {"order_id", "bigint", "false", "", "", ""}}
	src.owned[other] = [][]string{{"shop", "it's_n_seq", "integer", "1", "1", "1", "2147483647", "1", "false", "n", "i"}}
	src.cons[other] = [][]string{{"to_order", "f", "FOREIGN KEY (order_id) REFERENCES public.orders(id)"}}

	set := pgSetFor(src, dst, true)
	set.tables = src.pubTabs
	left, err := set.makeSchema(context.Background())
	if err != nil || left != 1 {
		t.Fatalf("left %d, %v", left, err)
	}
	if src.searchPath != `"$user", public` {
		t.Fatalf("the source's search_path stayed %q", src.searchPath)
	}
	for _, q := range src.queries {
		if !strings.HasPrefix(q, "SELECT ") {
			t.Fatalf("source got %q", q)
		}
	}
	all := strings.Join(dst.execs, "\n")
	want := []string{
		"BEGIN",
		`CREATE SCHEMA IF NOT EXISTS "public"`,
		`CREATE SEQUENCE IF NOT EXISTS "public"."orders_id_seq" AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE`,
		`CREATE TABLE "public"."orders" (`,
		`  "total" numeric(10,2) GENERATED ALWAYS AS ((price * (2)::numeric)) STORED,`,
		`  "n" integer GENERATED BY DEFAULT AS IDENTITY (SEQUENCE NAME "shop"."it's_n_seq" INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START WITH 1 CACHE 1 NO CYCLE) NOT NULL`,
		`ALTER SEQUENCE "public"."orders_id_seq" OWNED BY "public"."orders"."id"`,
		`ALTER TABLE "public"."orders" ALTER COLUMN "id" SET DEFAULT nextval('public.orders_id_seq'::regclass)`,
		`ALTER TABLE "public"."orders" ALTER COLUMN "note" SET DEFAULT 'it''s new; really'::character varying`,
		`ALTER TABLE "public"."orders" ALTER COLUMN "made" SET DEFAULT now()`,
		`ALTER TABLE "public"."orders" ADD CONSTRAINT "note_short" CHECK ((length((note)::text) < 70))`,
		"CREATE INDEX orders_made ON public.orders USING btree (made)",
		`ALTER TABLE "shop"."it's" ADD CONSTRAINT "to_order" FOREIGN KEY (order_id) REFERENCES public.orders(id)`,
		"COMMIT",
	}
	at := 0
	for _, w := range want {
		i := strings.Index(all[at:], w)
		if i < 0 {
			t.Fatalf("missing or out of order %q in\n%s", w, all)
		}
		at += i + len(w)
	}
	// The default that isn't one statement never reached the target.
	if strings.Contains(all, "DROP TABLE") {
		t.Fatalf("ran a hidden statement:\n%s", all)
	}

	// A constraint the target refuses is left out on its own; a table it
	// refuses takes everything back.
	dst = newPG()
	dst.failExec = `ALTER TABLE "shop"."it's" ADD CONSTRAINT`
	set = pgSetFor(src, dst, true)
	set.tables = src.pubTabs
	if left, err := set.makeSchema(context.Background()); err != nil || left != 2 {
		t.Fatalf("left %d, %v", left, err)
	}
	if !slices.Contains(dst.execs, "ROLLBACK TO SAVEPOINT zoctl_schema") || dst.execs[len(dst.execs)-1] != "COMMIT" {
		t.Fatalf("%v", dst.execs)
	}
	dst = newPG()
	dst.failExec = `CREATE TABLE "shop"`
	set = pgSetFor(src, dst, true)
	set.tables = src.pubTabs
	if _, err := set.makeSchema(context.Background()); !errors.Is(err, ErrPostgres) || dst.execs[len(dst.execs)-1] != "ROLLBACK" {
		t.Fatalf("%v %v", err, dst.execs)
	}
}

func TestOneStatement(t *testing.T) {
	for s, want := range map[string]bool{
		"now()":                           true,
		"'a;b'::text":                     true,
		`'it''s'::text`:                   true,
		`"we;ird" > 0`:                    true,
		"'--'::text":                      true,
		"1; DROP TABLE x":                 false,
		"1 -- x":                          false,
		"1 /* x */":                       false,
		"$$x$$":                           false,
		"'open":                           false,
		`'it''s'; DROP TABLE x; SELECT '`: false,
		"":                                false,
	} {
		if oneStatement(s) != want {
			t.Errorf("%q: want %v", s, want)
		}
	}
}

func TestOpenPostgresNeedsBothURLs(t *testing.T) {
	previous := connect
	t.Cleanup(func() { connect = previous })
	var readOnlyFlags []bool
	connect = func(_ context.Context, url string, ro bool) (targetDB, error) {
		readOnlyFlags = append(readOnlyFlags, ro)
		return newPG(), nil
	}
	d := Dataset{Name: "orders", Kind: "postgres", FromEnv: "SOURCE_DB", ToEnv: "TARGET_DB", Publication: "p", Slot: "s"}
	env := map[string]string{"SOURCE_DB": "postgres://a@old.example.test/x?sslmode=require", "TARGET_DB": "postgres://b@new.example.test/x?sslmode=verify-full"}
	ds, err := openPostgres(d, Options{Env: func(k string) string { return env[k] }})
	if err != nil || len(readOnlyFlags) != 2 || !readOnlyFlags[0] || readOnlyFlags[1] {
		t.Fatalf("%v %v", err, readOnlyFlags)
	}
	if ds.(*pgSet).sub != "zoctl_orders" {
		t.Fatal(ds.(*pgSet).sub)
	}
	env["TARGET_DB"] = env["SOURCE_DB"]
	if _, err := openPostgres(d, Options{Env: func(k string) string { return env[k] }}); !errors.Is(err, ErrMove) {
		t.Fatalf("same database on both sides: %v", err)
	}
}

// A URL that allows a connection without TLS is refused before anything is
// dialed, on either side and for the way back: the default falls back to
// plaintext, and the URL carries a password.
func TestADatabaseURLHasToAskForTLS(t *testing.T) {
	previous := connect
	t.Cleanup(func() { connect = previous })
	dialed := 0
	connect = func(context.Context, string, bool) (targetDB, error) {
		dialed++
		return newPG(), nil
	}
	const good = "?sslmode=require"
	d := Dataset{Name: "orders", Kind: "postgres", FromEnv: "SOURCE_DB", ToEnv: "TARGET_DB", Publication: "p", Slot: "s",
		BackEnv: "STANDBY_DB", BackFromEnv: "STANDBY_FROM"}
	for _, plain := range []string{"", "?sslmode=prefer", "?sslmode=allow", "?sslmode=disable"} {
		for _, side := range []string{"SOURCE_DB", "TARGET_DB", "STANDBY_DB", "STANDBY_FROM"} {
			env := map[string]string{"SOURCE_DB": "postgres://a:pw@old.example.test/x" + good, "TARGET_DB": "postgres://b:pw@new.example.test/x" + good,
				"STANDBY_DB": "postgres://c:pw@old.example.test/x" + good, "STANDBY_FROM": "postgres://d:pw@new.example.test/x" + good}
			env[side] = strings.TrimSuffix(env[side], good) + plain
			o := Options{Env: func(k string) string { return env[k] }}
			if side == "SOURCE_DB" || side == "TARGET_DB" {
				if _, err := openPostgres(d, o); !errors.Is(err, ErrPlainDatabase) {
					t.Errorf("%s with %q: %v", side, plain, err)
				}
			}
			if side != "SOURCE_DB" {
				if _, err := openPostgresBack(d, o); !errors.Is(err, ErrPlainDatabase) {
					t.Errorf("the way back, %s with %q: %v", side, plain, err)
				}
			}
		}
	}
	if dialed != 0 {
		t.Fatalf("%d connections were opened for a URL that was refused", dialed)
	}
	for _, mode := range []string{"require", "verify-ca", "verify-full"} {
		if _, err := pgConfig("postgres://a:pw@old.example.test/x?sslmode=" + mode); err != nil {
			t.Errorf("sslmode=%s: %v", mode, err)
		}
	}
	// The real opener holds the same rule, and says nothing of the URL.
	_, err := previous(context.Background(), "postgres://a:hunter2@old.example.test/x", true)
	if !errors.Is(err, ErrPlainDatabase) || strings.Contains(err.Error(), "hunter2") || strings.Contains(err.Error(), "old.example") {
		t.Fatalf("the opener on a plain URL: %v", err)
	}
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// The statement that makes a subscription holds the other side's password, so
// the session asks the server to log no statement before it and puts the
// settings back after. A role that may not set them still gets its
// subscription.
func TestASubscriptionIsMadeWithStatementLoggingOff(t *testing.T) {
	for _, refused := range []bool{false, true} {
		src, dst := sourcePG(), newPG()
		set := pgSetFor(src, dst, true)
		ctx := context.Background()
		plan, err := set.Plan(ctx)
		if err != nil || plan.Blocked {
			t.Fatalf("plan: %+v %v", plan, err)
		}
		if refused {
			dst.failExec = "SET log_"
		}
		if _, err := set.Start(ctx, plan, nil); err != nil {
			t.Fatalf("start, settings refused %v: %v", refused, err)
		}
		at := slices.IndexFunc(dst.execs, func(sql string) bool { return strings.HasPrefix(sql, "CREATE SUBSCRIPTION ") })
		if at < len(quiet) || !slices.Equal(dst.execs[at-len(quiet):at], quiet) {
			t.Fatalf("before the subscription, settings refused %v: %q", refused, dst.execs[:max(at, 0)])
		}
		if !slices.Equal(dst.execs[at+1:at+1+len(loud)], loud) {
			t.Fatalf("after the subscription: %q", dst.execs[at+1:])
		}
		if !dst.subs["zoctl_orders"] {
			t.Fatalf("no subscription, settings refused %v", refused)
		}
		for _, want := range []string{"log_statement = 'none'", "log_min_error_statement = 'panic'", "log_min_duration_statement = -1"} {
			if !strings.Contains(strings.Join(quiet, ";"), want) {
				t.Fatalf("the session doesn't set %s", want)
			}
		}
	}
}

// A subscription with the move's name on another slot or publication isn't
// the move's to enable: the plan blocks, and nothing is switched on.
func TestASubscriptionOnAnotherSlotIsNotResumed(t *testing.T) {
	src, dst := sourcePG(), newPG()
	dst.tables[`"public"."orders"`], dst.tables[`"shop"."it's"`] = 3, 1
	dst.subs["zoctl_orders"] = false
	set := pgSetFor(src, dst, false)
	ctx := context.Background()
	plan, err := set.Plan(ctx)
	if err != nil || plan.Blocked {
		t.Fatalf("the move's own subscription: %+v %v", plan, err)
	}
	if len(dst.asked) != 1 || dst.asked[0] != [2]string{"zoctl_slot", "zoctl_pub"} {
		t.Fatalf("held to %q", dst.asked)
	}
	dst.theirs = map[string]bool{"zoctl_orders": true}
	plan, err = set.Plan(ctx)
	if err != nil || !plan.Blocked || !contains(plan.Findings, findingPrerequisite) {
		t.Fatalf("another slot's subscription: %+v %v", plan, err)
	}
	if strings.Contains(strings.Join(dst.execs, "\n"), "ENABLE") {
		t.Fatalf("it was enabled: %q", dst.execs)
	}
}

// Both URLs naming one database is a mistake a move file can't show, since
// they come from the environment. With its tables empty nothing else stops
// it, and the subscription would be made on the source.
func TestPostgresPlanRefusesTheSourceAsItsOwnTarget(t *testing.T) {
	src := sourcePG()
	for k := range src.tables {
		src.tables[k] = 0
	}
	p, err := pgSetFor(src, src, false).Plan(context.Background())
	if err != nil || !p.Blocked || !slices.Contains(p.Findings, findingSameDatabase) {
		t.Fatalf("the source as its own target: %+v %v", p, err)
	}
	// A role that may not ask is not a reason to refuse a move.
	src, dst := sourcePG(), newPG()
	for k := range src.tables {
		dst.tables[k] = 0
	}
	dst.noSystem = true
	if p, err := pgSetFor(src, dst, false).Plan(context.Background()); err != nil || p.Blocked {
		t.Fatalf("a target that won't say: %+v %v", p, err)
	}
	if !slices.Contains(FindingCodes(), findingSameDatabase) {
		t.Fatal("the code isn't one a runner may report")
	}
}

// dropped is a connection the server or something on the way closed: every
// call on it fails.
type dropped struct{ *fakePG }

func (dropped) Rows(context.Context, string, ...any) ([][]string, error) { return nil, ErrPostgres }
func (dropped) Exec(context.Context, string) error                       { return ErrPostgres }

// A sync holds one connection to the target for hours. If it drops, the
// subscription goes on writing there on its own, so Stop dials again: an
// approval that ended has to end the writes.
func TestStopDialsAgainWhenTheConnectionDropped(t *testing.T) {
	ctx := context.Background()
	src, dst := sourcePG(), newPG()
	dst.subs["zoctl_orders"] = true
	set := pgSetFor(src, dst, false)
	set.dst = dropped{dst}
	dials := 0
	set.redial = func(context.Context) (targetDB, error) { dials++; return dst, nil }
	if err := set.Stop(ctx); err != nil {
		t.Fatalf("stop on a dropped connection: %v", err)
	}
	if dst.subs["zoctl_orders"] || dials != 1 {
		t.Fatalf("still enabled %v after %d dials", dst.subs["zoctl_orders"], dials)
	}
	// Once only, and a target that can't be reached is an error, not a
	// subscription taken as stopped.
	dst.subs["zoctl_orders"] = true
	set.dst, dials = dropped{dst}, 0
	set.redial = func(context.Context) (targetDB, error) { dials++; return dropped{dst}, nil }
	if err := set.Stop(ctx); err == nil || dials != 1 {
		t.Fatalf("a target that stays down: %v after %d dials", err, dials)
	}
	set.redial = func(context.Context) (targetDB, error) { return nil, ErrPostgres }
	if err := set.Stop(ctx); err == nil {
		t.Fatal("a target that can't be dialed read as stopped")
	}
	// A connection that works isn't replaced.
	set.dst, dials = dst, 0
	set.redial = func(context.Context) (targetDB, error) { dials++; return dst, nil }
	if err := set.Stop(ctx); err != nil || dials != 0 || dst.subs["zoctl_orders"] {
		t.Fatalf("a live connection: %v, %d dials", err, dials)
	}
}

// A subscription keeps the tables it had when it was made. One the
// publication has since dropped is still ready there, and must not stand in
// for a table added since, which the subscription doesn't copy at all.
func TestPostgresCheckCountsOnlyThePublishedTables(t *testing.T) {
	src, dst := sourcePG(), newPG()
	// Empty on both sides, so the row counts agree and say nothing.
	src.tables[`"shop"."it's"`] = 0
	dst.tables[`"public"."orders"`], dst.tables[`"shop"."it's"`] = 3, 0
	dst.subs["zoctl_orders"] = true
	dst.rels = []table{{"public", "orders"}, {"public", "dropped_since"}}
	set := pgSetFor(src, dst, false)
	set.tables = src.pubTabs
	st, err := set.Check(context.Background())
	if err != nil || st.Verified || st.Pending != 1 {
		t.Fatalf("a table the subscription never took: %+v %v", st, err)
	}
	dst.rels = src.pubTabs
	if st, err := set.Check(context.Background()); err != nil || !st.Verified {
		t.Fatalf("both tables ready: %+v %v", st, err)
	}
}

// Azure's B2s showed this one (20 November 2026): the subscription ran,
// every table sat at i with no error, and a round said only that tables were
// still copying. Now it says the copy never started, but only once the
// subscription has had time to start one, and not while one copies.
func TestPostgresSaysWhenNoTableStartedCopying(t *testing.T) {
	src, dst := sourcePG(), newPG()
	dst.subs["zoctl_orders"] = true
	dst.rels = []table{}
	dst.waiting = src.pubTabs
	set := pgSetFor(src, dst, false)
	set.tables = src.pubTabs
	st, err := set.Round(context.Background(), nil)
	if err != nil || st.Pending != 2 || slices.Contains(st.Findings, findingCopyNotStarted) {
		t.Fatalf("a subscription that only just started: %+v %v", st, err)
	}
	dst.stuck = true
	st, err = set.Round(context.Background(), nil)
	if err != nil || st.Pending != 2 || !slices.Contains(st.Findings, findingCopyNotStarted) {
		t.Fatalf("no table started: %+v %v", st, err)
	}
	// One table copying or copied: the copy started, whatever the other waits on.
	dst.rels, dst.waiting = src.pubTabs[:1], src.pubTabs[1:]
	if st, err := set.Round(context.Background(), nil); err != nil || slices.Contains(st.Findings, findingCopyNotStarted) {
		t.Fatalf("one table copied: %+v %v", st, err)
	}
}

// A real Postgres 17 showed this one (18 November 2026): a table published
// after the sync started isn't on the target, and counting its rows there is
// an error, so the check said nothing but that the command failed.
func TestPostgresCheckSaysATableIsMissing(t *testing.T) {
	src, dst := sourcePG(), newPG()
	dst.tables[`"public"."orders"`] = 3
	dst.subs["zoctl_orders"] = true
	dst.rels = []table{{"public", "orders"}}
	set := pgSetFor(src, dst, false)
	set.tables = src.pubTabs
	st, err := set.Check(context.Background())
	if err != nil {
		t.Fatalf("a published table the target lacks is an answer, not an error: %v", err)
	}
	if st.Verified || st.Pending != 1 || st.Extra != 1 || !slices.Contains(st.Findings, findingSchema) {
		t.Fatalf("a published table the target lacks: %+v", st)
	}
}
