package datasync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zoctl/cli/internal/transfer"
)

// fakeMy is one MySQL server and database in memory: its tables, rows and
// binary log. A test uses one as the source and another as the target.
type fakeMy struct {
	mu      sync.Mutex
	id      string
	vars    map[string]string
	tables  []myTable
	rows    map[string]map[string][]any // table -> key -> row
	pos     myPos
	logs    []myLog
	events  []myEvent
	more    chan struct{}
	noPos   bool
	from    *fakeMy // a target makes its tables as this source has them
	onSnap  func()  // runs as a snapshot opens, before it reads
	commits int
}

func newFakeMy(id string) *fakeMy {
	return &fakeMy{
		id:   id,
		vars: map[string]string{"log_bin": "ON", "binlog_format": "ROW", "binlog_row_image": "FULL"},
		rows: map[string]map[string][]any{},
		pos:  myPos{File: "bin.000001", Pos: 4},
		logs: []myLog{{File: "bin.000001", Size: 4}},
		more: make(chan struct{}),
	}
}

func myItems() myTable {
	return myTable{Name: "items", Columns: []myColumn{
		{Name: "id", Type: "int"},
		{Name: "name", Type: "varchar", Charset: "utf8mb4"},
	}, Key: []int{0}}
}

func (f *fakeMy) table(name string) *myTable {
	for i := range f.tables {
		if f.tables[i].Name == name {
			return &f.tables[i]
		}
	}
	return nil
}

func rowKey(t myTable, r []any) string {
	var parts []string
	for _, i := range t.Key {
		parts = append(parts, cellString(r[i]))
	}
	return strings.Join(parts, "\x00")
}

// put writes rows as they are, by key.
func (f *fakeMy) put(t myTable, rows ...[]any) {
	if f.rows[t.Name] == nil {
		f.rows[t.Name] = map[string][]any{}
	}
	for _, r := range rows {
		f.rows[t.Name][rowKey(t, r)] = r
	}
}

// write is a transaction on the source: the rows change, and the log has
// them.
func (f *fakeMy) write(schema, table string, op myOp, rows ...[]any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if t := f.table(table); t != nil && schema == "shop" {
		for i, r := range rows {
			switch {
			case op == opInsert:
				f.put(*t, r)
			case op == opDelete:
				delete(f.rows[t.Name], rowKey(*t, r))
			case i%2 == 0:
				delete(f.rows[t.Name], rowKey(*t, r))
			default:
				f.put(*t, r)
			}
		}
	}
	cols := 2
	if len(rows) > 0 {
		cols = len(rows[0])
	}
	step := func(e myEvent) {
		f.pos.Pos += 100
		e.Next, e.Time = f.pos, time.Now()
		f.events = append(f.events, e)
	}
	step(myEvent{Kind: evBegin})
	step(myEvent{Kind: evRows, Op: op, Schema: schema, Table: table, Columns: cols, Rows: rows})
	step(myEvent{Kind: evCommit})
	f.logs[len(f.logs)-1].Size = int64(f.pos.Pos)
	close(f.more)
	f.more = make(chan struct{})
}

func (f *fakeMy) Identity(context.Context) (string, error) { return f.id, nil }

func (f *fakeMy) Vars(_ context.Context, names ...string) (map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]string{}
	for _, n := range names {
		if v, ok := f.vars[n]; ok {
			out[n] = v
		}
	}
	return out, nil
}

func (f *fakeMy) Tables(context.Context) ([]myTable, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.tables), nil
}

func (f *fakeMy) Empty(_ context.Context, t myTable) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.rows[t.Name]) == 0, nil
}

func (f *fakeMy) Sum(_ context.Context, t myTable) (mySum, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var all []string
	for _, r := range f.rows[t.Name] {
		var cells []string
		for _, c := range r {
			cells = append(cells, cellString(c))
		}
		all = append(all, strings.Join(cells, "\x1f"))
	}
	slices.Sort(all)
	return mySum{Rows: int64(len(all)), Hash: strings.Join(all, "\n")}, nil
}

func (f *fakeMy) Close() {}

func (f *fakeMy) Position(context.Context) (myPos, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.noPos {
		return myPos{}, ErrMySQL
	}
	return f.pos, nil
}

func (f *fakeMy) Logs(context.Context) ([]myLog, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.logs), nil
}

func (f *fakeMy) CreateTable(_ context.Context, t myTable) (string, error) {
	return "CREATE TABLE " + quoteIdent(t.Name), nil
}

func (f *fakeMy) Create(_ context.Context, stmt string) error {
	name := strings.Trim(strings.TrimPrefix(stmt, "CREATE TABLE "), "`")
	t := f.from.table(name)
	if t == nil {
		return ErrMySQL
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tables = append(f.tables, *t)
	return nil
}

func (f *fakeMy) Insert(_ context.Context, t myTable, rows [][]any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, r := range rows {
		if _, ok := f.rows[t.Name][rowKey(t, r)]; ok {
			return ErrMySQL // a duplicate key
		}
		f.put(t, r)
	}
	return nil
}

type fakeMySnap struct{ rows map[string][][]any }

func (f *fakeMy) Snapshot(context.Context) (mySnapshot, error) {
	if f.onSnap != nil {
		f.onSnap()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	s := fakeMySnap{rows: map[string][][]any{}}
	for name, rows := range f.rows {
		for _, r := range rows {
			s.rows[name] = append(s.rows[name], r)
		}
	}
	return s, nil
}

type fakeMyRows struct{ rows [][]any }

func (r *fakeMyRows) Next() ([]any, error) {
	if len(r.rows) == 0 {
		return nil, io.EOF
	}
	row := r.rows[0]
	r.rows = r.rows[1:]
	return row, nil
}
func (r *fakeMyRows) Close() {}

func (s fakeMySnap) Rows(_ context.Context, t myTable) (myRows, error) {
	return &fakeMyRows{rows: s.rows[t.Name]}, nil
}
func (s fakeMySnap) Close() {}

type fakeMyLog struct {
	f    *fakeMy
	next int
}

func (f *fakeMy) Binlog(_ context.Context, from myPos) (myBinlog, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	known := slices.ContainsFunc(f.logs, func(l myLog) bool { return l.File == from.File })
	if !known {
		return nil, ErrMySQL
	}
	i := 0
	for i < len(f.events) && f.events[i].Next.Pos <= from.Pos {
		i++
	}
	return &fakeMyLog{f: f, next: i}, nil
}

func (l *fakeMyLog) Next(ctx context.Context) (myEvent, error) {
	for {
		l.f.mu.Lock()
		if l.next < len(l.f.events) {
			e := l.f.events[l.next]
			l.next++
			l.f.mu.Unlock()
			return e, nil
		}
		more := l.f.more
		l.f.mu.Unlock()
		select {
		case <-ctx.Done():
			return myEvent{}, ctx.Err()
		case <-more:
		}
	}
}
func (l *fakeMyLog) Close() {}

type fakeMyTx struct {
	f   *fakeMy
	ops []func()
}

func (f *fakeMy) Begin(context.Context) (myTx, error) { return &fakeMyTx{f: f}, nil }

func (t *fakeMyTx) Replace(_ context.Context, tb myTable, rows [][]any) error {
	t.ops = append(t.ops, func() { t.f.put(tb, rows...) })
	return nil
}

func (t *fakeMyTx) Delete(_ context.Context, tb myTable, rows [][]any) error {
	t.ops = append(t.ops, func() {
		for _, r := range rows {
			delete(t.f.rows[tb.Name], rowKey(tb, r))
		}
	})
	return nil
}

func (t *fakeMyTx) Commit(context.Context) error {
	t.f.mu.Lock()
	defer t.f.mu.Unlock()
	for _, op := range t.ops {
		op()
	}
	t.f.commits++
	return nil
}
func (t *fakeMyTx) Rollback() {}

// testWorkspace is a private workspace for one test.
func testWorkspace(t *testing.T) *transfer.Workspace {
	t.Helper()
	w, err := transfer.OpenWorkspace(filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	return w
}

func row(id int64, name string) []any { return []any{id, []byte(name)} }

func mySet(t *testing.T, src, dst *fakeMy, w *transfer.Workspace) *mysqlSet {
	t.Helper()
	return &mysqlSet{name: "shop", db: "shop", src: src, dst: dst, w: w, copySchema: true, now: time.Now}
}

// caughtUp runs rounds until the target is at the source's position.
func caughtUp(t *testing.T, s *mysqlSet) DatasetStatus {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		st, err := s.Round(context.Background(), nil)
		if err != nil {
			t.Fatalf("round: %v", err)
		}
		if st.LagBytes == 0 {
			return st
		}
		if time.Now().After(deadline) {
			t.Fatalf("never caught up: %+v", st)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestMySQLCopyThenFollow(t *testing.T) {
	ctx := context.Background()
	src, dst := newFakeMy("a"), newFakeMy("b")
	dst.from = src
	src.tables = []myTable{myItems()}
	src.put(myItems(), row(1, "one"), row(2, "two"))
	// A write between reading the log's place and the copy's read: the
	// copy has it and the log has it again.
	src.onSnap = func() {
		src.onSnap = nil
		src.write("shop", "items", opInsert, row(3, "three"))
	}
	w := testWorkspace(t)
	s := mySet(t, src, dst, w)
	p, err := s.Plan(ctx)
	if err != nil || p.Blocked || p.Tables != 1 || p.Copy != 1 || p.Digest == "" {
		t.Fatalf("plan %+v %v", p, err)
	}
	st, err := s.Start(ctx, p, nil)
	if err != nil || st.Copied != 3 {
		t.Fatalf("start %+v %v", st, err)
	}
	src.write("shop", "items", opUpdate, row(1, "one"), row(1, "uno"))
	src.write("shop", "items", opUpdate, row(2, "two"), row(20, "two"))
	src.write("shop", "items", opDelete, row(3, "three"))
	src.write("other", "items", opInsert, row(9, "not this database"))
	caughtUp(t, s)
	c, err := s.Check(ctx)
	if err != nil || !c.Verified || c.Extra != 0 {
		t.Fatalf("check %+v %v", c, err)
	}
	if got := len(dst.rows["items"]); got != 2 || cellString(dst.rows["items"]["1"][1]) != "uno" || dst.rows["items"]["20"] == nil {
		t.Fatalf("target rows %v", dst.rows["items"])
	}
	if err := s.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	var saved myState
	if err := w.Load(s.stateName(), &saved); err != nil || saved.myPos != src.pos || saved.Source != "a" {
		t.Fatalf("saved %+v %v", saved, err)
	}

	// The next approval carries on from there, over the rows already on
	// the target.
	src.write("shop", "items", opInsert, row(4, "four"))
	s2 := mySet(t, src, dst, w)
	p2, err := s2.Plan(ctx)
	if err != nil || p2.Blocked || p2.Extra != 0 || p2.work != "resume" {
		t.Fatalf("second plan %+v %v", p2, err)
	}
	if st, err := s2.Start(ctx, p2, nil); err != nil || st.Copied != 0 {
		t.Fatalf("second start %+v %v", st, err)
	}
	caughtUp(t, s2)
	if c, err := s2.Check(ctx); err != nil || !c.Verified {
		t.Fatalf("second check %+v %v", c, err)
	}
	s2.Stop(ctx)
}

func TestMySQLPlanBlocks(t *testing.T) {
	ctx := context.Background()
	src, dst := newFakeMy("same"), newFakeMy("same")
	src.vars["binlog_format"] = "STATEMENT"
	noKey := myItems()
	noKey.Name, noKey.Key = "log", nil
	src.tables = []myTable{myItems(), noKey}
	dst.tables = []myTable{myItems(), noKey}
	dst.put(myItems(), row(1, "already"))
	w := testWorkspace(t)
	p, err := mySet(t, src, dst, w).Plan(ctx)
	if err != nil || !p.Blocked {
		t.Fatalf("plan %+v %v", p, err)
	}
	for _, want := range []string{findingPrerequisite, findingNoIdentity, findingNotEmpty, findingSameDatabase} {
		if !slices.Contains(p.Findings, want) {
			t.Errorf("findings %v, want %s", p.Findings, want)
		}
	}
	// Without REPLICATION CLIENT the log's place can't be read.
	src2, dst2 := newFakeMy("a"), newFakeMy("b")
	src2.tables, src2.noPos = []myTable{myItems()}, true
	dst2.from = src2
	if p, err := mySet(t, src2, dst2, w).Plan(ctx); err != nil || !slices.Contains(p.Findings, findingPrerequisite) {
		t.Fatalf("plan without the position %+v %v", p, err)
	}
}

func TestMySQLPositionLost(t *testing.T) {
	ctx := context.Background()
	src, dst := newFakeMy("a"), newFakeMy("b")
	src.tables, dst.tables = []myTable{myItems()}, []myTable{myItems()}
	dst.put(myItems(), row(1, "one"))
	w := testWorkspace(t)
	s := mySet(t, src, dst, w)
	// The source has purged the log the last sync reached.
	if err := s.save(myPos{File: "bin.000000", Pos: 900}); err != nil {
		t.Fatal(err)
	}
	p, err := s.Plan(ctx)
	if err != nil || !p.Blocked || !slices.Contains(p.Findings, findingPositionLost) {
		t.Fatalf("plan %+v %v", p, err)
	}
	// Nor is another server's place in its log any use here.
	if err := s.save(myPos{File: "bin.000001", Pos: 4}); err != nil {
		t.Fatal(err)
	}
	s.srcID = ""
	s.src = &fakeMyOther{src}
	if p, err := s.Plan(ctx); err != nil || !slices.Contains(p.Findings, findingPositionLost) {
		t.Fatalf("plan for another source %+v %v", p, err)
	}
}

type fakeMyOther struct{ *fakeMy }

func (fakeMyOther) Identity(context.Context) (string, error) { return "c", nil }

func TestMySQLSchemaChangeStops(t *testing.T) {
	ctx := context.Background()
	src, dst := newFakeMy("a"), newFakeMy("b")
	dst.from = src
	src.tables = []myTable{myItems()}
	w := testWorkspace(t)
	s := mySet(t, src, dst, w)
	p, err := s.Plan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Start(ctx, p, nil); err != nil {
		t.Fatal(err)
	}
	src.write("shop", "made_since", opInsert, row(1, "x"))
	deadline := time.Now().Add(5 * time.Second)
	for {
		st, err := s.Round(ctx, nil)
		if errors.Is(err, errSchemaChanged) {
			if !slices.Contains(st.Findings, findingSchemaChanged) {
				t.Fatalf("round findings %v", st.Findings)
			}
			break
		}
		if err != nil || time.Now().After(deadline) {
			t.Fatalf("round %+v %v", st, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if retryable(errSchemaChanged) {
		t.Fatal("a changed table can't be made up by another round")
	}
	s.Stop(ctx)
	c, err := s.Check(ctx)
	if err != nil || c.Verified || !slices.Contains(c.Findings, findingSchemaChanged) {
		t.Fatalf("check %+v %v", c, err)
	}
}

func TestMySQLStatements(t *testing.T) {
	tb := myTable{Name: "o`dd", Columns: []myColumn{
		{Name: "id", Type: "int", Unsigned: true},
		{Name: "body", Type: "text", Charset: "latin1"},
		{Name: "doc", Type: "json"},
		{Name: "twice", Type: "int", Generated: true},
		{Name: "small", Type: "mediumint", Unsigned: true},
	}, Key: []int{0}}
	stmt, args, err := rowsSQL("REPLACE", tb, [][]any{{int32(-1), []byte("caf\xe9"), []byte(`{"a":1}`), int64(7), int32(-1)}})
	if err != nil {
		t.Fatal(err)
	}
	want := "REPLACE INTO `o``dd` (`id`, `body`, `doc`, `small`) VALUES (?, CONVERT(? USING latin1), ?, ?)"
	if stmt != want {
		t.Fatalf("statement\n%s\nwant\n%s", stmt, want)
	}
	if fmt.Sprint(args) != fmt.Sprint([]any{uint64(4294967295), []byte("caf\xe9"), `{"a":1}`, uint64(16777215)}) {
		t.Fatalf("args %v", args)
	}
	stmt, args, err = deleteSQL(tb, [][]any{{int32(5), nil, nil, nil, nil}, {int32(6), nil, nil, nil, nil}})
	if err != nil || stmt != "DELETE FROM `o``dd` WHERE (`id` = ?) OR (`id` = ?)" || len(args) != 2 {
		t.Fatalf("delete %q %v %v", stmt, args, err)
	}
	if _, _, err := rowsSQL("INSERT", tb, [][]any{{1}}); err == nil {
		t.Fatal("a row of the wrong width was written")
	}
	if _, _, err := myArg(myColumn{}, struct{}{}); err == nil {
		t.Fatal("a value of no known type was written")
	}
	sum := mySumSQL(tb)
	if !strings.Contains(sum, "CAST(`doc` AS CHAR)") || !strings.Contains(sum, "CAST(`body` AS BINARY)") || !strings.Contains(sum, "BIT_XOR") {
		t.Fatalf("sum %s", sum)
	}
}

func TestMySQLURL(t *testing.T) {
	u, err := parseMyURL("mysql://mover:secret@db.example.com:3307/shop?tls=true")
	if err != nil || u.port != 3307 || u.db != "shop" || u.user != "mover" || u.tls == nil || u.tls.InsecureSkipVerify {
		t.Fatalf("url %+v %v", u, err)
	}
	if u, err := parseMyURL("mysql://mover@db.example.com/shop?tls=skip-verify"); err != nil || !u.tls.InsecureSkipVerify || u.port != 3306 {
		t.Fatalf("skip-verify %+v %v", u, err)
	}
	for _, bad := range []string{
		"mysql://mover:secret@db.example.com/shop",
		"mysql://mover:secret@db.example.com/shop?tls=false",
		"mysql://mover:secret@db.example.com/shop?tls=true&allowCleartextPasswords=1",
		"mysql://mover:secret@db.example.com/?tls=true",
		"mysql://db.example.com/shop?tls=true",
		"postgres://mover:secret@db.example.com/shop?tls=true",
	} {
		if _, err := parseMyURL(bad); !errors.Is(err, ErrMove) {
			t.Errorf("%s: %v", bad, err)
		}
	}
}
