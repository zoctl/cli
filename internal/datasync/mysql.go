package datasync

// A mysql dataset is a copy and a binary-log stream run by zoctl itself:
// MySQL has no subscription a managed target lets a client make, so the CLI
// is the replica. Plan reads both sides; Start copies every table from one
// consistent read of the source, then reads the source's binary log from
// just before that read and writes each change to the target, a source
// transaction to a target transaction. Every change is written as the row
// it left, by its primary key, so going over a stretch of the log twice
// ends where once does, and the copy and the log meet without a lock.
//
// zoctl's connection to the source only reads: a read-only session and the
// replication stream. Where the stream has reached is kept in the
// workspace, so the next approval carries on from there.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/zoctl/cli/internal/transfer"
)

// ErrMySQL means a MySQL step failed. The server's message isn't kept: it
// can name tables and values.
var ErrMySQL = errors.New("datasync: a mysql step failed")

// errSchemaChanged ends a stream that met a table it can't write: one made
// since the plan, or whose columns changed. Another round can't fix that.
var errSchemaChanged = errors.New("datasync: a table changed on the source")

// Finding codes only a log-reading dataset reports.
const (
	// findingPositionLost: the target has rows, and the place in the
	// source's log the last sync reached is gone, so the changes since
	// can't be read. Emptying the target starts again from a copy.
	findingPositionLost = "binlog_position_lost"
	// findingSchemaChanged: a table was made, or its columns changed, on
	// the source while it synced.
	findingSchemaChanged = "source_schema_changed"
)

// myPos is a place in the source's binary log.
type myPos struct {
	File string `json:"file"`
	Pos  uint32 `json:"pos"`
}

// myLog is one binary log file and its size.
type myLog struct {
	File string
	Size int64
}

// myColumn is what a write needs to know about a column.
type myColumn struct {
	Name      string
	Type      string // DATA_TYPE: int, varchar, json, ...
	Charset   string // a character column's, or ""
	Unsigned  bool
	Generated bool // the server works it out; never written
}

// myTable is a base table, its columns in the order the log has them and
// its primary key.
type myTable struct {
	Name    string
	Columns []myColumn
	Key     []int
}

// mySum is a table's row count and a digest of its rows in any order.
type mySum struct {
	Rows int64
	Hash string
}

// myRows pulls rows one at a time, io.EOF at the end. A cell is nil for
// NULL or []byte as the server sent it.
type myRows interface {
	Next() ([]any, error)
	Close()
}

// myReader is what both sides are read for.
type myReader interface {
	// Identity names the server and database, hashed.
	Identity(ctx context.Context) (string, error)
	// Vars reads server variables; one the server doesn't have is absent.
	Vars(ctx context.Context, names ...string) (map[string]string, error)
	// Tables lists the database's base tables.
	Tables(ctx context.Context) ([]myTable, error)
	Empty(ctx context.Context, t myTable) (bool, error)
	Sum(ctx context.Context, t myTable) (mySum, error)
	Close()
}

// mySource is the source: only read, its log included.
type mySource interface {
	myReader
	Position(ctx context.Context) (myPos, error)
	Logs(ctx context.Context) ([]myLog, error)
	CreateTable(ctx context.Context, t myTable) (string, error)
	// Snapshot is one consistent read of every table.
	Snapshot(ctx context.Context) (mySnapshot, error)
	// Binlog streams the log from a place in it.
	Binlog(ctx context.Context, from myPos) (myBinlog, error)
}

type mySnapshot interface {
	Rows(ctx context.Context, t myTable) (myRows, error)
	Close()
}

// myTarget is the side written.
type myTarget interface {
	myReader
	// Create runs a CREATE TABLE the source gave.
	Create(ctx context.Context, stmt string) error
	Insert(ctx context.Context, t myTable, rows [][]any) error
	Begin(ctx context.Context) (myTx, error)
}

// myTx is one target transaction.
type myTx interface {
	// Replace writes each row as it is, over any row with its key.
	Replace(ctx context.Context, t myTable, rows [][]any) error
	// Delete removes the rows with these rows' keys.
	Delete(ctx context.Context, t myTable, rows [][]any) error
	Commit(ctx context.Context) error
	Rollback()
}

type myEventKind int

const (
	evRows myEventKind = iota + 1
	evBegin
	evCommit
	evOther // any other event: the log moves on past it
)

type myOp int

const (
	opInsert myOp = iota + 1
	opUpdate
	opDelete
)

// myEvent is one log event the stream acts on. Next is where the log goes
// on after it.
type myEvent struct {
	Kind    myEventKind
	Op      myOp
	Schema  string
	Table   string
	Columns int
	// Rows are the rows' images: for an update, before and after in turn.
	Rows [][]any
	Next myPos
	Time time.Time
	// DDL is a statement that changes a table of the database.
	DDL bool
}

type myBinlog interface {
	Next(ctx context.Context) (myEvent, error)
	Close()
}

// myState is what the workspace keeps of a dataset: whose log, which
// target, and how far it was written.
type myState struct {
	Source string `json:"source"`
	Target string `json:"target"`
	myPos
}

// mysqlSet is one mysql dataset.
type mysqlSet struct {
	name       string
	db         string // the source's database: log events of any other are passed over
	src        mySource
	dst        myTarget
	w          *transfer.Workspace
	copySchema bool
	// settle is the wait between reading the log's position and opening
	// the copy's read: a transaction the log has before the position is
	// committed by then, so the copy or the log has it.
	settle time.Duration
	now    func() time.Time

	tables      []myTable
	srcID       string
	dstID       string
	saved       *myState
	tablesKnown bool

	mu      sync.Mutex
	applied myPos
	at      time.Time // when the last change written was made on the source
	rows    int64     // rows written since the last round
	err     error     // why the stream ended, if it did
	cancel  context.CancelFunc
	done    chan struct{}
}

// stateName is the dataset's file in the workspace. A name can take the
// whole of a workspace name's length, so it's hashed.
func (s *mysqlSet) stateName() string { return "mysql-" + hashID(s.name) }

func (s *mysqlSet) Close() {
	s.halt()
	s.src.Close()
	s.dst.Close()
}

func (s *mysqlSet) Plan(ctx context.Context) (DatasetPlan, error) {
	plan := DatasetPlan{Kind: "mysql"}
	if s.w == nil {
		return plan, transfer.ErrWorkspace
	}
	var findings []string
	blocked := false
	block := func(code string) {
		if !slices.Contains(findings, code) {
			findings = append(findings, code)
		}
		blocked = true
	}

	var err error
	if s.srcID, err = s.src.Identity(ctx); err != nil {
		return plan, err
	}
	if s.dstID, err = s.dst.Identity(ctx); err != nil {
		return plan, err
	}
	if s.srcID == s.dstID {
		block(findingSameDatabase)
	}

	// The source: what the customer set up.
	v, err := s.src.Vars(ctx, "log_bin", "binlog_format", "binlog_row_image", "binlog_row_value_options")
	if err != nil {
		return plan, err
	}
	if v["log_bin"] != "ON" && v["log_bin"] != "1" || v["binlog_format"] != "ROW" || v["binlog_row_image"] != "FULL" || v["binlog_row_value_options"] != "" {
		block(findingPrerequisite)
	}
	// Reading the log's position takes REPLICATION CLIENT.
	if _, err := s.src.Position(ctx); err != nil {
		block(findingPrerequisite)
	}
	if s.tables, err = s.src.Tables(ctx); err != nil {
		return plan, err
	}
	s.tablesKnown = true
	if len(s.tables) == 0 {
		block(findingPrerequisite)
	}
	for _, t := range s.tables {
		// A row is found again only by its key: without one, a change
		// read twice would be a second row.
		if len(t.Key) == 0 {
			block(findingNoIdentity)
		}
	}

	// The target.
	there, err := s.dst.Tables(ctx)
	if err != nil {
		return plan, err
	}
	byName := map[string]myTable{}
	for _, t := range there {
		byName[t.Name] = t
	}
	missing, nonEmpty := 0, 0
	for _, t := range s.tables {
		d, ok := byName[t.Name]
		if !ok {
			missing++
			continue
		}
		if !sameColumns(t, d) {
			block(findingSchema)
		}
		empty, err := s.dst.Empty(ctx, d)
		if err != nil {
			return plan, err
		}
		if !empty {
			nonEmpty++
		}
	}
	if missing > 0 && !s.copySchema {
		block(findingSchema)
	}

	// Where an earlier approval got to, if it's still to be had.
	state := "new"
	s.saved = nil
	var saved myState
	switch err := s.w.Load(s.stateName(), &saved); {
	case err == nil:
		s.saved = &saved
	case !errors.Is(err, fs.ErrNotExist):
		return plan, err
	}
	resumable := false
	if s.saved != nil && s.saved.Source == s.srcID && s.saved.Target == s.dstID && missing == 0 {
		logs, err := s.src.Logs(ctx)
		if err == nil {
			resumable = slices.ContainsFunc(logs, func(l myLog) bool { return l.File == s.saved.File })
		}
	}
	switch {
	case resumable:
		state = "resume"
	case nonEmpty > 0 && s.saved != nil:
		block(findingPositionLost)
	case nonEmpty > 0:
		block(findingNotEmpty)
	}

	plan.Tables = int64(len(s.tables))
	plan.Copy = int64(missing)
	plan.Same = int64(len(s.tables) - missing)
	plan.Extra = int64(nonEmpty)
	if state == "resume" {
		// The rows there are the last approval's.
		plan.Extra = 0
	}
	plan.Findings = findings
	plan.Blocked = blocked
	parts := [][]byte{[]byte("mysql-plan-v1"), []byte(state), []byte(s.srcID), []byte(s.dstID), []byte(strconv.FormatBool(s.copySchema)),
		[]byte(strconv.Itoa(missing)), []byte(strconv.Itoa(nonEmpty))}
	if state == "resume" {
		parts = append(parts, []byte(s.saved.File), []byte(strconv.FormatUint(uint64(s.saved.Pos), 10)))
	}
	for _, t := range s.tables {
		parts = append(parts, []byte(t.Name))
	}
	plan.Digest = transfer.Digest(parts...)
	plan.work = state
	return plan, nil
}

// sameColumns reports whether the target's table has every column the
// source's has, under the same names. Order needn't match: writes name them.
func sameColumns(src, dst myTable) bool {
	have := map[string]bool{}
	for _, c := range dst.Columns {
		have[c.Name] = true
	}
	for _, c := range src.Columns {
		if !have[c.Name] {
			return false
		}
	}
	return true
}

func (s *mysqlSet) Start(ctx context.Context, plan DatasetPlan, _ *transfer.Budget) (DatasetStatus, error) {
	st := DatasetStatus{Kind: "mysql", Findings: plan.Findings}
	if !s.tablesKnown {
		return st, ErrMove
	}
	var from myPos
	switch plan.work {
	case "resume":
		from = s.saved.myPos
	case "new":
		if s.copySchema && plan.Copy > 0 {
			if err := s.makeSchema(ctx); err != nil {
				return st, err
			}
		}
		var err error
		if from, err = s.src.Position(ctx); err != nil {
			return st, err
		}
		if s.settle > 0 {
			t := time.NewTimer(s.settle)
			select {
			case <-ctx.Done():
				t.Stop()
				return st, ctx.Err()
			case <-t.C:
			}
		}
		n, bytes, err := s.copyAll(ctx)
		st.Copied, st.Bytes = n, bytes
		if err != nil {
			return st, err
		}
		// From here a restart reads the log from where the copy began.
		if err := s.save(from); err != nil {
			return st, err
		}
	default:
		return st, ErrMove
	}
	s.mu.Lock()
	s.applied, s.err = from, nil
	s.mu.Unlock()
	s.run(ctx, from)
	return st, nil
}

// makeSchema makes the tables the target doesn't have, as the source has
// them. Views, triggers, routines and grants are left to the customer.
func (s *mysqlSet) makeSchema(ctx context.Context) error {
	there, err := s.dst.Tables(ctx)
	if err != nil {
		return err
	}
	have := map[string]bool{}
	for _, t := range there {
		have[t.Name] = true
	}
	for _, t := range s.tables {
		if have[t.Name] {
			continue
		}
		stmt, err := s.src.CreateTable(ctx, t)
		if err != nil {
			return err
		}
		if err := s.dst.Create(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

// copyBatch bounds one insert: rows and bytes.
const (
	copyRows  = 500
	copyBytes = 1 << 20
	copyCells = 60000 // under the protocol's 65535 placeholders
)

// copyAll copies every table from one consistent read of the source.
func (s *mysqlSet) copyAll(ctx context.Context) (rows, bytes int64, err error) {
	snap, err := s.src.Snapshot(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer snap.Close()
	for _, t := range s.tables {
		r, err := snap.Rows(ctx, t)
		if err != nil {
			return rows, bytes, err
		}
		limit := min(copyRows, max(1, copyCells/max(1, len(t.Columns))))
		var batch [][]any
		size := 0
		flush := func() error {
			if len(batch) == 0 {
				return nil
			}
			if err := s.dst.Insert(ctx, t, batch); err != nil {
				return err
			}
			rows += int64(len(batch))
			bytes += int64(size)
			batch, size = batch[:0], 0
			return nil
		}
		for {
			row, err := r.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				r.Close()
				return rows, bytes, err
			}
			batch = append(batch, row)
			for _, c := range row {
				if b, ok := c.([]byte); ok {
					size += len(b)
				}
			}
			if len(batch) >= limit || size >= copyBytes {
				if err := flush(); err != nil {
					r.Close()
					return rows, bytes, err
				}
			}
		}
		r.Close()
		if err := flush(); err != nil {
			return rows, bytes, err
		}
	}
	return rows, bytes, nil
}

func (s *mysqlSet) save(p myPos) error {
	return s.w.Save(s.stateName(), myState{Source: s.srcID, Target: s.dstID, myPos: p})
}

// run starts the stream from a place in the log.
func (s *mysqlSet) run(ctx context.Context, from myPos) {
	sctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	s.mu.Lock()
	s.cancel, s.done = cancel, done
	s.mu.Unlock()
	go func() {
		defer close(done)
		err := s.stream(sctx, from)
		s.mu.Lock()
		if s.err == nil {
			s.err = err
		}
		s.mu.Unlock()
	}()
}

// halt ends the stream and waits for it.
func (s *mysqlSet) halt() {
	s.mu.Lock()
	cancel, done := s.cancel, s.done
	s.cancel, s.done = nil, nil
	s.mu.Unlock()
	if cancel != nil {
		cancel()
		<-done
	}
}

// stream reads the log and writes each transaction of the database's
// tables to the target in one of its own. Where it has got to moves on at
// each commit, and at each statement outside a transaction, so a restart
// begins at the start of a transaction.
func (s *mysqlSet) stream(ctx context.Context, from myPos) error {
	log, err := s.src.Binlog(ctx, from)
	if err != nil {
		return err
	}
	defer log.Close()
	byName := map[string]*myTable{}
	for i := range s.tables {
		byName[s.tables[i].Name] = &s.tables[i]
	}
	var tx myTx
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	inTx := false
	var n int64
	for {
		ev, err := log.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		switch ev.Kind {
		case evBegin:
			inTx = true
		case evRows:
			if ev.Schema != s.db {
				continue
			}
			t := byName[ev.Table]
			if t == nil || ev.Columns != len(t.Columns) {
				return errSchemaChanged
			}
			if tx == nil {
				if tx, err = s.dst.Begin(ctx); err != nil {
					return err
				}
			}
			if err := applyRows(ctx, tx, t, ev); err != nil {
				return err
			}
			n += int64(len(ev.Rows))
			if ev.Op == opUpdate {
				n -= int64(len(ev.Rows)) / 2
			}
		case evCommit:
			if tx != nil {
				err := tx.Commit(ctx)
				tx = nil
				if err != nil {
					return err
				}
			}
			inTx = false
			s.advance(ev.Next, ev.Time, n)
			n = 0
		case evOther:
			if ev.DDL {
				return errSchemaChanged
			}
			if !inTx && tx == nil {
				s.advance(ev.Next, ev.Time, 0)
			}
		}
	}
}

func (s *mysqlSet) advance(p myPos, at time.Time, rows int64) {
	s.mu.Lock()
	s.applied = p
	if !at.IsZero() {
		s.at = at
	}
	s.rows += rows
	s.mu.Unlock()
}

// applyRows writes one rows event: inserts as rows, updates as the new rows
// after removing any whose key changed, deletes by key.
func applyRows(ctx context.Context, tx myTx, t *myTable, ev myEvent) error {
	for _, r := range ev.Rows {
		if len(r) != len(t.Columns) {
			return errSchemaChanged
		}
	}
	switch ev.Op {
	case opInsert:
		return tx.Replace(ctx, *t, ev.Rows)
	case opDelete:
		return tx.Delete(ctx, *t, ev.Rows)
	case opUpdate:
		if len(ev.Rows)%2 != 0 {
			return ErrMySQL
		}
		var gone, after [][]any
		for i := 0; i < len(ev.Rows); i += 2 {
			before, now := ev.Rows[i], ev.Rows[i+1]
			if !sameKey(t, before, now) {
				gone = append(gone, before)
			}
			after = append(after, now)
		}
		if len(gone) > 0 {
			if err := tx.Delete(ctx, *t, gone); err != nil {
				return err
			}
		}
		return tx.Replace(ctx, *t, after)
	}
	return ErrMySQL
}

func sameKey(t *myTable, a, b []any) bool {
	for _, i := range t.Key {
		if cellString(a[i]) != cellString(b[i]) || (a[i] == nil) != (b[i] == nil) {
			return false
		}
	}
	return true
}

// cellString is a cell's value as text, to compare two of them.
func cellString(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case []byte:
		return string(x)
	case string:
		return x
	}
	return strconvAny(v)
}

func (s *mysqlSet) Round(ctx context.Context, _ *transfer.Budget) (DatasetStatus, error) {
	st := DatasetStatus{Kind: "mysql"}
	s.mu.Lock()
	err, applied, rows := s.err, s.applied, s.rows
	s.rows = 0
	running := s.done != nil
	s.mu.Unlock()
	st.Copied = rows
	if !running {
		return st, ErrMove
	}
	if err != nil {
		if errors.Is(err, errSchemaChanged) {
			st.Findings = []string{findingSchemaChanged}
			return st, err
		}
		// The stream stopped on a failed call: the next one starts again
		// where the last commit left it.
		s.halt()
		s.mu.Lock()
		s.err = nil
		s.mu.Unlock()
		s.run(ctx, applied)
		return st, ErrMySQL
	}
	if err := s.save(applied); err != nil {
		return st, err
	}
	st.LagBytes, st.LagSeconds, err = s.lag(ctx, applied)
	return st, err
}

// lag is how far the target is behind the source's log: in bytes of log,
// and in seconds since the last change it wrote was made, if it's behind.
func (s *mysqlSet) lag(ctx context.Context, applied myPos) (int64, int, error) {
	cur, err := s.src.Position(ctx)
	if err != nil {
		return 0, 0, err
	}
	if cur == applied {
		return 0, 0, nil
	}
	var bytes int64
	if cur.File == applied.File {
		bytes = int64(cur.Pos) - int64(applied.Pos)
	} else {
		logs, err := s.src.Logs(ctx)
		if err != nil {
			return 0, 0, err
		}
		for _, l := range logs {
			switch {
			case l.File == applied.File:
				bytes += l.Size - int64(applied.Pos)
			case l.File > applied.File && l.File < cur.File:
				bytes += l.Size
			}
		}
		bytes += int64(cur.Pos)
	}
	bytes = max(bytes, 1)
	s.mu.Lock()
	at := s.at
	s.mu.Unlock()
	secs := 0
	if !at.IsZero() {
		secs = max(0, int(s.now().Sub(at)/time.Second))
	}
	return bytes, secs, nil
}

// Check compares every table's rows on the two sides.
func (s *mysqlSet) Check(ctx context.Context) (DatasetStatus, error) {
	st := DatasetStatus{Kind: "mysql"}
	tables, err := s.src.Tables(ctx)
	if err != nil {
		return st, err
	}
	there, err := s.dst.Tables(ctx)
	if err != nil {
		return st, err
	}
	byName := map[string]myTable{}
	for _, t := range there {
		byName[t.Name] = t
	}
	different := int64(0)
	for _, t := range tables {
		d, ok := byName[t.Name]
		if !ok || !sameColumns(t, d) {
			different++
			if !slices.Contains(st.Findings, findingSchema) {
				st.Findings = append(st.Findings, findingSchema)
			}
			continue
		}
		a, err := s.src.Sum(ctx, t)
		if err != nil {
			return st, err
		}
		// The target's sum reads its columns as the source has them.
		b, err := s.dst.Sum(ctx, t)
		if err != nil {
			return st, err
		}
		if a != b {
			different++
		}
	}
	st.Extra = different
	s.mu.Lock()
	streamErr, applied, ran := s.err, s.applied, s.applied != (myPos{})
	s.mu.Unlock()
	if errors.Is(streamErr, errSchemaChanged) {
		st.Findings = append(st.Findings, findingSchemaChanged)
	}
	if ran && different > 0 {
		if st.LagBytes, st.LagSeconds, err = s.lag(ctx, applied); err != nil {
			return st, err
		}
	}
	st.Verified = len(tables) > 0 && different == 0 && len(st.Findings) == 0
	return st, nil
}

// Stop ends the stream and keeps where it got to. The stream only ever
// wrote from this process, so once it has ended nothing writes.
func (s *mysqlSet) Stop(ctx context.Context) error {
	s.halt()
	s.mu.Lock()
	applied := s.applied
	s.mu.Unlock()
	if applied != (myPos{}) && s.w != nil {
		// A round saved it a moment ago at worst; the log read again from
		// there ends the same.
		_ = s.save(applied)
	}
	return nil
}

// Position is where the source's log is. Any write on the source moves it,
// so a cutover reads it twice to tell its writers stopped.
func (s *mysqlSet) Position(ctx context.Context) (string, error) {
	p, err := s.src.Position(ctx)
	if err != nil {
		return "", err
	}
	return p.File + ":" + strconv.FormatUint(uint64(p.Pos), 10), nil
}

// hashID hashes what names a server and database, so the workspace and a
// plan's digest hold neither.
func hashID(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write([]byte(strconv.Itoa(len(p))))
		h.Write([]byte{0})
		h.Write([]byte(p))
	}
	return hex.EncodeToString(h.Sum(nil)[:16])
}
