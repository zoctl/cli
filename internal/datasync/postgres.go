package datasync

// A postgres dataset is Postgres's own logical replication: the customer
// makes a publication and a logical slot on the source with their own
// credentials, and zoctl makes a subscription on the target that streams from
// that slot. zoctl's own connection to the source is a read-only session that
// only reads the catalog and counts rows.

import (
	"context"
	"errors"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/zoctl/cli/internal/pgwire"
	"github.com/zoctl/cli/internal/transfer"
)

// ErrPostgres means a Postgres step failed. The server's message isn't kept:
// it can name tables and values.
var ErrPostgres = errors.New("datasync: a postgres step failed")

// Finding codes a postgres dataset reports.
const (
	findingPrerequisite = "replication_prerequisite"
	findingSchema       = "schema_mismatch"
	findingNotEmpty     = "target_not_empty"
	// findingTablesOnly keeps the code it had when copy_schema made only
	// tables: it is what copy_schema leaves to the customer (schema.go).
	findingTablesOnly = "schema_copied_tables_only"
	// findingSchemaLeft is a default, constraint or index copy_schema
	// couldn't make on the target, so the target lacks it.
	findingSchemaLeft   = "schema_part_not_copied"
	findingNoIdentity   = "replica_identity_missing"
	findingNoSequence   = "sequence_unreadable"
	findingSameDatabase = "source_is_target"
	// findingCopyNotStarted is a subscription whose apply worker has run
	// for a while with every table still waiting and no worker copying
	// one: the target had no background worker free for it (Azure's B2s,
	// 20 November 2026, until max_worker_processes went from 8 to 16).
	findingCopyNotStarted = "table_copy_not_started"
)

// copyNotStarted is true when the subscription's apply worker started more
// than 30 seconds ago and no table-sync worker runs for it. Postgres starts
// one within seconds of the apply worker when a table waits, so none by then
// is a server that couldn't. A role that may not see the worker's start gets
// nothing back, which reads as false.
const copyNotStarted = `SELECT (count(*) > 0)::text FROM pg_stat_subscription s JOIN pg_stat_activity a ON a.pid = s.pid
WHERE s.subname = $1 AND s.relid IS NULL AND a.backend_start < now() - interval '30 seconds'
AND s.subid IN (SELECT oid FROM pg_subscription WHERE ` + thisDatabase + `)
AND NOT EXISTS (SELECT 1 FROM pg_stat_subscription w WHERE w.subid = s.subid AND w.relid IS NOT NULL)`

// whichDatabase tells one running database from every other: the cluster's
// identifier, which a clone restored from a backup shares, when this server
// started, which it doesn't, and the database's name.
const whichDatabase = "SELECT system_identifier::text || ' ' || pg_postmaster_start_time()::text || ' ' || current_database() FROM pg_control_system()"

// ErrSequence means the target has a sequence whose value on the source
// the source's role may not read. A cutover that left it where it is would
// hand out numbers the copied rows already have.
var ErrSequence = errors.New("datasync: the source's role can't read a sequence the target has")

// hiddenSequences lists the source's sequences its role may not read.
// pg_sequences shows those with no value, like one never used.
const hiddenSequences = `SELECT schemaname::text, sequencename::text FROM pg_sequences
WHERE NOT has_sequence_privilege(quote_ident(schemaname) || '.' || quote_ident(sequencename), 'SELECT, USAGE') ORDER BY 1, 2`

// db is what a dataset needs from one side's connection. Rows returns every
// column as text, so each query casts what it selects.
type db interface {
	Rows(ctx context.Context, sql string, args ...any) ([][]string, error)
	Close()
}

// targetDB can also write. Only the target's connection is one.
type targetDB interface {
	db
	Exec(ctx context.Context, sql string) error
}

// ErrPlainDatabase means a database URL allows a connection without TLS. A
// URL has to say sslmode=require, verify-ca or verify-full.
var ErrPlainDatabase = errors.New("datasync: a database URL has to ask for TLS: sslmode=require, verify-ca or verify-full")

// connectTimeout is how long a database has to answer when its URL sets no
// connect_timeout of its own.
const connectTimeout = 30 * time.Second

// pgConfig reads a database URL and refuses one that allows a connection
// without TLS. The default, sslmode=prefer, falls back to plaintext for any
// server that says it has no TLS, which is whoever sits between the two, and
// the password and every row then cross in the clear. The same URL is the one
// a subscription's server dials with, so the rule covers that hop too.
func pgConfig(url string) (*pgwire.Config, error) {
	cfg, err := pgwire.ParseURL(url)
	if errors.Is(err, pgwire.ErrPlain) {
		return nil, ErrPlainDatabase
	}
	if err != nil {
		return nil, ErrMove
	}
	return cfg, nil
}

// connect opens a connection from a URL. A source is opened with every
// transaction read-only, so even a bug here can't write to it.
var connect = func(ctx context.Context, url string, readOnly bool) (targetDB, error) {
	cfg, err := pgConfig(url)
	if err != nil {
		return nil, err
	}
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = connectTimeout
	}
	// literal quotes for standard strings, whatever the role's default is.
	cfg.Params["standard_conforming_strings"] = "on"
	if readOnly {
		cfg.Params["default_transaction_read_only"] = "on"
	}
	cfg.Params["application_name"] = "zoctl"
	c, err := pgwire.Connect(ctx, cfg)
	if err != nil {
		return nil, ErrPostgres
	}
	return wireDB{c}, nil
}

type wireDB struct{ c *pgwire.Conn }

func (p wireDB) Rows(ctx context.Context, sql string, args ...any) ([][]string, error) {
	rows, err := p.c.Rows(ctx, sql, args...)
	if err != nil {
		return nil, ErrPostgres
	}
	return rows, nil
}

func (p wireDB) Exec(ctx context.Context, sql string) error {
	if err := p.c.Exec(ctx, sql); err != nil {
		return ErrPostgres
	}
	return nil
}

func (p wireDB) Close() { _ = p.c.Close() }

// readOnly hides a connection's Exec, so the source's type has no way to
// write.
type readOnly struct{ db }

// thisDatabase keeps a pg_subscription lookup to the database the connection
// is in. The catalog is the whole server's, and another database can have a
// subscription of the same name.
const thisDatabase = "subdbid = (SELECT oid FROM pg_database WHERE datname = current_database())"

type table struct{ schema, name string }

func (t table) ident() string { return pgwire.Ident(t.schema, t.name) }

// pgSet is one published database replicated to a target database.
type pgSet struct {
	src         db
	dst         targetDB
	sourceURL   string
	publication string
	slot        string
	sub         string
	copySchema  bool
	// twoWay is a database with a way back (back.go): both directions'
	// subscriptions ask only for changes made on the side they read, so
	// what one brought is never sent round again. backName names the
	// publication, slot and subscription of the way back, and peer the
	// subscription the other direction keeps on the side this one reads.
	twoWay   bool
	backName string
	peer     string
	// back is the way back itself: src is the target, dst the old side.
	back bool
	// redial connects to dst again, for Stop on a connection that dropped.
	redial func(context.Context) (targetDB, error)
	// sleep replaces the wait for a stopped subscription's slot in tests.
	sleep func(context.Context) error

	tables []table
}

func openPostgres(d Dataset, o Options) (dataset, error) {
	from, to := o.Env(d.FromEnv), o.Env(d.ToEnv)
	if from == "" || to == "" || from == to {
		return nil, ErrMove
	}
	for _, url := range []string{from, to} {
		if _, err := pgConfig(url); err != nil {
			return nil, err
		}
	}
	ctx := context.Background()
	src, err := connect(ctx, from, true)
	if err != nil {
		return nil, err
	}
	dst, err := connect(ctx, to, false)
	if err != nil {
		src.Close()
		return nil, err
	}
	set := &pgSet{
		src: readOnly{src}, dst: dst, sourceURL: from,
		publication: d.Publication, slot: d.Slot, copySchema: d.CopySchema,
		sub: subName(d),
	}
	set.redial = func(ctx context.Context) (targetDB, error) { return connect(ctx, to, false) }
	if d.BackEnv != "" {
		set.twoWay, set.backName, set.peer = true, backName(d), backName(d)
	}
	return set, nil
}

// subName is the subscription a dataset's sync keeps on the target, and
// backName the publication and slot its way back keeps there and the
// subscription it keeps on the old side.
func subName(d Dataset) string  { return "zoctl_" + strings.ReplaceAll(d.Name, "-", "_") }
func backName(d Dataset) string { return "zoctl_back_" + strings.ReplaceAll(d.Name, "-", "_") }

// openPostgresBack opens a database's way back: the target is read, with
// every transaction read-only, and the old side is the one written, by the
// URL in the dataset's STANDBY_ name.
func openPostgresBack(d Dataset, o Options) (dataset, error) {
	target, old, reach := o.Env(d.ToEnv), o.Env(d.BackEnv), o.Env(d.BackFromEnv)
	if target == "" || old == "" || reach == "" || target == old || reach == old {
		return nil, ErrMove
	}
	for _, url := range []string{target, old, reach} {
		if _, err := pgConfig(url); err != nil {
			return nil, err
		}
	}
	ctx := context.Background()
	src, err := connect(ctx, target, true)
	if err != nil {
		return nil, err
	}
	dst, err := connect(ctx, old, false)
	if err != nil {
		src.Close()
		return nil, err
	}
	return &pgSet{
		src: readOnly{src}, dst: dst, sourceURL: reach,
		publication: backName(d), slot: backName(d), sub: backName(d),
		twoWay: true, back: true, backName: backName(d), peer: subName(d),
		redial: func(ctx context.Context) (targetDB, error) { return connect(ctx, old, false) },
	}, nil
}

func (p *pgSet) Close() {
	p.src.Close()
	p.dst.Close()
}

func one(rows [][]string, err error) (string, bool, error) {
	if err != nil {
		return "", false, err
	}
	if len(rows) == 0 || len(rows[0]) == 0 {
		return "", false, nil
	}
	return rows[0][0], true, nil
}

func (p *pgSet) Plan(ctx context.Context) (DatasetPlan, error) {
	plan := DatasetPlan{Kind: "postgres"}
	var findings []string
	blocked := false
	block := func(code string) {
		blocked = true
		findings = append(findings, code)
	}

	// Both URLs come from the environment, so one database named twice
	// shows nowhere in the move file. With its tables empty nothing below
	// stops that either, and the subscription would be made on the
	// source. A side whose role may not ask is taken to differ.
	if here, ok, err := one(p.src.Rows(ctx, whichDatabase)); err == nil && ok {
		if there, ok, err := one(p.dst.Rows(ctx, whichDatabase)); err == nil && ok && here == there {
			block(findingSameDatabase)
		}
	}

	// The source: what the customer set up.
	wal, _, err := one(p.src.Rows(ctx, "SELECT current_setting('wal_level')"))
	if err != nil {
		return plan, err
	}
	if wal != "logical" {
		block(findingPrerequisite)
	}
	_, pub, err := one(p.src.Rows(ctx, "SELECT pubname::text FROM pg_publication WHERE pubname = $1", p.publication))
	if err != nil {
		return plan, err
	}
	slot, err := p.src.Rows(ctx, "SELECT plugin::text, slot_type::text, (database = current_database())::text FROM pg_replication_slots WHERE slot_name = $1", p.slot)
	if err != nil {
		return plan, err
	}
	if !pub || len(slot) != 1 || slot[0][0] != "pgoutput" || slot[0][1] != "logical" || slot[0][2] != "true" {
		if !blocked {
			block(findingPrerequisite)
		}
	}
	if err := p.loadTables(ctx); err != nil {
		return plan, err
	}
	if pub && len(p.tables) == 0 && !blocked {
		block(findingPrerequisite)
	}
	// A published table with no primary key or replica identity can't
	// replicate updates or deletes; the source refuses them until it has one.
	noID, _, err := one(p.src.Rows(ctx, `SELECT count(*)::text FROM pg_publication_tables p
JOIN pg_namespace n ON n.nspname = p.schemaname
JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = p.tablename
WHERE p.pubname = $1 AND c.relreplident = 'd'
AND NOT EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary)`, p.publication))
	if err != nil {
		return plan, err
	}
	if noID != "0" && noID != "" {
		findings = append(findings, findingNoIdentity)
	}
	// Rows copy without it, so this doesn't block a sync. A cutover is
	// refused over one the target has.
	hidden, err := p.src.Rows(ctx, hiddenSequences)
	if err != nil {
		return plan, err
	}
	if len(hidden) > 0 {
		findings = append(findings, findingNoSequence)
	}
	if p.twoWay {
		ok, err := p.loopFree(ctx)
		if err != nil {
			return plan, err
		}
		if !ok {
			block(findingReturnSetup)
		}
	}

	// The target: an earlier approval's subscription is enabled again, and
	// a new one needs every table there and empty, or made from the source.
	state := "new"
	enabled, exists, err := one(p.dst.Rows(ctx, "SELECT subenabled::text FROM pg_subscription WHERE subname = $1 AND "+thisDatabase, p.sub))
	if err != nil {
		return plan, err
	}
	var missing, nonEmpty int
	switch {
	case exists:
		state = "resume:" + enabled
		// A subscription of this name that reads another slot or another
		// publication isn't this move's, and enabling it would stream
		// something else into the target.
		ours, _, err := one(p.dst.Rows(ctx, "SELECT (coalesce(subslotname::text, '') = $2 AND subpublications::text[] = ARRAY[$3]::text[])::text FROM pg_subscription WHERE subname = $1 AND "+thisDatabase, p.sub, p.slot, p.publication))
		if err != nil {
			return plan, err
		}
		if ours != "true" {
			block(findingPrerequisite)
		}
	case p.back:
		// The old side had every row when the slot was made, at the
		// cutover, so nothing is copied: the tables only have to be there.
		state = "back"
		for _, t := range p.tables {
			reg, _, err := one(p.dst.Rows(ctx, "SELECT coalesce(to_regclass($1)::text, '')", t.ident()))
			if err != nil {
				return plan, err
			}
			if reg == "" {
				missing++
			}
		}
	default:
		// Postgres copies each table through a slot of its own, which it
		// makes on the source and drops when the table is copied. With
		// none free the copy waits for ever.
		free, _, err := one(p.src.Rows(ctx, "SELECT (current_setting('max_replication_slots')::int - count(*))::text FROM pg_replication_slots"))
		if err != nil {
			return plan, err
		}
		if n, _ := strconv.Atoi(free); n < 1 && !blocked {
			block(findingPrerequisite)
		}
		for _, t := range p.tables {
			reg, _, err := one(p.dst.Rows(ctx, "SELECT coalesce(to_regclass($1)::text, '')", t.ident()))
			if err != nil {
				return plan, err
			}
			if reg == "" {
				missing++
				continue
			}
			any, _, err := one(p.dst.Rows(ctx, "SELECT EXISTS (SELECT 1 FROM "+t.ident()+")::text"))
			if err != nil {
				return plan, err
			}
			if any == "true" {
				nonEmpty++
			}
		}
	}
	if missing > 0 && !p.copySchema {
		block(findingSchema)
	}
	if missing > 0 && p.copySchema {
		findings = append(findings, findingTablesOnly)
	}
	if nonEmpty > 0 {
		block(findingNotEmpty)
	}

	plan.Tables = int64(len(p.tables))
	plan.Copy = int64(missing)
	plan.Extra = int64(nonEmpty)
	plan.Same = int64(len(p.tables) - missing)
	plan.Findings = findings
	plan.Blocked = blocked
	if p.back {
		// Nothing is copied into the old side, only replicated.
		plan.Copy = 0
	}
	parts := [][]byte{[]byte("postgres-plan-v1"), []byte(p.publication), []byte(p.slot), []byte(p.sub), []byte(state), []byte(strconv.FormatBool(p.twoWay)),
		[]byte(strconv.FormatBool(p.copySchema)), []byte(strconv.Itoa(missing)), []byte(strconv.Itoa(nonEmpty))}
	for _, t := range p.tables {
		parts = append(parts, []byte(t.schema), []byte(t.name))
	}
	plan.Digest = transfer.Digest(parts...)
	plan.work = state
	return plan, nil
}

// loadTables reads the publication's tables from the source.
func (p *pgSet) loadTables(ctx context.Context) error {
	rows, err := p.src.Rows(ctx, "SELECT schemaname::text, tablename::text FROM pg_publication_tables WHERE pubname = $1 ORDER BY 1, 2", p.publication)
	if err != nil {
		return err
	}
	p.tables = p.tables[:0]
	for _, r := range rows {
		p.tables = append(p.tables, table{r[0], r[1]})
	}
	return nil
}

// literal quotes s as a standard SQL string literal.
func literal(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// quiet turns off, for this session, what would write a statement to the
// server's log, and loud puts it back.
var (
	quiet = []string{"SET log_statement = 'none'", "SET log_min_duration_statement = -1", "SET log_min_error_statement = 'panic'"}
	loud  = []string{"RESET log_statement", "RESET log_min_duration_statement", "RESET log_min_error_statement"}
)

// subscribe runs a CREATE SUBSCRIPTION, whose text holds the other side's URL
// and so its password. A server set to log statements, slow ones or failed
// ones would write it to its log, so the session asks for none of that first.
// A role that may not change those settings (they take a superuser, or a
// grant on the parameter) is refused, and the subscription is made all the
// same: the README says what to check on such a server.
func (p *pgSet) subscribe(ctx context.Context, statement string) error {
	for _, set := range quiet {
		_ = p.dst.Exec(ctx, set)
	}
	err := p.dst.Exec(ctx, statement)
	for _, reset := range loud {
		_ = p.dst.Exec(ctx, reset)
	}
	return err
}

func (p *pgSet) Start(ctx context.Context, plan DatasetPlan, _ *transfer.Budget) (DatasetStatus, error) {
	st := DatasetStatus{Kind: "postgres", Findings: plan.Findings}
	state, _ := plan.work.(string)
	sub := pgwire.Ident(p.sub)
	if strings.HasPrefix(state, "resume:") {
		if p.twoWay {
			// A subscription made before the move file had a way back
			// would send round what the other direction brought.
			if err := p.dst.Exec(ctx, "ALTER SUBSCRIPTION "+sub+" SET (origin = none)"); err != nil {
				return st, err
			}
		}
		return st, p.dst.Exec(ctx, "ALTER SUBSCRIPTION "+sub+" ENABLE")
	}
	if p.back {
		// The slot was made on the target when the move cut over, with
		// both sides holding the same rows, so it has every change since
		// and nothing is copied.
		return st, p.subscribe(ctx, "CREATE SUBSCRIPTION "+sub+
			" CONNECTION "+literal(p.sourceURL)+
			" PUBLICATION "+pgwire.Ident(p.publication)+
			" WITH (slot_name = "+literal(p.slot)+", create_slot = false, copy_data = false, enabled = true, origin = none)")
	}
	if p.copySchema && plan.Copy > 0 {
		left, err := p.makeSchema(ctx)
		if err != nil {
			return st, err
		}
		if left > 0 {
			st.Findings = append(slices.Clone(st.Findings), findingSchemaLeft)
		}
	}
	// The slot is the customer's, so the subscription never makes or drops
	// it. copy_data copies what's there, each table through a slot Postgres
	// makes on the source for that copy and drops after it, then the
	// customer's slot streams the rest.
	with := "slot_name = " + literal(p.slot) + ", create_slot = false, copy_data = true, enabled = true"
	if p.twoWay {
		with += ", origin = none"
	}
	err := p.subscribe(ctx, "CREATE SUBSCRIPTION "+sub+
		" CONNECTION "+literal(p.sourceURL)+
		" PUBLICATION "+pgwire.Ident(p.publication)+
		" WITH ("+with+")")
	return st, err
}

// loopFree reads whether the two directions can share a database without
// sending each other's rows round again. Each subscription asks only for
// changes made on the side it reads (origin = none), which takes Postgres
// 16 on both. That also leaves out what any other subscription brings to
// the side read, so one there that isn't zoctl's is refused: its rows
// would never cross.
func (p *pgSet) loopFree(ctx context.Context) (bool, error) {
	for _, side := range []db{p.src, p.dst} {
		version, _, err := one(side.Rows(ctx, "SELECT current_setting('server_version_num')"))
		if err != nil {
			return false, err
		}
		if n, _ := strconv.Atoi(version); n < 160000 {
			return false, nil
		}
	}
	others, _, err := one(p.src.Rows(ctx, "SELECT count(*)::text FROM pg_subscription WHERE subname <> $1 AND "+thisDatabase, p.peer))
	if err != nil {
		return false, err
	}
	return others == "0", nil
}

// backReady reads whether the target can hold a way back: logical WAL, a
// slot to spare or the way back's own from an earlier cutover, and both
// directions able to share the database.
func (p *pgSet) backReady(ctx context.Context) (bool, error) {
	if !p.twoWay || p.back {
		return false, nil
	}
	wal, _, err := one(p.dst.Rows(ctx, "SELECT current_setting('wal_level')"))
	if err != nil {
		return false, err
	}
	free, _, err := one(p.dst.Rows(ctx, "SELECT (current_setting('max_replication_slots')::int - count(*) FILTER (WHERE slot_name <> $1))::text FROM pg_replication_slots", p.backName))
	if err != nil {
		return false, err
	}
	if n, _ := strconv.Atoi(free); wal != "logical" || n < 1 {
		return false, nil
	}
	// The way back reads the target, where the sync's own subscription is
	// the only one that may be.
	others, _, err := one(p.dst.Rows(ctx, "SELECT count(*)::text FROM pg_subscription WHERE subname <> $1 AND "+thisDatabase, p.sub))
	if err != nil || others != "0" {
		return false, err
	}
	return p.loopFree(ctx)
}

// makeBack makes the way back on the target as a cutover's last step
// before DNS: a publication of the tables the move publishes and a slot,
// which from that moment holds every change the target takes. The two
// sides hold the same rows then, so one left by an earlier cutover has
// nothing the old side lacks and is made again. The publication comes
// first: a slot older than its publication can't be read.
func (p *pgSet) makeBack(ctx context.Context) error {
	if err := p.loadTables(ctx); err != nil {
		return err
	}
	if len(p.tables) == 0 {
		return ErrPostgres
	}
	if err := p.endBack(ctx); err != nil {
		return err
	}
	var names []string
	for _, t := range p.tables {
		names = append(names, t.ident())
	}
	if err := p.dst.Exec(ctx, "CREATE PUBLICATION "+pgwire.Ident(p.backName)+" FOR TABLE "+strings.Join(names, ", ")); err != nil {
		return err
	}
	_, _, err := one(p.dst.Rows(ctx, "SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput')", p.backName))
	return err
}

// endBack drops the way back's slot and publication from the target, so
// the slot stops holding the target's WAL. A return ends with it, and the
// next cutover makes them again.
func (p *pgSet) endBack(ctx context.Context) error {
	active, exists, err := one(p.dst.Rows(ctx, "SELECT active::text FROM pg_replication_slots WHERE slot_name = $1", p.backName))
	if err != nil {
		return err
	}
	// The old side's subscription lets go of the slot a moment after it
	// is stopped.
	for tries := 0; exists && active == "true" && tries < 20; tries++ {
		if err := p.pause(ctx); err != nil {
			return err
		}
		if active, exists, err = one(p.dst.Rows(ctx, "SELECT active::text FROM pg_replication_slots WHERE slot_name = $1", p.backName)); err != nil {
			return err
		}
	}
	if exists {
		if _, _, err := one(p.dst.Rows(ctx, "SELECT pg_drop_replication_slot($1)::text", p.backName)); err != nil {
			return err
		}
	}
	return p.dst.Exec(ctx, "DROP PUBLICATION IF EXISTS "+pgwire.Ident(p.backName))
}

// progress reads replication progress: tables ready, the slot's lag in
// bytes on the source, and seconds since the target heard from it.
//
// The lag is how far the source's WAL is past what the target confirmed.
// That WAL is the whole server's: another database's rows, a table the
// publication leaves out, a checkpoint. A subscription that is running
// confirms past all of it; one that is stopped, as it is after a run and
// through a cutover, doesn't, so the distance grows with nothing of the
// move's in it. When exact is set and there is a distance, progress asks
// the slot whether any of it is a change the publication sends.
func (p *pgSet) progress(ctx context.Context, exact bool) (DatasetStatus, error) {
	st := DatasetStatus{Kind: "postgres"}
	states, err := p.dst.Rows(ctx, `SELECT r.srsubstate::text, n.nspname::text, c.relname::text FROM pg_subscription_rel r
JOIN pg_subscription s ON s.oid = r.srsubid JOIN pg_class c ON c.oid = r.srrelid
JOIN pg_namespace n ON n.oid = c.relnamespace WHERE s.subname = $1 AND s.`+thisDatabase, p.sub)
	if err != nil {
		return st, err
	}
	// Only the tables published now count. A subscription keeps the ones
	// it had when it was made, and one dropped from the publication since
	// mustn't stand in for one added since, which it doesn't copy.
	waiting := 0
	for _, s := range states {
		if len(s) != 3 || !slices.Contains(p.tables, table{s[1], s[2]}) {
			continue
		}
		switch s[0] {
		case "r":
			st.Copied++
		case "i":
			waiting++
		}
	}
	st.Pending = int64(len(p.tables)) - st.Copied
	if !p.back && waiting > 0 && waiting == len(p.tables) {
		// Not one table has started copying. Say so if the subscription
		// has been running long enough that one should have.
		if stuck, _, err := one(p.dst.Rows(ctx, copyNotStarted, p.sub)); err == nil && stuck == "true" {
			st.Findings = append(st.Findings, findingCopyNotStarted)
		}
	}
	if p.back && len(states) == 0 {
		// No reverse sync has run, so the old side has no subscription.
		// It copies nothing, so nothing is pending but what the slot holds.
		st.Pending = 0
	}
	slot, err := p.src.Rows(ctx, "SELECT coalesce(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)::bigint::text, active::text FROM pg_replication_slots WHERE slot_name = $1", p.slot)
	if err != nil {
		return st, err
	}
	if len(slot) != 1 || len(slot[0]) != 2 {
		st.Findings = []string{findingPrerequisite}
		return st, nil
	}
	st.LagBytes, _ = strconv.ParseInt(slot[0][0], 10, 64)
	if st.LagBytes > 0 && exact && p.nothingPending(ctx, slot[0][1] == "true") {
		st.LagBytes = 0
	}
	if st.LagBytes > 0 {
		secs, _, err := one(p.dst.Rows(ctx, "SELECT coalesce(extract(epoch FROM now() - latest_end_time), 0)::int::text FROM pg_stat_subscription WHERE subname = $1 AND relid IS NULL AND subid IN (SELECT oid FROM pg_subscription WHERE "+thisDatabase+")", p.sub))
		if err != nil {
			return st, err
		}
		st.LagSeconds, _ = strconv.Atoi(secs)
		st.LagSeconds = max(st.LagSeconds, 1)
	}
	return st, nil
}

func (p *pgSet) Round(ctx context.Context, _ *transfer.Budget) (DatasetStatus, error) {
	st, err := p.progress(ctx, false)
	// Rows replicate on their own; a round copies nothing it can count.
	st.Copied = 0
	return st, err
}

// Check is progress plus the row count of every published table on both
// sides. It is verified when every table is ready, the slot has nothing
// left and every count matches.
func (p *pgSet) Check(ctx context.Context) (DatasetStatus, error) {
	if err := p.loadTables(ctx); err != nil {
		return DatasetStatus{Kind: "postgres"}, err
	}
	st, err := p.progress(ctx, true)
	if err != nil {
		return st, err
	}
	different := int64(0)
	for _, t := range p.tables {
		a, _, err := one(p.src.Rows(ctx, "SELECT count(*)::text FROM "+t.ident()))
		if err != nil {
			return st, err
		}
		// A table published since the sync started may not be on the
		// target, and counting its rows there is an error that says
		// nothing: it's a difference, with the finding a plan gives it.
		reg, _, err := one(p.dst.Rows(ctx, "SELECT coalesce(to_regclass($1)::text, '')", t.ident()))
		if err != nil {
			return st, err
		}
		if reg == "" {
			different++
			if !slices.Contains(st.Findings, findingSchema) {
				st.Findings = append(st.Findings, findingSchema)
			}
			continue
		}
		b, _, err := one(p.dst.Rows(ctx, "SELECT count(*)::text FROM "+t.ident()))
		if err != nil {
			return st, err
		}
		if a != b {
			different++
		}
	}
	st.Extra = different
	st.Verified = len(p.tables) > 0 && st.Pending == 0 && st.LagBytes == 0 && different == 0 && len(st.Findings) == 0
	st.Copied = 0
	return st, nil
}

// Stop disables the subscription at the end of an approval. The source's
// slot keeps its place, and keeps WAL on the source until the next approval
// enables it again or the customer drops the slot.
func (p *pgSet) Stop(ctx context.Context) error {
	err := p.stop(ctx)
	if err == nil || p.redial == nil || ctx.Err() != nil {
		return err
	}
	// A sync holds this connection for hours, and one that dropped
	// leaves the subscription writing on its own. Dial once more.
	dst, derr := p.redial(ctx)
	if derr != nil {
		return err
	}
	p.dst.Close()
	p.dst = dst
	return p.stop(ctx)
}

func (p *pgSet) stop(ctx context.Context) error {
	_, exists, err := one(p.dst.Rows(ctx, "SELECT subname::text FROM pg_subscription WHERE subname = $1 AND "+thisDatabase, p.sub))
	if err != nil || !exists {
		return err
	}
	return p.dst.Exec(ctx, "ALTER SUBSCRIPTION "+pgwire.Ident(p.sub)+" DISABLE")
}

// nothingPending reports whether the slot holds no change the publication
// sends past what the target confirmed. It says false whenever it can't
// tell: on a server before Postgres 15, whose slot answers with every empty
// transaction, while a walsender has the slot, and when the source's role
// may not read it.
//
// The peek reads the slot and moves nothing in it. The source decodes its
// WAL to answer, up to the first transaction that has a change.
func (p *pgSet) nothingPending(ctx context.Context, active bool) bool {
	version, _, err := one(p.src.Rows(ctx, "SELECT current_setting('server_version_num')"))
	if n, _ := strconv.Atoi(version); err != nil || n < 150000 {
		return false
	}
	// A subscription just stopped lets go of the slot within a moment.
	for tries := 0; active && tries < 20; tries++ {
		enabled, exists, err := one(p.dst.Rows(ctx, "SELECT subenabled::text FROM pg_subscription WHERE subname = $1 AND "+thisDatabase, p.sub))
		if err != nil || exists && enabled == "true" {
			return false
		}
		if p.pause(ctx) != nil {
			return false
		}
		got, ok, err := one(p.src.Rows(ctx, "SELECT active::text FROM pg_replication_slots WHERE slot_name = $1", p.slot))
		if err != nil || !ok {
			return false
		}
		active = got == "true"
	}
	if active {
		return false
	}
	peek := "SELECT count(*)::text FROM pg_logical_slot_peek_binary_changes($1, NULL, 1, 'proto_version', '1', 'publication_names', $2)"
	if p.twoWay {
		// What the other direction brought here isn't this one's to send.
		peek = "SELECT count(*)::text FROM pg_logical_slot_peek_binary_changes($1, NULL, 1, 'proto_version', '1', 'publication_names', $2, 'origin', 'none')"
	}
	n, ok, err := one(p.src.Rows(ctx, peek, p.slot, pgwire.Ident(p.publication)))
	return err == nil && ok && n == "0"
}

// pause waits half a second, or less in a test.
func (p *pgSet) pause(ctx context.Context) error {
	if p.sleep != nil {
		return p.sleep(ctx)
	}
	t := time.NewTimer(500 * time.Millisecond)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Position says where the source's published tables and its sequences are:
// the rows written to the tables since the server's counters began, and the
// sequences' values. It stays put while nothing writes to them, whatever
// else the server writes. A transaction that has written to a published
// table and hasn't committed isn't in the counters yet, so while one is
// open the position is a new one every time it is read.
//
// The counters are the server's statistics. A session reports its writes
// within a second or so of a commit, ten when it goes idle, so a write
// that recent can be missing here. The check of the data that follows a
// cutover's wait reads the slot, which has every commit.
func (p *pgSet) Position(ctx context.Context) (string, error) {
	pos, _, err := one(p.src.Rows(ctx, `SELECT 'writes ' || coalesce((SELECT sum(s.n_tup_ins + s.n_tup_upd + s.n_tup_del)
  FROM pg_publication_tables t JOIN pg_stat_all_tables s ON s.schemaname = t.schemaname AND s.relname = t.tablename
  WHERE t.pubname = $1), 0)::text
|| ' sequences ' || coalesce((SELECT md5(string_agg(schemaname || '.' || sequencename || '=' || coalesce(last_value::text, ''), ',' ORDER BY schemaname, sequencename))
  FROM pg_sequences), '')
|| CASE WHEN EXISTS (SELECT 1 FROM pg_locks l
  JOIN pg_class c ON c.oid = l.relation JOIN pg_namespace n ON n.oid = c.relnamespace
  JOIN pg_publication_tables t ON t.schemaname = n.nspname AND t.tablename = c.relname
  WHERE t.pubname = $1 AND l.locktype = 'relation' AND l.mode = 'RowExclusiveLock' AND l.granted AND l.pid <> pg_backend_pid()
  AND l.database = (SELECT oid FROM pg_database WHERE datname = current_database()))
 THEN ' open ' || clock_timestamp()::text ELSE '' END`, p.publication))
	return pos, err
}

// Sequences sets each target sequence the source also has to the source's
// value. Sequences don't replicate, so a cutover runs this once the
// source's writes have stopped. It returns how many it set.
func (p *pgSet) Sequences(ctx context.Context) (int, error) {
	hidden, err := p.src.Rows(ctx, hiddenSequences)
	if err != nil {
		return 0, err
	}
	for _, r := range hidden {
		if len(r) != 2 {
			return 0, ErrPostgres
		}
		reg, _, err := one(p.dst.Rows(ctx, "SELECT coalesce(to_regclass($1)::text, '')", pgwire.Ident(r[0], r[1])))
		if err != nil {
			return 0, err
		}
		if reg != "" {
			return 0, ErrSequence
		}
	}
	rows, err := p.src.Rows(ctx, "SELECT schemaname::text, sequencename::text, last_value::text FROM pg_sequences WHERE last_value IS NOT NULL ORDER BY 1, 2")
	if err != nil {
		return 0, err
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i][0]+"."+rows[i][1] < rows[j][0]+"."+rows[j][1] })
	n := 0
	for _, r := range rows {
		v, err := strconv.ParseInt(r[2], 10, 64)
		if err != nil {
			return n, ErrPostgres
		}
		seq := pgwire.Ident(r[0], r[1])
		reg, _, err := one(p.dst.Rows(ctx, "SELECT coalesce(to_regclass($1)::text, '')", seq))
		if err != nil {
			return n, err
		}
		if reg == "" {
			continue
		}
		if _, _, err := one(p.dst.Rows(ctx, "SELECT setval($1::regclass, $2)::text", seq, v)); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}
