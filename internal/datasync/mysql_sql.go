package datasync

// The mysql dataset's two sides as real servers: go-sql-driver/mysql for
// queries and go-mysql's replication package for the binary log. Every
// statement is built here, and nothing a server says goes into an error.

import (
	"context"
	"crypto/tls"
	"database/sql"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-sql-driver/mysql"
)

func init() {
	// The driver's own log can hold a server's address; nothing here
	// writes one.
	_ = mysql.SetLogger(&mysql.NopLogger{})
}

// myURL is a MySQL URL taken apart: mysql://user:password@host:port/database?tls=true.
// tls is true (the server's certificate checked against the system's
// roots) or skip-verify (encrypted, not checked), never off.
type myURL struct {
	host     string
	port     uint16
	user     string
	password string
	db       string
	tls      *tls.Config
}

var charsetPattern = regexp.MustCompile(`^[a-z0-9_]{1,32}$`)

func parseMyURL(raw string) (myURL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "mysql" || u.User == nil || u.User.Username() == "" || u.Hostname() == "" || u.Fragment != "" {
		return myURL{}, ErrMove
	}
	db := strings.TrimPrefix(u.Path, "/")
	if db == "" || strings.Contains(db, "/") {
		return myURL{}, ErrMove
	}
	port := uint64(3306)
	if p := u.Port(); p != "" {
		if port, err = strconv.ParseUint(p, 10, 16); err != nil || port == 0 {
			return myURL{}, ErrMove
		}
	}
	q := u.Query()
	for k := range q {
		if k != "tls" {
			return myURL{}, ErrMove
		}
	}
	c := &tls.Config{ServerName: u.Hostname(), MinVersion: tls.VersionTLS12}
	switch q.Get("tls") {
	case "true":
	case "skip-verify":
		c.InsecureSkipVerify = true
	default:
		// A URL that could fall back to plaintext is refused, as a
		// Postgres one is.
		return myURL{}, ErrMove
	}
	pw, _ := u.User.Password()
	return myURL{host: u.Hostname(), port: uint16(port), user: u.User.Username(), password: pw, db: db, tls: c}, nil
}

// connect opens a pool on one side. Every session is in UTC, gives
// character columns back as the bytes stored, and on the source is
// read-only. On the target it checks no foreign keys, so tables fill in
// any order, and takes a row's 0 in an AUTO_INCREMENT column as 0.
func (m myURL) connect(source bool) (*sql.DB, error) {
	cfg := mysql.NewConfig()
	cfg.User, cfg.Passwd, cfg.Net = m.user, m.password, "tcp"
	cfg.Addr = net.JoinHostPort(m.host, strconv.Itoa(int(m.port)))
	cfg.DBName = m.db
	cfg.TLS = m.tls.Clone()
	cfg.AllowNativePasswords = true
	cfg.InterpolateParams = true
	cfg.Loc = time.UTC
	cfg.Timeout = 15 * time.Second
	cfg.Logger = &mysql.NopLogger{}
	cfg.Params = map[string]string{
		"time_zone":             "'+00:00'",
		"character_set_results": "binary",
	}
	if source {
		cfg.Params["transaction_read_only"] = "1"
	} else {
		cfg.Params["foreign_key_checks"] = "0"
		cfg.Params["sql_mode"] = "'NO_AUTO_VALUE_ON_ZERO,ALLOW_INVALID_DATES'"
	}
	conn, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, ErrMove
	}
	db := sql.OpenDB(conn)
	db.SetMaxOpenConns(4)
	db.SetConnMaxIdleTime(time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if db.PingContext(ctx) != nil {
		db.Close()
		return nil, ErrMySQL
	}
	return db, nil
}

func openMySQL(d Dataset, o Options) (dataset, error) {
	from, to := o.Env(d.FromEnv), o.Env(d.ToEnv)
	if from == "" || to == "" || from == to {
		return nil, ErrMove
	}
	fu, err := parseMyURL(from)
	if err != nil {
		return nil, err
	}
	tu, err := parseMyURL(to)
	if err != nil {
		return nil, err
	}
	src, err := fu.connect(true)
	if err != nil {
		return nil, err
	}
	dst, err := tu.connect(false)
	if err != nil {
		src.Close()
		return nil, err
	}
	now := o.Now
	if now == nil {
		now = time.Now
	}
	return &mysqlSet{
		name: d.Name, db: fu.db, copySchema: d.CopySchema, w: o.Workspace,
		src: &mySQLSource{mySQL{db: src}, fu}, dst: &mySQLTarget{mySQL{db: dst}},
		settle: time.Second, now: now,
	}, nil
}

// quoteIdent quotes a name for MySQL.
func quoteIdent(name string) string { return "`" + strings.ReplaceAll(name, "`", "``") + "`" }

// mySQL is either side's reads.
type mySQL struct{ db *sql.DB }

type sqlRows struct {
	rows *sql.Rows
	cell []sql.RawBytes
	ptrs []any
}

func query(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, stmt string, args ...any) (*sqlRows, error) {
	rows, err := q.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, ErrMySQL
	}
	cols, err := rows.Columns()
	if err != nil {
		rows.Close()
		return nil, ErrMySQL
	}
	r := &sqlRows{rows: rows, cell: make([]sql.RawBytes, len(cols)), ptrs: make([]any, len(cols))}
	for i := range r.cell {
		r.ptrs[i] = &r.cell[i]
	}
	return r, nil
}

func (r *sqlRows) Next() ([]any, error) {
	if !r.rows.Next() {
		if r.rows.Err() != nil {
			return nil, ErrMySQL
		}
		return nil, io.EOF
	}
	if r.rows.Scan(r.ptrs...) != nil {
		return nil, ErrMySQL
	}
	out := make([]any, len(r.cell))
	for i, c := range r.cell {
		if c != nil {
			out[i] = append([]byte{}, c...)
		}
	}
	return out, nil
}

func (r *sqlRows) Close() { r.rows.Close() }

// strings reads every row as text.
func (m mySQL) strings(ctx context.Context, stmt string, args ...any) ([][]string, error) {
	r, err := query(ctx, m.db, stmt, args...)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	var out [][]string
	for {
		row, err := r.Next()
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		s := make([]string, len(row))
		for i, c := range row {
			s[i] = cellString(c)
		}
		out = append(out, s)
	}
}

func (m mySQL) Close() { m.db.Close() }

func (m mySQL) Vars(ctx context.Context, names ...string) (map[string]string, error) {
	if len(names) == 0 {
		return map[string]string{}, nil
	}
	args := make([]any, len(names))
	for i, n := range names {
		args[i] = n
	}
	rows, err := m.strings(ctx, "SHOW VARIABLES WHERE Variable_name IN (?"+strings.Repeat(", ?", len(names)-1)+")", args...)
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for _, r := range rows {
		if len(r) == 2 {
			out[r[0]] = r[1]
		}
	}
	return out, nil
}

func (m mySQL) Identity(ctx context.Context) (string, error) {
	v, err := m.Vars(ctx, "server_uuid", "server_id", "hostname", "port")
	if err != nil {
		return "", err
	}
	db, err := m.strings(ctx, "SELECT DATABASE()")
	if err != nil || len(db) != 1 || len(db[0]) != 1 {
		return "", ErrMySQL
	}
	return hashID(v["server_uuid"], v["server_id"], v["hostname"], v["port"], db[0][0]), nil
}

const myColumnsSQL = `SELECT c.TABLE_NAME, c.COLUMN_NAME, c.DATA_TYPE, c.COLUMN_TYPE, COALESCE(c.CHARACTER_SET_NAME, ''), c.EXTRA
FROM information_schema.COLUMNS c JOIN information_schema.TABLES t ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME
WHERE c.TABLE_SCHEMA = DATABASE() AND t.TABLE_TYPE = 'BASE TABLE' ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION`

const myKeysSQL = `SELECT TABLE_NAME, COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE() AND CONSTRAINT_NAME = 'PRIMARY' ORDER BY TABLE_NAME, ORDINAL_POSITION`

func (m mySQL) Tables(ctx context.Context) ([]myTable, error) {
	cols, err := m.strings(ctx, myColumnsSQL)
	if err != nil {
		return nil, err
	}
	var out []myTable
	for _, r := range cols {
		if len(r) != 6 {
			return nil, ErrMySQL
		}
		if len(out) == 0 || out[len(out)-1].Name != r[0] {
			out = append(out, myTable{Name: r[0]})
		}
		t := &out[len(out)-1]
		cs := r[4]
		if cs != "" && !charsetPattern.MatchString(cs) {
			return nil, ErrMySQL
		}
		t.Columns = append(t.Columns, myColumn{
			Name: r[1], Type: strings.ToLower(r[2]), Charset: cs,
			Unsigned:  strings.Contains(strings.ToLower(r[3]), "unsigned"),
			Generated: strings.Contains(strings.ToUpper(r[5]), "GENERATED"),
		})
	}
	keys, err := m.strings(ctx, myKeysSQL)
	if err != nil {
		return nil, err
	}
	for _, r := range keys {
		if len(r) != 2 {
			return nil, ErrMySQL
		}
		for i := range out {
			if out[i].Name != r[0] {
				continue
			}
			for j, c := range out[i].Columns {
				if c.Name == r[1] {
					out[i].Key = append(out[i].Key, j)
				}
			}
		}
	}
	return out, nil
}

func (m mySQL) Empty(ctx context.Context, t myTable) (bool, error) {
	rows, err := m.strings(ctx, "SELECT 1 FROM "+quoteIdent(t.Name)+" LIMIT 1")
	return len(rows) == 0, err
}

func (m mySQL) Sum(ctx context.Context, t myTable) (mySum, error) {
	rows, err := m.strings(ctx, mySumSQL(t))
	if err != nil {
		return mySum{}, err
	}
	if len(rows) != 1 || len(rows[0]) != 3 {
		return mySum{}, ErrMySQL
	}
	n, err := strconv.ParseInt(rows[0][0], 10, 64)
	if err != nil {
		return mySum{}, ErrMySQL
	}
	return mySum{Rows: n, Hash: rows[0][1] + ":" + rows[0][2]}, nil
}

// mySumSQL counts a table's rows and folds an MD5 of each into two 64-bit
// XORs, so the order rows come in doesn't matter and no row leaves the
// server. Each column is read as the bytes it holds, a JSON one as its text,
// and which are NULL is part of the row.
func mySumSQL(t myTable) string {
	var vals, nulls []string
	for _, c := range t.Columns {
		q := quoteIdent(c.Name)
		if c.Type == "json" {
			vals = append(vals, "CAST("+q+" AS CHAR)")
		} else {
			vals = append(vals, "CAST("+q+" AS BINARY)")
		}
		nulls = append(nulls, "ISNULL("+q+")")
	}
	row := "MD5(CONCAT_WS(0x1f, " + strings.Join(vals, ", ") + ", CONCAT(" + strings.Join(nulls, ", ") + ")))"
	return "SELECT COUNT(*), COALESCE(BIT_XOR(CAST(CONV(SUBSTRING(h, 1, 16), 16, 10) AS UNSIGNED)), 0), " +
		"COALESCE(BIT_XOR(CAST(CONV(SUBSTRING(h, 17, 16), 16, 10) AS UNSIGNED)), 0) FROM (SELECT " + row + " AS h FROM " + quoteIdent(t.Name) + ") AS zoctl_rows"
}

// mySQLSource is the source: reads only.
type mySQLSource struct {
	mySQL
	url myURL
}

func (m *mySQLSource) Position(ctx context.Context) (myPos, error) {
	// 8.4 renamed it; older servers and MariaDB know the old name.
	rows, err := m.strings(ctx, "SHOW BINARY LOG STATUS")
	if err != nil {
		rows, err = m.strings(ctx, "SHOW MASTER STATUS")
	}
	if err != nil || len(rows) != 1 || len(rows[0]) < 2 {
		return myPos{}, ErrMySQL
	}
	p, err := strconv.ParseUint(rows[0][1], 10, 32)
	if err != nil {
		return myPos{}, ErrMySQL
	}
	return myPos{File: rows[0][0], Pos: uint32(p)}, nil
}

func (m *mySQLSource) Logs(ctx context.Context) ([]myLog, error) {
	rows, err := m.strings(ctx, "SHOW BINARY LOGS")
	if err != nil {
		return nil, err
	}
	var out []myLog
	for _, r := range rows {
		if len(r) < 2 {
			return nil, ErrMySQL
		}
		n, err := strconv.ParseInt(r[1], 10, 64)
		if err != nil {
			return nil, ErrMySQL
		}
		out = append(out, myLog{File: r[0], Size: n})
	}
	return out, nil
}

func (m *mySQLSource) CreateTable(ctx context.Context, t myTable) (string, error) {
	rows, err := m.strings(ctx, "SHOW CREATE TABLE "+quoteIdent(t.Name))
	if err != nil || len(rows) != 1 || len(rows[0]) < 2 || !strings.HasPrefix(rows[0][1], "CREATE TABLE ") {
		return "", ErrMySQL
	}
	return rows[0][1], nil
}

type sqlSnapshot struct{ conn *sql.Conn }

// Snapshot opens a transaction on one connection that sees the source as
// it was when it began, for every table.
func (m *mySQLSource) Snapshot(ctx context.Context) (mySnapshot, error) {
	conn, err := m.db.Conn(ctx)
	if err != nil {
		return nil, ErrMySQL
	}
	for _, stmt := range []string{"SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ", "START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY"} {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			conn.Close()
			return nil, ErrMySQL
		}
	}
	return sqlSnapshot{conn}, nil
}

func (s sqlSnapshot) Rows(ctx context.Context, t myTable) (myRows, error) {
	r, err := query(ctx, s.conn, "SELECT "+columnList(t)+" FROM "+quoteIdent(t.Name))
	if err != nil {
		return nil, err
	}
	return writtenOnly{r, t}, nil
}

func (s sqlSnapshot) Close() {
	_, _ = s.conn.ExecContext(context.Background(), "ROLLBACK")
	s.conn.Close()
}

// writtenOnly gives a snapshot's rows the table's full width, with nil
// where a generated column is, which is never written: the copy's rows and
// the log's then line up.
type writtenOnly struct {
	r *sqlRows
	t myTable
}

func (w writtenOnly) Next() ([]any, error) {
	row, err := w.r.Next()
	if err != nil {
		return nil, err
	}
	out := make([]any, len(w.t.Columns))
	j := 0
	for i, c := range w.t.Columns {
		if c.Generated {
			continue
		}
		if j >= len(row) {
			return nil, ErrMySQL
		}
		out[i] = row[j]
		j++
	}
	return out, nil
}

func (w writtenOnly) Close() { w.r.Close() }

// columnList names the columns a write sets: all but generated ones.
func columnList(t myTable) string {
	var names []string
	for _, c := range t.Columns {
		if !c.Generated {
			names = append(names, quoteIdent(c.Name))
		}
	}
	return strings.Join(names, ", ")
}

// myArg is how one cell goes into a statement. Bytes for a character
// column are the bytes stored in its character set (the source's sessions
// read them that way, and the log has them so), so they're relabelled with
// it rather than converted. A JSON column takes text. A whole number from
// the log comes signed and is made unsigned again where the column is.
func myArg(c myColumn, v any) (string, any, error) {
	switch x := v.(type) {
	case nil:
		return "?", nil, nil
	case string:
		return myArg(c, []byte(x))
	case []byte:
		switch {
		case c.Type == "json":
			return "?", string(x), nil
		case c.Charset != "" && c.Charset != "binary":
			return "CONVERT(? USING " + c.Charset + ")", x, nil
		}
		return "?", x, nil
	case int8:
		return "?", whole(c, int64(x), 8), nil
	case int16:
		return "?", whole(c, int64(x), 16), nil
	case int32:
		bits := 32
		if c.Type == "mediumint" {
			bits = 24
		}
		return "?", whole(c, int64(x), bits), nil
	case int64:
		return "?", whole(c, x, 64), nil
	case int:
		return "?", whole(c, int64(x), 64), nil
	case uint8:
		return "?", uint64(x), nil
	case uint16:
		return "?", uint64(x), nil
	case uint32:
		return "?", uint64(x), nil
	case uint64:
		return "?", x, nil
	case float32:
		return "?", strconv.FormatFloat(float64(x), 'g', -1, 32), nil
	case float64:
		return "?", x, nil
	case interface{ String() string }:
		// A DECIMAL from the log, exactly.
		return "?", x.String(), nil
	}
	return "", nil, ErrMySQL
}

func whole(c myColumn, v int64, bits int) any {
	if c.Type == "bit" {
		return uint64(v)
	}
	if !c.Unsigned || v >= 0 {
		return v
	}
	if bits == 64 {
		return uint64(v)
	}
	return uint64(v) & (1<<bits - 1)
}

// strconvAny is a number from the log as text.
func strconvAny(v any) string {
	_, a, err := myArg(myColumn{}, v)
	if err != nil {
		return ""
	}
	switch x := a.(type) {
	case int64:
		return strconv.FormatInt(x, 10)
	case uint64:
		return strconv.FormatUint(x, 10)
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	case string:
		return x
	}
	return ""
}

// rowsSQL is an INSERT or REPLACE of rows, generated columns left out.
func rowsSQL(verb string, t myTable, rows [][]any) (string, []any, error) {
	var b strings.Builder
	b.WriteString(verb + " INTO " + quoteIdent(t.Name) + " (" + columnList(t) + ") VALUES ")
	var args []any
	for i, r := range rows {
		if len(r) != len(t.Columns) {
			return "", nil, ErrMySQL
		}
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteByte('(')
		first := true
		for j, c := range t.Columns {
			if c.Generated {
				continue
			}
			p, a, err := myArg(c, r[j])
			if err != nil {
				return "", nil, err
			}
			if !first {
				b.WriteString(", ")
			}
			first = false
			b.WriteString(p)
			args = append(args, a)
		}
		b.WriteByte(')')
	}
	return b.String(), args, nil
}

// deleteSQL removes rows by their keys.
func deleteSQL(t myTable, rows [][]any) (string, []any, error) {
	if len(t.Key) == 0 {
		return "", nil, ErrMySQL
	}
	var b strings.Builder
	b.WriteString("DELETE FROM " + quoteIdent(t.Name) + " WHERE ")
	var args []any
	for i, r := range rows {
		if len(r) != len(t.Columns) {
			return "", nil, ErrMySQL
		}
		if i > 0 {
			b.WriteString(" OR ")
		}
		b.WriteByte('(')
		for k, j := range t.Key {
			p, a, err := myArg(t.Columns[j], r[j])
			if err != nil {
				return "", nil, err
			}
			if k > 0 {
				b.WriteString(" AND ")
			}
			b.WriteString(quoteIdent(t.Columns[j].Name) + " = " + p)
			args = append(args, a)
		}
		b.WriteByte(')')
	}
	return b.String(), args, nil
}

// mySQLTarget is the target.
type mySQLTarget struct{ mySQL }

func (m *mySQLTarget) Create(ctx context.Context, stmt string) error {
	if _, err := m.db.ExecContext(ctx, stmt); err != nil {
		return ErrMySQL
	}
	return nil
}

func (m *mySQLTarget) Insert(ctx context.Context, t myTable, rows [][]any) error {
	stmt, args, err := rowsSQL("INSERT", t, rows)
	if err != nil {
		return err
	}
	if _, err := m.db.ExecContext(ctx, stmt, args...); err != nil {
		return ErrMySQL
	}
	return nil
}

type sqlTx struct{ tx *sql.Tx }

func (m *mySQLTarget) Begin(ctx context.Context) (myTx, error) {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, ErrMySQL
	}
	return sqlTx{tx}, nil
}

func (t sqlTx) exec(ctx context.Context, stmt string, args []any, err error) error {
	if err != nil {
		return err
	}
	if _, err := t.tx.ExecContext(ctx, stmt, args...); err != nil {
		return ErrMySQL
	}
	return nil
}

func (t sqlTx) Replace(ctx context.Context, tb myTable, rows [][]any) error {
	stmt, args, err := rowsSQL("REPLACE", tb, rows)
	return t.exec(ctx, stmt, args, err)
}

func (t sqlTx) Delete(ctx context.Context, tb myTable, rows [][]any) error {
	stmt, args, err := deleteSQL(tb, rows)
	return t.exec(ctx, stmt, args, err)
}

func (t sqlTx) Commit(context.Context) error {
	if t.tx.Commit() != nil {
		return ErrMySQL
	}
	return nil
}

func (t sqlTx) Rollback() { _ = t.tx.Rollback() }

// goBinlog is the source's log through go-mysql's replication client,
// which connects as a replica with a server ID of its own.
type goBinlog struct {
	syncer *replication.BinlogSyncer
	st     *replication.BinlogStreamer
	file   string
	db     string
}

func (m *mySQLSource) Binlog(ctx context.Context, from myPos) (myBinlog, error) {
	v, err := m.Vars(ctx, "version")
	if err != nil {
		return nil, err
	}
	flavor := gomysql.MySQLFlavor
	if strings.Contains(strings.ToLower(v["version"]), "mariadb") {
		flavor = gomysql.MariaDBFlavor
	}
	syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		// A replica's ID has to differ from every other one's on the
		// source; one drawn from this range almost surely does.
		ServerID: 1<<24 + rand.Uint32N(1<<30),
		Flavor:   flavor,
		Host:     m.url.host, Port: m.url.port, User: m.url.user, Password: m.url.password,
		TLSConfig:               m.url.tls.Clone(),
		Logger:                  slog.New(slog.DiscardHandler),
		UseDecimal:              true,
		TimestampStringLocation: time.UTC,
		HeartbeatPeriod:         10 * time.Second,
		ReadTimeout:             time.Minute,
		MaxReconnectAttempts:    3,
	})
	st, err := syncer.StartSync(gomysql.Position{Name: from.File, Pos: from.Pos})
	if err != nil {
		syncer.Close()
		return nil, ErrMySQL
	}
	return &goBinlog{syncer: syncer, st: st, file: from.File, db: m.url.db}, nil
}

func (b *goBinlog) Close() { b.syncer.Close() }

// ddl reports whether a statement may change one of the database's tables:
// a TRUNCATE the log has as a statement, or anything that changes a
// table's columns. It errs towards yes; the stream then stops for a new plan.
func (b *goBinlog) ddl(ev *replication.QueryEvent) bool {
	q := strings.ToUpper(strings.TrimSpace(string(ev.Query)))
	for _, verb := range []string{"ALTER ", "CREATE ", "DROP ", "RENAME ", "TRUNCATE "} {
		if strings.HasPrefix(q, verb) {
			return string(ev.Schema) == b.db || strings.Contains(string(ev.Query), b.db)
		}
	}
	return false
}

func (b *goBinlog) Next(ctx context.Context) (myEvent, error) {
	for {
		e, err := b.st.GetEvent(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return myEvent{}, ctx.Err()
			}
			return myEvent{}, ErrMySQL
		}
		h := e.Header
		at := time.Unix(int64(h.Timestamp), 0)
		next := myPos{File: b.file, Pos: h.LogPos}
		switch ev := e.Event.(type) {
		case *replication.RotateEvent:
			b.file = string(ev.NextLogName)
			return myEvent{Kind: evOther, Next: myPos{File: b.file, Pos: uint32(ev.Position)}}, nil
		case *replication.XIDEvent:
			return myEvent{Kind: evCommit, Next: next, Time: at}, nil
		case *replication.QueryEvent:
			if h.LogPos == 0 {
				continue
			}
			switch strings.ToUpper(strings.TrimSpace(string(ev.Query))) {
			case "BEGIN":
				return myEvent{Kind: evBegin, Next: next, Time: at}, nil
			case "COMMIT":
				return myEvent{Kind: evCommit, Next: next, Time: at}, nil
			}
			return myEvent{Kind: evOther, Next: next, Time: at, DDL: b.ddl(ev)}, nil
		case *replication.RowsEvent:
			var op myOp
			switch ev.Type() {
			case replication.EnumRowsEventTypeInsert:
				op = opInsert
			case replication.EnumRowsEventTypeUpdate:
				op = opUpdate
			case replication.EnumRowsEventTypeDelete:
				op = opDelete
			default:
				continue
			}
			return myEvent{Kind: evRows, Op: op, Schema: string(ev.Table.Schema), Table: string(ev.Table.Table),
				Columns: int(ev.Table.ColumnCount), Rows: ev.Rows, Next: next, Time: at}, nil
		}
		if h.LogPos > 0 {
			return myEvent{Kind: evOther, Next: next}, nil
		}
	}
}

// The real sides are the interfaces', checked here rather than at run time.
var (
	_ mySource = (*mySQLSource)(nil)
	_ myTarget = (*mySQLTarget)(nil)
)
