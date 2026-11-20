package datasync

import (
	"context"
	"regexp"
	"strconv"
	"strings"

	"github.com/zoctl/cli/internal/pgwire"
)

// copy_schema makes the published tables the target lacks from the
// source's catalog, in one transaction on the target: the tables with their
// columns, NOT NULL, primary keys, identity and generated columns and the
// sequences their columns own, then each one's defaults, unique, check and
// exclusion constraints and indexes, then the foreign keys between them.
// Types, extensions, views, functions, triggers, policies and grants stay
// the customer's (findingTablesOnly). A table or a sequence that can't be
// made rolls the whole of it back; a default, constraint or index that
// can't is left out on its own and counted (findingSchemaLeft).
//
// The statements are the server's own deparsed text (pg_get_expr,
// pg_get_constraintdef, pg_get_indexdef), read with only pg_catalog on the
// search path so every other name in them comes qualified, and each has to
// be one statement (oneStatement) before it goes to the target.

// typePattern is what a column type from the source's format_type may be
// before it goes into a CREATE TABLE.
var typePattern = regexp.MustCompile(`^[A-Za-z0-9_ ,()\[\]".]{1,128}$`)

// tableDDL is what copy_schema makes for one table.
type tableDDL struct {
	schemas []string // schemas its sequences are in
	before  []string // its sequences: made before it, or nothing is
	create  string
	after   []string // ownership, defaults, constraints and indexes: each kept or left
	keys    []string // foreign keys, once every table is there
}

// ownedSeq is a sequence one of a table's columns owns: a serial's (deptype
// a) or an identity column's (i).
type ownedSeq struct {
	schema, name, typ, column, kind string
	options                         string
}

func (s ownedSeq) ident() string { return pgwire.Ident(s.schema, s.name) }

var seqTypes = map[string]bool{"smallint": true, "integer": true, "bigint": true}

// ownedSequences reads the sequences t's columns own on the source.
func (p *pgSet) ownedSequences(ctx context.Context, t table) ([]ownedSeq, error) {
	rows, err := p.src.Rows(ctx, `SELECT n.nspname::text, c.relname::text, format_type(s.seqtypid, NULL), s.seqstart::text, s.seqincrement::text,
s.seqmin::text, s.seqmax::text, s.seqcache::text, s.seqcycle::text, a.attname::text, d.deptype::text
FROM pg_depend d JOIN pg_class c ON c.oid = d.objid JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_sequence s ON s.seqrelid = c.oid JOIN pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
WHERE d.classid = 'pg_class'::regclass AND d.refclassid = 'pg_class'::regclass AND d.refobjid = to_regclass($1)
AND d.deptype IN ('a', 'i') AND c.relkind = 'S' ORDER BY 2`, t.ident())
	if err != nil {
		return nil, err
	}
	var out []ownedSeq
	for _, r := range rows {
		if len(r) != 11 || !seqTypes[r[2]] {
			return nil, ErrPostgres
		}
		for _, n := range r[3:8] {
			if _, err := strconv.ParseInt(n, 10, 64); err != nil {
				return nil, ErrPostgres
			}
		}
		options := "INCREMENT BY " + r[4] + " MINVALUE " + r[5] + " MAXVALUE " + r[6] + " START WITH " + r[3] + " CACHE " + r[7]
		if r[8] == "true" {
			options += " CYCLE"
		} else {
			options += " NO CYCLE"
		}
		out = append(out, ownedSeq{schema: r[0], name: r[1], typ: r[2], column: r[9], kind: r[10], options: options})
	}
	return out, nil
}

// createTable reads what copy_schema makes for one published table.
func (p *pgSet) createTable(ctx context.Context, t table) (tableDDL, error) {
	var ddl tableDDL
	cols, err := p.src.Rows(ctx, `SELECT a.attname::text, format_type(a.atttypid, a.atttypmod), a.attnotnull::text, a.attidentity::text,
a.attgenerated::text, coalesce(pg_get_expr(d.adbin, d.adrelid), '')
FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
WHERE a.attrelid = to_regclass($1) AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum`, t.ident())
	if err != nil {
		return ddl, err
	}
	if len(cols) == 0 {
		return ddl, ErrPostgres
	}
	pk, err := p.src.Rows(ctx, `SELECT a.attname::text FROM pg_index i
JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
WHERE i.indrelid = to_regclass($1) AND i.indisprimary ORDER BY array_position(i.indkey, a.attnum)`, t.ident())
	if err != nil {
		return ddl, err
	}
	owned, err := p.ownedSequences(ctx, t)
	if err != nil {
		return ddl, err
	}
	seqs := map[string]ownedSeq{}
	for _, s := range owned {
		seqs[s.column] = s
		if s.kind != "a" {
			continue
		}
		ddl.schemas = append(ddl.schemas, s.schema)
		ddl.before = append(ddl.before, "CREATE SEQUENCE IF NOT EXISTS "+s.ident()+" AS "+s.typ+" "+s.options)
		ddl.after = append(ddl.after, "ALTER SEQUENCE "+s.ident()+" OWNED BY "+t.ident()+"."+pgwire.Ident(s.column))
	}
	var parts []string
	for _, c := range cols {
		if len(c) != 6 || !typePattern.MatchString(c[1]) || strings.Contains(c[1], "--") {
			return ddl, ErrPostgres
		}
		name := pgwire.Ident(c[0])
		col := name + " " + c[1]
		switch identity, generated, expr := c[3], c[4], c[5]; {
		case generated == "s" || generated == "v":
			// A generated column isn't replicated: the target works it
			// out, so it has to be one there too.
			if !oneStatement(expr) {
				return ddl, ErrPostgres
			}
			col += " GENERATED ALWAYS AS (" + expr + ")"
			if generated == "s" {
				col += " STORED"
			}
		case identity == "a" || identity == "d":
			if identity == "a" {
				col += " GENERATED ALWAYS AS IDENTITY"
			} else {
				col += " GENERATED BY DEFAULT AS IDENTITY"
			}
			if s, ok := seqs[c[0]]; ok && s.kind == "i" {
				col += " (SEQUENCE NAME " + s.ident() + " " + s.options + ")"
			}
		case expr != "":
			ddl.after = append(ddl.after, "ALTER TABLE "+t.ident()+" ALTER COLUMN "+name+" SET DEFAULT "+expr)
		}
		if c[2] == "true" {
			col += " NOT NULL"
		}
		parts = append(parts, col)
	}
	if len(pk) > 0 {
		var names []string
		for _, k := range pk {
			names = append(names, pgwire.Ident(k[0]))
		}
		parts = append(parts, "PRIMARY KEY ("+strings.Join(names, ", ")+")")
	}
	ddl.create = "CREATE TABLE " + t.ident() + " (\n  " + strings.Join(parts, ",\n  ") + "\n)"

	cons, err := p.src.Rows(ctx, `SELECT conname::text, contype::text, pg_get_constraintdef(oid) FROM pg_constraint
WHERE conrelid = to_regclass($1) AND contype IN ('c', 'u', 'x', 'f') ORDER BY 1`, t.ident())
	if err != nil {
		return ddl, err
	}
	for _, c := range cons {
		if len(c) != 3 {
			return ddl, ErrPostgres
		}
		add := "ALTER TABLE " + t.ident() + " ADD CONSTRAINT " + pgwire.Ident(c[0]) + " " + c[2]
		if c[1] == "f" {
			ddl.keys = append(ddl.keys, add)
		} else {
			ddl.after = append(ddl.after, add)
		}
	}
	// A unique or exclusion constraint's index comes with it.
	indexes, err := p.src.Rows(ctx, `SELECT pg_get_indexdef(i.indexrelid) FROM pg_index i
WHERE i.indrelid = to_regclass($1) AND NOT i.indisprimary
AND NOT EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conindid = i.indexrelid AND c.conrelid = i.indrelid) ORDER BY 1`, t.ident())
	if err != nil {
		return ddl, err
	}
	for _, ix := range indexes {
		if len(ix) != 1 || !strings.HasPrefix(ix[0], "CREATE INDEX ") && !strings.HasPrefix(ix[0], "CREATE UNIQUE INDEX ") {
			return ddl, ErrPostgres
		}
		ddl.after = append(ddl.after, ix[0])
	}
	return ddl, nil
}

// makeSchema makes the published tables the target lacks, and says how many
// of their defaults, constraints and indexes it had to leave out.
func (p *pgSet) makeSchema(ctx context.Context) (int, error) {
	var missing []table
	for _, t := range p.tables {
		reg, _, err := one(p.dst.Rows(ctx, "SELECT coalesce(to_regclass($1)::text, '')", t.ident()))
		if err != nil {
			return 0, err
		}
		if reg == "" {
			missing = append(missing, t)
		}
	}
	if len(missing) == 0 {
		return 0, nil
	}

	// Only pg_catalog on the path, so the deparsed text qualifies every
	// other name and means the same on the target. A SET is no write, and
	// the session's path goes back after.
	path, _, err := one(p.src.Rows(ctx, "SELECT current_setting('search_path')"))
	if err != nil {
		return 0, err
	}
	if _, err := p.src.Rows(ctx, "SELECT set_config('search_path', 'pg_catalog', false)"); err != nil {
		return 0, err
	}
	var tables []tableDDL
	for _, t := range missing {
		ddl, err := p.createTable(ctx, t)
		if err != nil {
			_, _ = p.src.Rows(context.WithoutCancel(ctx), "SELECT set_config('search_path', $1, false)", path)
			return 0, err
		}
		tables = append(tables, ddl)
	}
	if _, err := p.src.Rows(ctx, "SELECT set_config('search_path', $1, false)", path); err != nil {
		return 0, err
	}

	var schemas, required, optional []string
	seen := map[string]bool{}
	for i, ddl := range tables {
		for _, s := range append([]string{missing[i].schema}, ddl.schemas...) {
			if !seen[s] {
				seen[s] = true
				schemas = append(schemas, "CREATE SCHEMA IF NOT EXISTS "+pgwire.Ident(s))
			}
		}
		required = append(required, ddl.before...)
		required = append(required, ddl.create)
	}
	for _, ddl := range tables {
		optional = append(optional, ddl.after...)
	}
	for _, ddl := range tables {
		optional = append(optional, ddl.keys...)
	}

	if err := p.dst.Exec(ctx, "BEGIN"); err != nil {
		return 0, err
	}
	fail := func(err error) (int, error) {
		_ = p.dst.Exec(context.WithoutCancel(ctx), "ROLLBACK")
		return 0, err
	}
	for _, s := range append(schemas, required...) {
		if err := p.dst.Exec(ctx, s); err != nil {
			return fail(err)
		}
	}
	left := 0
	for _, s := range optional {
		if !oneStatement(s) {
			left++
			continue
		}
		if err := p.dst.Exec(ctx, "SAVEPOINT zoctl_schema"); err != nil {
			return fail(err)
		}
		if p.dst.Exec(ctx, s) != nil {
			left++
			if err := p.dst.Exec(ctx, "ROLLBACK TO SAVEPOINT zoctl_schema"); err != nil {
				return fail(err)
			}
			continue
		}
		if err := p.dst.Exec(ctx, "RELEASE SAVEPOINT zoctl_schema"); err != nil {
			return fail(err)
		}
	}
	if err := p.dst.Exec(ctx, "COMMIT"); err != nil {
		return fail(err)
	}
	return left, nil
}

// oneStatement reports whether s, text from the source's catalog, is one
// statement with nothing hidden in it: no semicolon, comment or dollar quote
// outside a quoted string or name, and every quote closed. The server's
// deparser writes none of those, so anything with one isn't its text.
func oneStatement(s string) bool {
	if s == "" || len(s) > 16384 {
		return false
	}
	var quote, prev rune
	for _, r := range s {
		switch {
		case quote != 0:
			// A doubled quote closes and opens again.
			if r == quote {
				quote = 0
			}
		case r == '\'' || r == '"':
			quote = r
		case r == ';' || r == '$' || r == 0:
			return false
		case r == '-' && prev == '-', r == '*' && prev == '/':
			return false
		}
		prev = r
	}
	return quote == 0
}
