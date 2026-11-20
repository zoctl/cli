// Package pgwire is a small PostgreSQL client on the standard library: TLS,
// SCRAM-SHA-256, MD5 or password sign-in, and queries whose every column comes
// back as text. It is what zoctl's data sync needs from a database and no
// more: no pool, no types, no copy protocol.
package pgwire

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// ErrPlain means a URL allows a connection without TLS. It has to say
// sslmode=require, verify-ca or verify-full.
var ErrPlain = errors.New("pgwire: the URL has to ask for TLS: sslmode=require, verify-ca or verify-full")

// ErrURL means a URL couldn't be read. It never repeats the URL, which holds a
// password.
var ErrURL = errors.New("pgwire: the database URL couldn't be read")

// ErrClosed means the connection broke or was closed, and can't be used again.
var ErrClosed = errors.New("pgwire: the connection is closed")

// Error is an ErrorResponse from the server. Code is the SQLSTATE.
type Error struct {
	Severity string
	Code     string
	Message  string
}

func (e *Error) Error() string { return "pgwire: " + e.Severity + " " + e.Code + ": " + e.Message }

// Host is one server a URL names. A URL may name several, tried in order.
type Host struct {
	Name string
	Port string
}

// Config is a parsed URL.
type Config struct {
	Hosts    []Host
	User     string
	Password string
	Database string
	// SSLMode is require, verify-ca or verify-full.
	SSLMode     string
	SSLRootCert string
	// ConnectTimeout is zero when the URL set none.
	ConnectTimeout time.Duration
	// Params go in the startup message: application_name and anything the
	// caller adds.
	Params map[string]string
	// Dial opens the transport. Nil is a net.Dialer; tests pass a pipe.
	Dial func(ctx context.Context, network, addr string) (net.Conn, error)
	// RootCAs, when set, is used instead of SSLRootCert or the system's.
	RootCAs *x509.CertPool
}

// ParseURL reads a postgres:// or postgresql:// URL, or a key=value
// connection string, the way libpq does for the parts zoctl takes. A URL
// that doesn't ask for TLS is refused with ErrPlain.
func ParseURL(s string) (*Config, error) {
	kv := map[string]string{}
	var hosts []Host
	switch {
	case strings.HasPrefix(s, "postgres://"), strings.HasPrefix(s, "postgresql://"):
		u, err := parseLibpqURL(s)
		if err != nil {
			return nil, ErrURL
		}
		kv, hosts = u.kv, u.hosts
	default:
		var err error
		if kv, err = parseKeyValue(s); err != nil {
			return nil, ErrURL
		}
		hs := splitList(kv["host"])
		ps := splitList(kv["port"])
		for i, h := range hs {
			p := ""
			if len(ps) == 1 {
				p = ps[0]
			} else if i < len(ps) {
				p = ps[i]
			}
			hosts = append(hosts, Host{h, p})
		}
	}
	c := &Config{User: kv["user"], Password: kv["password"], Database: kv["dbname"], SSLMode: kv["sslmode"],
		SSLRootCert: kv["sslrootcert"], Params: map[string]string{}}
	if len(hosts) == 0 {
		hosts = []Host{{Name: "localhost"}}
	}
	for i := range hosts {
		if hosts[i].Port == "" {
			hosts[i].Port = "5432"
		}
		if strings.HasPrefix(hosts[i].Name, "/") {
			// A Unix socket can't carry TLS, and a sync never runs beside the
			// database it reads.
			return nil, ErrPlain
		}
		if n, err := strconv.Atoi(hosts[i].Port); err != nil || n < 1 || n > 65535 {
			return nil, ErrURL
		}
	}
	c.Hosts = hosts
	switch c.SSLMode {
	case "require", "verify-ca", "verify-full":
	default:
		return nil, ErrPlain
	}
	if v := kv["connect_timeout"]; v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return nil, ErrURL
		}
		c.ConnectTimeout = time.Duration(n) * time.Second
	}
	if v := kv["application_name"]; v != "" {
		c.Params["application_name"] = v
	}
	if c.User == "" {
		return nil, ErrURL
	}
	if c.Database == "" {
		c.Database = c.User
	}
	return c, nil
}

type libpqURL struct {
	kv    map[string]string
	hosts []Host
}

// parseLibpqURL reads a URL whose authority may list several host:port pairs,
// which net/url refuses.
func parseLibpqURL(s string) (libpqURL, error) {
	out := libpqURL{kv: map[string]string{}}
	rest := s[strings.Index(s, "://")+3:]
	query := ""
	if i := strings.IndexByte(rest, '?'); i >= 0 {
		rest, query = rest[:i], rest[i+1:]
	}
	path := ""
	if i := strings.IndexByte(rest, '/'); i >= 0 {
		rest, path = rest[:i], rest[i+1:]
	}
	if i := strings.LastIndexByte(rest, '@'); i >= 0 {
		info := rest[:i]
		rest = rest[i+1:]
		user, pass, hasPass := strings.Cut(info, ":")
		var err error
		if out.kv["user"], err = url.PathUnescape(user); err != nil {
			return out, err
		}
		if hasPass {
			if out.kv["password"], err = url.PathUnescape(pass); err != nil {
				return out, err
			}
		}
	}
	if path != "" {
		db, err := url.PathUnescape(path)
		if err != nil {
			return out, err
		}
		out.kv["dbname"] = db
	}
	for _, hp := range splitList(rest) {
		h, p := hp, ""
		if strings.HasPrefix(hp, "[") {
			end := strings.IndexByte(hp, ']')
			if end < 0 {
				return out, ErrURL
			}
			h = hp[1:end]
			if strings.HasPrefix(hp[end+1:], ":") {
				p = hp[end+2:]
			}
		} else if i := strings.LastIndexByte(hp, ':'); i >= 0 {
			h, p = hp[:i], hp[i+1:]
		}
		h, err := url.PathUnescape(h)
		if err != nil {
			return out, err
		}
		out.hosts = append(out.hosts, Host{h, p})
	}
	q, err := url.ParseQuery(query)
	if err != nil {
		return out, err
	}
	for k, v := range q {
		if len(v) > 0 {
			out.kv[k] = v[len(v)-1]
		}
	}
	if h := out.kv["host"]; h != "" && len(out.hosts) == 0 {
		out.hosts = append(out.hosts, Host{h, out.kv["port"]})
	}
	if p := out.kv["port"]; p != "" {
		for i := range out.hosts {
			if out.hosts[i].Port == "" {
				out.hosts[i].Port = p
			}
		}
	}
	return out, nil
}

func splitList(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, ",")
}

// parseKeyValue reads host=x dbname=y with libpq's quoting.
func parseKeyValue(s string) (map[string]string, error) {
	kv := map[string]string{}
	for {
		s = strings.TrimLeft(s, " \t\n\r")
		if s == "" {
			return kv, nil
		}
		eq := strings.IndexByte(s, '=')
		if eq <= 0 {
			return nil, ErrURL
		}
		key := strings.TrimSpace(s[:eq])
		s = strings.TrimLeft(s[eq+1:], " \t")
		var val strings.Builder
		if strings.HasPrefix(s, "'") {
			s = s[1:]
			closed := false
			for len(s) > 0 {
				ch := s[0]
				s = s[1:]
				if ch == '\\' && len(s) > 0 {
					val.WriteByte(s[0])
					s = s[1:]
					continue
				}
				if ch == '\'' {
					closed = true
					break
				}
				val.WriteByte(ch)
			}
			if !closed {
				return nil, ErrURL
			}
		} else {
			for len(s) > 0 && s[0] != ' ' && s[0] != '\t' && s[0] != '\n' {
				if s[0] == '\\' && len(s) > 1 {
					s = s[1:]
				}
				val.WriteByte(s[0])
				s = s[1:]
			}
		}
		kv[key] = val.String()
	}
}

// Conn is one session. It is not safe for concurrent use.
type Conn struct {
	nc     net.Conn
	r      *bufio.Reader
	broken bool
	// Params are the server's ParameterStatus values.
	Params map[string]string
}

// Connect opens a session to the first of the config's hosts that answers.
func Connect(ctx context.Context, c *Config) (*Conn, error) {
	if c.ConnectTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.ConnectTimeout)
		defer cancel()
	}
	var last error
	for _, h := range c.Hosts {
		conn, err := connectHost(ctx, c, h)
		if err == nil {
			return conn, nil
		}
		last = err
		if ctx.Err() != nil {
			break
		}
	}
	return nil, last
}

func (c *Config) tlsConfig(host string) (*tls.Config, error) {
	t := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: host}
	roots := c.RootCAs
	if roots == nil && c.SSLRootCert != "" && c.SSLRootCert != "system" {
		pem, err := os.ReadFile(c.SSLRootCert)
		if err != nil {
			return nil, fmt.Errorf("pgwire: sslrootcert couldn't be read")
		}
		roots = x509.NewCertPool()
		if !roots.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("pgwire: sslrootcert has no certificate")
		}
	}
	mode := c.SSLMode
	// libpq checks the chain under require when a root certificate is named.
	if mode == "require" && c.SSLRootCert != "" {
		mode = "verify-ca"
	}
	switch mode {
	case "require":
		t.InsecureSkipVerify = true
	case "verify-full":
		t.RootCAs = roots
	case "verify-ca":
		// The chain, without the name.
		t.InsecureSkipVerify = true
		t.VerifyConnection = func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return errors.New("pgwire: the server sent no certificate")
			}
			opts := x509.VerifyOptions{Roots: roots, Intermediates: x509.NewCertPool()}
			for _, ic := range cs.PeerCertificates[1:] {
				opts.Intermediates.AddCert(ic)
			}
			_, err := cs.PeerCertificates[0].Verify(opts)
			return err
		}
	}
	return t, nil
}

func connectHost(ctx context.Context, c *Config, h Host) (*Conn, error) {
	dial := c.Dial
	if dial == nil {
		dial = (&net.Dialer{}).DialContext
	}
	raw, err := dial(ctx, "tcp", net.JoinHostPort(h.Name, h.Port))
	if err != nil {
		return nil, err
	}
	stop := context.AfterFunc(ctx, func() { _ = raw.SetDeadline(time.Unix(1, 0)) })
	defer stop()
	fail := func(err error) (*Conn, error) {
		raw.Close()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	// SSLRequest
	var req [8]byte
	binary.BigEndian.PutUint32(req[0:], 8)
	binary.BigEndian.PutUint32(req[4:], 80877103)
	if _, err := raw.Write(req[:]); err != nil {
		return fail(err)
	}
	var yes [1]byte
	if _, err := io.ReadFull(raw, yes[:]); err != nil {
		return fail(err)
	}
	if yes[0] != 'S' {
		return fail(ErrPlain)
	}
	tc, err := c.tlsConfig(h.Name)
	if err != nil {
		return fail(err)
	}
	t := tls.Client(raw, tc)
	if err := t.HandshakeContext(ctx); err != nil {
		return fail(err)
	}
	conn := &Conn{nc: t, r: bufio.NewReaderSize(t, 32<<10), Params: map[string]string{}}
	if err := conn.startup(c); err != nil {
		return fail(err)
	}
	return conn, nil
}

func (c *Conn) startup(cfg *Config) error {
	var b []byte
	b = binary.BigEndian.AppendUint32(b, 0)
	b = binary.BigEndian.AppendUint32(b, 196608) // 3.0
	param := func(k, v string) {
		b = append(append(append(append(b, k...), 0), v...), 0)
	}
	param("user", cfg.User)
	param("database", cfg.Database)
	for k, v := range cfg.Params {
		param(k, v)
	}
	b = append(b, 0)
	binary.BigEndian.PutUint32(b, uint32(len(b)))
	if _, err := c.nc.Write(b); err != nil {
		return err
	}
	var scram *scramClient
	for {
		t, body, err := c.read()
		if err != nil {
			return err
		}
		switch t {
		case 'R':
			if len(body) < 4 {
				return ErrClosed
			}
			kind := binary.BigEndian.Uint32(body)
			data := body[4:]
			switch kind {
			case 0: // ok
			case 3: // cleartext, inside TLS
				if err := c.send('p', append([]byte(cfg.Password), 0)); err != nil {
					return err
				}
			case 5:
				if len(data) < 4 {
					return ErrClosed
				}
				if err := c.send('p', append([]byte(md5Password(cfg.User, cfg.Password, data[:4])), 0)); err != nil {
					return err
				}
			case 10:
				if !hasMechanism(data, "SCRAM-SHA-256") {
					return errors.New("pgwire: the server offers no sign-in this client has")
				}
				scram = newSCRAM(cfg.Password)
				first := scram.first()
				var m []byte
				m = append(append(m, "SCRAM-SHA-256"...), 0)
				m = binary.BigEndian.AppendUint32(m, uint32(len(first)))
				m = append(m, first...)
				if err := c.send('p', m); err != nil {
					return err
				}
			case 11:
				if scram == nil {
					return ErrClosed
				}
				final, err := scram.final(data)
				if err != nil {
					return err
				}
				if err := c.send('p', final); err != nil {
					return err
				}
			case 12:
				if scram == nil || !scram.verify(data) {
					return errors.New("pgwire: the server couldn't prove it knows the password")
				}
			default:
				return errors.New("pgwire: the server asks for a sign-in this client doesn't have")
			}
		case 'S':
			k, v, _ := strings.Cut(strings.TrimSuffix(string(body), "\x00"), "\x00")
			c.Params[k] = v
		case 'K', 'N':
		case 'E':
			return parseError(body)
		case 'Z':
			return nil
		default:
			return ErrClosed
		}
	}
}

func hasMechanism(data []byte, want string) bool {
	for _, m := range strings.Split(string(data), "\x00") {
		if m == want {
			return true
		}
	}
	return false
}

func (c *Conn) send(t byte, body []byte) error {
	b := make([]byte, 5, 5+len(body))
	b[0] = t
	binary.BigEndian.PutUint32(b[1:], uint32(len(body)+4))
	_, err := c.nc.Write(append(b, body...))
	return err
}

// maxMessage bounds one message from the server: a row of a catalog query is
// far smaller.
const maxMessage = 1 << 30

func (c *Conn) read() (byte, []byte, error) {
	var h [5]byte
	if _, err := io.ReadFull(c.r, h[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(h[1:])
	if n < 4 || n > maxMessage {
		return 0, nil, ErrClosed
	}
	body := make([]byte, n-4)
	if _, err := io.ReadFull(c.r, body); err != nil {
		return 0, nil, err
	}
	return h[0], body, nil
}

func parseError(body []byte) *Error {
	e := &Error{}
	for len(body) > 1 {
		f := body[0]
		end := 1
		for end < len(body) && body[end] != 0 {
			end++
		}
		v := string(body[1:end])
		switch f {
		case 'V':
			e.Severity = v
		case 'S':
			if e.Severity == "" {
				e.Severity = v
			}
		case 'C':
			e.Code = v
		case 'M':
			e.Message = v
		}
		if end >= len(body) {
			break
		}
		body = body[end+1:]
	}
	return e
}

// guard ties a call to its context: a cancelled context breaks the
// connection, as the protocol has no way to stop one call and keep going.
func (c *Conn) guard(ctx context.Context) (func() error, error) {
	if c.broken {
		return nil, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if d, ok := ctx.Deadline(); ok {
		_ = c.nc.SetDeadline(d)
	} else {
		_ = c.nc.SetDeadline(time.Time{})
	}
	stop := context.AfterFunc(ctx, func() { _ = c.nc.SetDeadline(time.Unix(1, 0)) })
	return func() error {
		stop()
		return ctx.Err()
	}, nil
}

// fault marks the connection unusable after a transport error.
func (c *Conn) fault(ctx context.Context, err error) error {
	c.broken = true
	_ = c.nc.Close()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// Exec runs SQL with the simple query protocol: no parameters, and a
// statement that refuses a transaction block, such as CREATE SUBSCRIPTION,
// runs as it would in psql.
func (c *Conn) Exec(ctx context.Context, sql string) error {
	done, err := c.guard(ctx)
	if err != nil {
		return err
	}
	defer done()
	if err := c.send('Q', append([]byte(sql), 0)); err != nil {
		return c.fault(ctx, err)
	}
	var first *Error
	for {
		t, body, err := c.read()
		if err != nil {
			return c.fault(ctx, err)
		}
		switch t {
		case 'E':
			if first == nil {
				first = parseError(body)
			}
		case 'Z':
			if first != nil {
				return first
			}
			return nil
		case 'G', 'H', 'W':
			// COPY isn't spoken here: give up on the session.
			return c.fault(ctx, errors.New("pgwire: COPY isn't supported"))
		}
	}
}

// Rows runs one statement with the extended protocol and returns every row,
// each column as text, NULL as "". Arguments go as text and the server works
// out their types, so a query casts where it has to.
func (c *Conn) Rows(ctx context.Context, sql string, args ...any) ([][]string, error) {
	done, err := c.guard(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	var parse []byte
	parse = append(parse, 0) // unnamed statement
	parse = append(append(parse, sql...), 0)
	parse = binary.BigEndian.AppendUint16(parse, 0)
	var bind []byte
	bind = append(bind, 0, 0)                     // portal, statement
	bind = binary.BigEndian.AppendUint16(bind, 0) // every parameter as text
	bind = binary.BigEndian.AppendUint16(bind, uint16(len(args)))
	for _, a := range args {
		v, null, err := text(a)
		if err != nil {
			return nil, err
		}
		if null {
			bind = binary.BigEndian.AppendUint32(bind, 0xFFFFFFFF)
			continue
		}
		bind = binary.BigEndian.AppendUint32(bind, uint32(len(v)))
		bind = append(bind, v...)
	}
	bind = binary.BigEndian.AppendUint16(bind, 0) // every result as text
	var msgs []byte
	add := func(t byte, body []byte) {
		msgs = append(msgs, t)
		msgs = binary.BigEndian.AppendUint32(msgs, uint32(len(body)+4))
		msgs = append(msgs, body...)
	}
	add('P', parse)
	add('B', bind)
	add('D', []byte{'P', 0})
	add('E', []byte{0, 0, 0, 0, 0})
	add('S', nil)
	if _, err := c.nc.Write(msgs); err != nil {
		return nil, c.fault(ctx, err)
	}
	var out [][]string
	var first *Error
	for {
		t, body, err := c.read()
		if err != nil {
			return nil, c.fault(ctx, err)
		}
		switch t {
		case 'D':
			row, err := dataRow(body)
			if err != nil {
				return nil, c.fault(ctx, err)
			}
			out = append(out, row)
		case 'E':
			if first == nil {
				first = parseError(body)
			}
		case 'Z':
			if first != nil {
				return nil, first
			}
			return out, nil
		case 'G', 'H', 'W':
			return nil, c.fault(ctx, errors.New("pgwire: COPY isn't supported"))
		}
	}
}

func dataRow(b []byte) ([]string, error) {
	if len(b) < 2 {
		return nil, ErrClosed
	}
	n := int(binary.BigEndian.Uint16(b))
	b = b[2:]
	row := make([]string, n)
	for i := range row {
		if len(b) < 4 {
			return nil, ErrClosed
		}
		l := int32(binary.BigEndian.Uint32(b))
		b = b[4:]
		if l < 0 {
			continue
		}
		if int(l) > len(b) {
			return nil, ErrClosed
		}
		row[i] = string(b[:l])
		b = b[l:]
	}
	return row, nil
}

// text writes an argument the way the server reads a text parameter.
func text(a any) (string, bool, error) {
	switch v := a.(type) {
	case nil:
		return "", true, nil
	case string:
		return v, false, nil
	case []byte:
		return string(v), false, nil
	case int:
		return strconv.Itoa(v), false, nil
	case int32:
		return strconv.FormatInt(int64(v), 10), false, nil
	case int64:
		return strconv.FormatInt(v, 10), false, nil
	case uint32:
		return strconv.FormatUint(uint64(v), 10), false, nil
	case uint64:
		return strconv.FormatUint(v, 10), false, nil
	case bool:
		return strconv.FormatBool(v), false, nil
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64), false, nil
	case time.Time:
		return v.UTC().Format("2006-01-02 15:04:05.999999Z07:00"), false, nil
	case fmt.Stringer:
		return v.String(), false, nil
	}
	return "", false, fmt.Errorf("pgwire: an argument of type %T can't be sent", a)
}

// Close ends the session.
func (c *Conn) Close() error {
	if c.broken {
		return nil
	}
	c.broken = true
	_ = c.nc.SetDeadline(time.Now().Add(time.Second))
	_ = c.send('X', nil)
	return c.nc.Close()
}

// Ident quotes an identifier, schema-qualified when given more than one part.
func Ident(parts ...string) string {
	q := make([]string, len(parts))
	for i, p := range parts {
		q[i] = `"` + strings.ReplaceAll(strings.ReplaceAll(p, "\x00", ""), `"`, `""`) + `"`
	}
	return strings.Join(q, ".")
}
