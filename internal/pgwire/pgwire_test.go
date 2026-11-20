package pgwire

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"
)

func TestSCRAMVector(t *testing.T) {
	// RFC 7677, section 3.
	s := &scramClient{password: "pencil", nonce: "rOprNGfwEbeRWgbNEkqO", firstBare: "n=user,r=rOprNGfwEbeRWgbNEkqO", maxIterRuns: 1 << 20}
	final, err := s.final([]byte("r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"))
	if err != nil {
		t.Fatal(err)
	}
	if string(final) != "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=" {
		t.Fatalf("final %s", final)
	}
	if !s.verify([]byte("v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=")) {
		t.Fatal("server signature refused")
	}
	if s.verify([]byte("v=AAAA")) {
		t.Fatal("a wrong server signature passed")
	}
	// A server nonce that doesn't extend ours is refused.
	if _, err := newSCRAM("x").final([]byte("r=other,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096")); err == nil {
		t.Fatal("foreign nonce accepted")
	}
}

func TestMD5(t *testing.T) {
	// md5(md5("secretpostgres") + salt)
	got := md5Password("postgres", "secret", []byte{1, 2, 3, 4})
	if !strings.HasPrefix(got, "md5") || len(got) != 35 {
		t.Fatal(got)
	}
}

func TestParseURL(t *testing.T) {
	for _, plain := range []string{"postgres://a:pw@h/x", "postgres://a:pw@h/x?sslmode=prefer", "postgres://a:pw@h/x?sslmode=disable",
		"host=h user=a sslmode=allow", "postgres://a@/x?host=/var/run/postgresql&sslmode=require"} {
		if _, err := ParseURL(plain); !errors.Is(err, ErrPlain) {
			t.Errorf("%s: %v", plain, err)
		}
	}
	c, err := ParseURL("postgresql://us%40r:p%3Ass@db1.example.test:6432,[2001:db8::1]/shop?sslmode=verify-full&connect_timeout=7&application_name=zoctl")
	if err != nil {
		t.Fatal(err)
	}
	if c.User != "us@r" || c.Password != "p:ss" || c.Database != "shop" || c.SSLMode != "verify-full" || c.ConnectTimeout != 7*time.Second {
		t.Fatalf("%+v", c)
	}
	if len(c.Hosts) != 2 || c.Hosts[0] != (Host{"db1.example.test", "6432"}) || c.Hosts[1] != (Host{"2001:db8::1", "5432"}) {
		t.Fatalf("hosts %+v", c.Hosts)
	}
	kv, err := ParseURL(`host=db.example.test port=5433 user=repl password='it\'s x' dbname=shop sslmode=require`)
	if err != nil {
		t.Fatal(err)
	}
	if kv.Password != "it's x" {
		t.Fatalf("password %q", kv.Password)
	}
	if kv.Hosts[0] != (Host{"db.example.test", "5433"}) || kv.Database != "shop" {
		t.Fatalf("%+v", kv)
	}
	if _, err := ParseURL("postgres://a:hunter2@h:99999/x?sslmode=require"); err == nil || strings.Contains(err.Error(), "hunter2") {
		t.Fatalf("bad port: %v", err)
	}
	if Ident("my", `t"x`) != `"my"."t""x"` {
		t.Fatal(Ident("my", `t"x`))
	}
}

// fakeServer answers one session on the other end of a pipe: TLS, SCRAM for
// the password "pw", then queries from the script.
type fakeServer struct {
	t       *testing.T
	cert    tls.Certificate
	params  map[string]string
	queries []string
	args    [][]string
	answer  func(sql string) ([][]string, *Error)
}

func selfSigned(t *testing.T, host string) (tls.Certificate, *x509.CertPool) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: host}, DNSNames: []string{host},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, _ := x509.ParseCertificate(der)
	pool := x509.NewCertPool()
	pool.AddCert(leaf)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}, pool
}

func readMsg(r io.Reader) (byte, []byte) {
	var h [5]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return 0, nil
	}
	b := make([]byte, binary.BigEndian.Uint32(h[1:])-4)
	io.ReadFull(r, b)
	return h[0], b
}

func writeMsg(w io.Writer, t byte, body []byte) {
	b := []byte{t, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(b[1:], uint32(len(body)+4))
	w.Write(append(b, body...))
}

func (f *fakeServer) serve(raw net.Conn) {
	defer raw.Close()
	var req [8]byte
	if _, err := io.ReadFull(raw, req[:]); err != nil {
		return
	}
	raw.Write([]byte{'S'})
	tc := tls.Server(raw, &tls.Config{Certificates: []tls.Certificate{f.cert}})
	if tc.Handshake() != nil {
		return
	}
	r := bufio.NewReader(tc)
	var h [4]byte
	io.ReadFull(r, h[:])
	start := make([]byte, binary.BigEndian.Uint32(h[:])-4)
	io.ReadFull(r, start)
	parts := strings.Split(string(start[4:]), "\x00")
	f.params = map[string]string{}
	for i := 0; i+1 < len(parts); i += 2 {
		f.params[parts[i]] = parts[i+1]
	}
	// SCRAM, server side, from the password.
	writeMsg(tc, 'R', append([]byte{0, 0, 0, 10}, "SCRAM-SHA-256-PLUS\x00SCRAM-SHA-256\x00\x00"...))
	_, init := readMsg(r)
	mech, rest, _ := strings.Cut(string(init), "\x00")
	if mech != "SCRAM-SHA-256" {
		f.t.Errorf("mechanism %q", mech)
		return
	}
	clientFirst := rest[4:]
	bare := strings.TrimPrefix(clientFirst, "n,,")
	cnonce := strings.TrimPrefix(bare, "n=,r=")
	salt := []byte("saltsaltsalt")
	serverFirst := "r=" + cnonce + "srv,s=" + base64.StdEncoding.EncodeToString(salt) + ",i=4096"
	writeMsg(tc, 'R', append([]byte{0, 0, 0, 11}, serverFirst...))
	_, final := readMsg(r)
	c := &scramClient{password: "pw", nonce: cnonce, firstBare: bare, maxIterRuns: 1 << 20}
	want, _ := c.final([]byte(serverFirst))
	if string(final) != string(want) {
		writeMsg(tc, 'E', []byte("SFATAL\x00C28P01\x00Mpassword authentication failed\x00\x00"))
		return
	}
	writeMsg(tc, 'R', append([]byte{0, 0, 0, 12}, "v="+base64.StdEncoding.EncodeToString(c.serverSig)...))
	writeMsg(tc, 'R', []byte{0, 0, 0, 0})
	writeMsg(tc, 'S', []byte("server_version\x0017.2\x00"))
	writeMsg(tc, 'Z', []byte{'I'})
	var sql string
	var args []string
	// Like a real server, the extended protocol answers at Sync: a pipe has no
	// buffer, and the client is still writing until then.
	var pending bytes.Buffer
	for {
		t, b := readMsg(r)
		switch t {
		case 0, 'X':
			return
		case 'Q':
			sql = strings.TrimSuffix(string(b), "\x00")
			f.queries = append(f.queries, sql)
			if _, e := f.answer(sql); e != nil {
				writeMsg(tc, 'E', []byte("SERROR\x00C"+e.Code+"\x00M"+e.Message+"\x00\x00"))
			} else {
				writeMsg(tc, 'C', []byte("OK\x00"))
			}
			writeMsg(tc, 'Z', []byte{'I'})
		case 'P':
			sql = strings.TrimSuffix(string(b[1:len(b)-2]), "\x00")
			writeMsg(&pending, '1', nil)
		case 'B':
			b = b[2:]
			n := int(binary.BigEndian.Uint16(b))
			b = b[2+2*n:]
			n = int(binary.BigEndian.Uint16(b))
			b = b[2:]
			args = nil
			for i := 0; i < n; i++ {
				l := int32(binary.BigEndian.Uint32(b))
				b = b[4:]
				if l < 0 {
					args = append(args, "<null>")
					continue
				}
				args = append(args, string(b[:l]))
				b = b[l:]
			}
			writeMsg(&pending, '2', nil)
		case 'D':
		case 'E':
			f.queries = append(f.queries, sql)
			f.args = append(f.args, args)
			rows, e := f.answer(sql)
			if e != nil {
				writeMsg(&pending, 'E', []byte("SERROR\x00C"+e.Code+"\x00M"+e.Message+"\x00\x00"))
				continue
			}
			for _, row := range rows {
				var d []byte
				d = binary.BigEndian.AppendUint16(d, uint16(len(row)))
				for _, v := range row {
					if v == "<null>" {
						d = binary.BigEndian.AppendUint32(d, 0xFFFFFFFF)
						continue
					}
					d = binary.BigEndian.AppendUint32(d, uint32(len(v)))
					d = append(d, v...)
				}
				writeMsg(&pending, 'D', d)
			}
			writeMsg(&pending, 'C', []byte("SELECT\x00"))
		case 'S':
			writeMsg(&pending, 'Z', []byte{'I'})
			tc.Write(pending.Bytes())
			pending.Reset()
		}
	}
}

func TestSession(t *testing.T) {
	cert, pool := selfSigned(t, "db.example.test")
	f := &fakeServer{t: t, cert: cert, answer: func(sql string) ([][]string, *Error) {
		switch {
		case strings.HasPrefix(sql, "SELECT name"):
			return [][]string{{"a", "1"}, {"b", "<null>"}}, nil
		case strings.HasPrefix(sql, "BROKEN"):
			return nil, &Error{Code: "42601", Message: "syntax error"}
		}
		return nil, nil
	}}
	done := make(chan struct{})
	cfg, err := ParseURL("postgres://repl:pw@db.example.test/shop?sslmode=verify-full")
	if err != nil {
		t.Fatal(err)
	}
	cfg.RootCAs = pool
	cfg.Params["default_transaction_read_only"] = "on"
	cfg.Dial = func(ctx context.Context, network, addr string) (net.Conn, error) {
		if addr != "db.example.test:5432" {
			t.Errorf("dialed %s", addr)
		}
		a, b := bufPipe()
		go func() { f.serve(b); close(done) }()
		return a, nil
	}
	c, err := Connect(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if c.Params["server_version"] != "17.2" || f.params["user"] != "repl" || f.params["database"] != "shop" || f.params["default_transaction_read_only"] != "on" {
		t.Fatalf("params %v %v", c.Params, f.params)
	}
	rows, err := c.Rows(context.Background(), "SELECT name, n FROM t WHERE a = $1 AND b = $2", "x'y", 42)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0][0] != "a" || rows[0][1] != "1" || rows[1][1] != "" {
		t.Fatalf("rows %q", rows)
	}
	if got := f.args[0]; len(got) != 2 || got[0] != "x'y" || got[1] != "42" {
		t.Fatalf("args %q", got)
	}
	_, err = c.Rows(context.Background(), "BROKEN")
	var pe *Error
	if !errors.As(err, &pe) || pe.Code != "42601" {
		t.Fatalf("error %v", err)
	}
	// The session goes on after a statement's error.
	if err := c.Exec(context.Background(), "CREATE SUBSCRIPTION s CONNECTION 'x' PUBLICATION p"); err != nil {
		t.Fatal(err)
	}
	if err := c.Exec(context.Background(), "BROKEN"); !errors.As(err, &pe) {
		t.Fatalf("exec error %v", err)
	}
	c.Close()
	<-done
}

func TestWrongPasswordAndName(t *testing.T) {
	cert, pool := selfSigned(t, "db.example.test")
	for _, tc := range []struct{ url, want string }{
		{"postgres://repl:nope@db.example.test/shop?sslmode=verify-full", "28P01"},
		{"postgres://repl:pw@other.example.test/shop?sslmode=verify-full", "certificate"},
	} {
		f := &fakeServer{t: t, cert: cert, answer: func(string) ([][]string, *Error) { return nil, nil }}
		cfg, _ := ParseURL(tc.url)
		cfg.RootCAs = pool
		cfg.Dial = func(ctx context.Context, network, addr string) (net.Conn, error) {
			a, b := bufPipe()
			go f.serve(b)
			return a, nil
		}
		_, err := Connect(context.Background(), cfg)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v", tc.want, err)
		}
	}
	// require takes the self-signed certificate under any name.
	f := &fakeServer{t: t, cert: cert, answer: func(string) ([][]string, *Error) { return nil, nil }}
	cfg, _ := ParseURL("postgres://repl:pw@other.example.test/shop?sslmode=require")
	cfg.Dial = func(ctx context.Context, network, addr string) (net.Conn, error) {
		a, b := bufPipe()
		go f.serve(b)
		return a, nil
	}
	c, err := Connect(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
}

func TestCancel(t *testing.T) {
	cert, pool := selfSigned(t, "db.example.test")
	block := make(chan struct{})
	f := &fakeServer{t: t, cert: cert, answer: func(sql string) ([][]string, *Error) {
		if sql == "SLOW" {
			<-block
		}
		return nil, nil
	}}
	cfg, _ := ParseURL("postgres://repl:pw@db.example.test/shop?sslmode=verify-full")
	cfg.RootCAs = pool
	cfg.Dial = func(ctx context.Context, network, addr string) (net.Conn, error) {
		a, b := bufPipe()
		go f.serve(b)
		return a, nil
	}
	c, err := Connect(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := c.Rows(ctx, "SLOW"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("slow query: %v", err)
	}
	close(block)
	if _, err := c.Rows(context.Background(), "SELECT 1"); !errors.Is(err, ErrClosed) {
		t.Fatalf("after a cancel: %v", err)
	}
}
