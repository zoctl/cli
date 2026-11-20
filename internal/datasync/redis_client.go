package datasync

// The redis dataset's two sides as real servers, through go-redis.

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// quietRedis keeps go-redis's own log, which can name a server, out of
// the output.
type quietRedis struct{}

func (quietRedis) Printf(context.Context, string, ...any) {}

func init() { redis.SetLogger(quietRedis{}) }

// rdURL is a Redis URL taken apart: rediss://user:password@host:port/db.
// It has to be rediss, with TLS; skip_verify=true encrypts without checking
// the server's certificate. A URL that would send keys in plaintext is
// refused, as a database's is.
type rdURL struct {
	addr     string
	user     string
	password string
	db       int
	tls      *tls.Config
}

func parseRedisURL(raw string) (rdURL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "rediss" || u.Hostname() == "" || u.Fragment != "" {
		return rdURL{}, ErrMove
	}
	port := u.Port()
	if port == "" {
		port = "6379"
	}
	if n, err := strconv.ParseUint(port, 10, 16); err != nil || n == 0 {
		return rdURL{}, ErrMove
	}
	db := 0
	if p := strings.TrimPrefix(u.Path, "/"); p != "" {
		if db, err = strconv.Atoi(p); err != nil || db < 0 || db > 1<<16 {
			return rdURL{}, ErrMove
		}
	}
	c := &tls.Config{ServerName: u.Hostname(), MinVersion: tls.VersionTLS12}
	for k, v := range u.Query() {
		if k != "skip_verify" || len(v) != 1 || v[0] != "true" {
			return rdURL{}, ErrMove
		}
		c.InsecureSkipVerify = true
	}
	r := rdURL{addr: net.JoinHostPort(u.Hostname(), port), db: db, tls: c}
	if u.User != nil {
		r.user = u.User.Username()
		r.password, _ = u.User.Password()
	}
	return r, nil
}

func (r rdURL) id() string { return hashID(r.addr, strconv.Itoa(r.db)) }

func (r rdURL) connect() (*goRedis, error) {
	c := redis.NewClient(&redis.Options{
		Addr: r.addr, Username: r.user, Password: r.password, DB: r.db,
		TLSConfig:       r.tls.Clone(),
		Protocol:        2,
		DisableIdentity: true,
		DialTimeout:     15 * time.Second,
		ReadTimeout:     time.Minute,
		WriteTimeout:    time.Minute,
		MaxRetries:      2,
		PoolSize:        4,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if c.Ping(ctx).Err() != nil {
		c.Close()
		return nil, ErrRedis
	}
	return &goRedis{c: c}, nil
}

func openRedis(d Dataset, o Options) (dataset, error) {
	from, to := o.Env(d.FromEnv), o.Env(d.ToEnv)
	if from == "" || to == "" || from == to {
		return nil, ErrMove
	}
	fu, err := parseRedisURL(from)
	if err != nil {
		return nil, err
	}
	tu, err := parseRedisURL(to)
	if err != nil {
		return nil, err
	}
	src, err := fu.connect()
	if err != nil {
		return nil, err
	}
	dst, err := tu.connect()
	if err != nil {
		src.Close()
		return nil, err
	}
	return &redisSet{
		name: d.Name, src: readOnlyRedis{src}, dst: dst, w: o.Workspace,
		srcID: fu.id(), dstID: tu.id(), srcDB: fu.db, dstDB: tu.db,
	}, nil
}

// goRedis is one side.
type goRedis struct{ c *redis.Client }

// readOnlyRedis is the source: only rdSide's reads, so nothing that writes
// is in reach.
type readOnlyRedis struct{ r *goRedis }

func (s readOnlyRedis) Info(ctx context.Context) (map[string]string, error) { return s.r.Info(ctx) }
func (s readOnlyRedis) Size(ctx context.Context) (int64, error)             { return s.r.Size(ctx) }
func (s readOnlyRedis) Scan(ctx context.Context, c uint64) ([]string, uint64, error) {
	return s.r.Scan(ctx, c)
}
func (s readOnlyRedis) Dump(ctx context.Context, k []string) ([]rdValue, error) {
	return s.r.Dump(ctx, k)
}
func (s readOnlyRedis) Exists(ctx context.Context, k []string) ([]bool, error) {
	return s.r.Exists(ctx, k)
}
func (s readOnlyRedis) Content(ctx context.Context, k string) (string, error) {
	return s.r.Content(ctx, k)
}
func (s readOnlyRedis) Close() { s.r.Close() }

func (g *goRedis) Close() { g.c.Close() }

func (g *goRedis) Info(ctx context.Context) (map[string]string, error) {
	text, err := g.c.Info(ctx, "server", "cluster").Result()
	if err != nil {
		return nil, ErrRedis
	}
	out := map[string]string{}
	for _, line := range strings.Split(text, "\n") {
		if k, v, ok := strings.Cut(strings.TrimSpace(line), ":"); ok && !strings.HasPrefix(k, "#") {
			out[k] = v
		}
	}
	return out, nil
}

func (g *goRedis) Size(ctx context.Context) (int64, error) {
	n, err := g.c.DBSize(ctx).Result()
	if err != nil {
		return 0, ErrRedis
	}
	return n, nil
}

func (g *goRedis) Scan(ctx context.Context, cursor uint64) ([]string, uint64, error) {
	keys, next, err := g.c.Scan(ctx, cursor, "", 1000).Result()
	if err != nil {
		return nil, 0, ErrRedis
	}
	return keys, next, nil
}

func (g *goRedis) Dump(ctx context.Context, keys []string) ([]rdValue, error) {
	p := g.c.Pipeline()
	dumps := make([]*redis.StringCmd, len(keys))
	ttls := make([]*redis.DurationCmd, len(keys))
	for i, k := range keys {
		dumps[i] = p.Dump(ctx, k)
		ttls[i] = p.PTTL(ctx, k)
	}
	if _, err := p.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, ErrRedis
	}
	out := make([]rdValue, len(keys))
	for i := range keys {
		payload, err := dumps[i].Result()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			return nil, ErrRedis
		}
		ttl, err := ttls[i].Result()
		if err != nil {
			return nil, ErrRedis
		}
		switch {
		case ttl == -2:
			// Gone between the two.
			continue
		case ttl < 0:
			ttl = -1
		}
		out[i] = rdValue{Present: true, Payload: payload, TTL: ttl}
	}
	return out, nil
}

func (g *goRedis) Exists(ctx context.Context, keys []string) ([]bool, error) {
	p := g.c.Pipeline()
	cmds := make([]*redis.IntCmd, len(keys))
	for i, k := range keys {
		cmds[i] = p.Exists(ctx, k)
	}
	if _, err := p.Exec(ctx); err != nil {
		return nil, ErrRedis
	}
	out := make([]bool, len(keys))
	for i, c := range cmds {
		out[i] = c.Val() > 0
	}
	return out, nil
}

// Content reads a key by its type. Members of a set or hash are sorted,
// a sorted set comes in score order and a list and a stream in theirs.
func (g *goRedis) Content(ctx context.Context, key string) (string, error) {
	kind, err := g.c.Type(ctx, key).Result()
	if err != nil {
		return "", ErrRedis
	}
	var parts []string
	switch kind {
	case "string":
		v, err := g.c.Get(ctx, key).Result()
		if err != nil {
			return "", ErrRedis
		}
		parts = []string{v}
	case "list":
		if parts, err = g.c.LRange(ctx, key, 0, -1).Result(); err != nil {
			return "", ErrRedis
		}
	case "set":
		if parts, err = g.c.SMembers(ctx, key).Result(); err != nil {
			return "", ErrRedis
		}
		slices.Sort(parts)
	case "hash":
		m, err := g.c.HGetAll(ctx, key).Result()
		if err != nil {
			return "", ErrRedis
		}
		for k, v := range m {
			parts = append(parts, strconv.Itoa(len(k))+":"+k+v)
		}
		slices.Sort(parts)
	case "zset":
		zs, err := g.c.ZRangeWithScores(ctx, key, 0, -1).Result()
		if err != nil {
			return "", ErrRedis
		}
		for _, z := range zs {
			m, _ := z.Member.(string)
			parts = append(parts, m, strconv.FormatFloat(z.Score, 'g', -1, 64))
		}
	case "stream":
		xs, err := g.c.XRange(ctx, key, "-", "+").Result()
		if err != nil {
			return "", ErrRedis
		}
		for _, x := range xs {
			parts = append(parts, x.ID)
			var fields []string
			for k, v := range x.Values {
				s, _ := v.(string)
				fields = append(fields, strconv.Itoa(len(k))+":"+k+s)
			}
			slices.Sort(fields)
			parts = append(parts, fields...)
		}
	default:
		// A module's type, or none: only a matching DUMP says it's the same.
		return "", ErrRedis
	}
	var b strings.Builder
	b.WriteString(kind)
	for _, p := range parts {
		b.WriteString("\x00" + strconv.Itoa(len(p)) + ":" + p)
	}
	return b.String(), nil
}

func (g *goRedis) Restore(ctx context.Context, items []rdItem) error {
	p := g.c.Pipeline()
	for _, it := range items {
		ttl := it.TTL
		if ttl < 0 {
			ttl = 0 // RESTORE's 0 is no expiry
		}
		p.RestoreReplace(ctx, it.Key, ttl, it.Payload)
	}
	if _, err := p.Exec(ctx); err != nil {
		return ErrRedis
	}
	return nil
}

func (g *goRedis) Delete(ctx context.Context, keys []string) error {
	if g.c.Del(ctx, keys...).Err() != nil {
		return ErrRedis
	}
	return nil
}

var (
	_ rdTarget = (*goRedis)(nil)
	_ rdSide   = readOnlyRedis{}
)
