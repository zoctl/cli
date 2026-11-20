package datasync

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/zoctl/cli/internal/transfer"
)

// fakeRD is one Redis database in memory. A key's dump is what DUMP gives
// and its content what the same value reads as however it's kept.
type fakeRD struct {
	mu       sync.Mutex
	info     map[string]string
	keys     map[string]fakeRDValue
	noDump   bool
	restores int
}

type fakeRDValue struct {
	dump, content string
	ttl           time.Duration
}

func newFakeRD(runID string) *fakeRD {
	return &fakeRD{info: map[string]string{"run_id": runID, "redis_version": "7.2.4", "cluster_enabled": "0"}, keys: map[string]fakeRDValue{}}
}

func (f *fakeRD) set(k, v string, ttl time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.keys[k] = fakeRDValue{dump: "dump:" + v, content: v, ttl: ttl}
}

func (f *fakeRD) Info(context.Context) (map[string]string, error) { return f.info, nil }

func (f *fakeRD) Size(context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return int64(len(f.keys)), nil
}

// Scan gives two keys a call, in order.
func (f *fakeRD) Scan(_ context.Context, cursor uint64) ([]string, uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var all []string
	for k := range f.keys {
		all = append(all, k)
	}
	slices.Sort(all)
	i := min(int(cursor), len(all))
	end := min(i+2, len(all))
	next := uint64(end)
	if end == len(all) {
		next = 0
	}
	return all[i:end], next, nil
}

func (f *fakeRD) Dump(_ context.Context, keys []string) ([]rdValue, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.noDump {
		return nil, ErrRedis
	}
	out := make([]rdValue, len(keys))
	for i, k := range keys {
		if v, ok := f.keys[k]; ok {
			out[i] = rdValue{Present: true, Payload: v.dump, TTL: v.ttl}
		}
	}
	return out, nil
}

func (f *fakeRD) Exists(_ context.Context, keys []string) ([]bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]bool, len(keys))
	for i, k := range keys {
		_, out[i] = f.keys[k]
	}
	return out, nil
}

func (f *fakeRD) Content(_ context.Context, k string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.keys[k]
	if !ok {
		return "", ErrRedis
	}
	return v.content, nil
}

func (f *fakeRD) Restore(_ context.Context, items []rdItem) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, it := range items {
		content := it.Payload[len("dump:"):]
		f.keys[it.Key] = fakeRDValue{dump: it.Payload, content: content, ttl: it.TTL}
		f.restores++
	}
	return nil
}

func (f *fakeRD) Delete(_ context.Context, keys []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, k := range keys {
		delete(f.keys, k)
	}
	return nil
}

func (f *fakeRD) Close() {}

func rdSet(src, dst *fakeRD, w *transfer.Workspace) *redisSet {
	return &redisSet{name: "cache", src: src, dst: dst, w: w, srcID: "a", dstID: "b"}
}

func budget(objects int64) *transfer.Budget {
	return transfer.NewBudget(transfer.Limits{MaxObjects: objects, MaxBytes: 1 << 30, Until: time.Now().Add(time.Hour)}, nil)
}

func TestRedisCopyThenFollow(t *testing.T) {
	ctx := context.Background()
	src, dst := newFakeRD("one"), newFakeRD("two")
	for i := range 5 {
		src.set("k"+strconv.Itoa(i), "v"+strconv.Itoa(i), -1)
	}
	src.set("session", "s", 90*time.Second)
	src.set("going", "g", 0) // expires as it's read
	w := testWorkspace(t)
	s := rdSet(src, dst, w)
	p, err := s.Plan(ctx)
	if err != nil || p.Blocked || p.Copy != 7 || p.Extra != 0 || p.Digest == "" {
		t.Fatalf("plan %+v %v", p, err)
	}
	b := budget(100)
	st, err := s.Start(ctx, p, b)
	if err != nil || st.Copied != 6 || st.Extra != 0 {
		t.Fatalf("start %+v %v", st, err)
	}
	if dst.keys["session"].ttl != 90*time.Second || dst.keys["k1"].ttl != -1 {
		t.Fatalf("times to live %+v", dst.keys)
	}
	if _, ok := dst.keys["going"]; ok {
		t.Fatal("a key expiring as it was read was copied")
	}

	src.set("k1", "changed", -1)
	src.mu.Lock()
	delete(src.keys, "k2")
	delete(src.keys, "going")
	src.mu.Unlock()
	st, err = s.Round(ctx, b)
	if err != nil || st.Copied != 1 || st.Extra != 1 || st.Pending != 0 {
		t.Fatalf("round %+v %v", st, err)
	}
	if dst.keys["k1"].content != "changed" {
		t.Fatalf("k1 is %q", dst.keys["k1"].content)
	}
	if _, ok := dst.keys["k2"]; ok {
		t.Fatal("a key the source removed is still on the target")
	}
	if c, err := s.Check(ctx); err != nil || !c.Verified {
		t.Fatalf("check %+v %v", c, err)
	}
	// A target that keeps a value otherwise, with the same in it, isn't
	// copied again.
	dst.mu.Lock()
	v := dst.keys["k3"]
	v.dump = "listpack:v3"
	dst.keys["k3"] = v
	dst.mu.Unlock()
	before := dst.restores
	if st, err := s.Round(ctx, b); err != nil || st.Copied != 0 || dst.restores != before {
		t.Fatalf("round over an equal value %+v %v", st, err)
	}
	// Check writes nothing.
	src.set("k4", "new", -1)
	if c, err := s.Check(ctx); err != nil || c.Verified || c.Pending != 1 || dst.keys["k4"].content != "v4" {
		t.Fatalf("check after a change %+v %v", c, err)
	}

	// The next approval takes up the target it filled.
	p2, err := rdSet(src, dst, w).Plan(ctx)
	if err != nil || p2.Blocked || p2.work != "resume" {
		t.Fatalf("second plan %+v %v", p2, err)
	}
}

func TestRedisPlanBlocks(t *testing.T) {
	ctx := context.Background()
	src, dst := newFakeRD("one"), newFakeRD("one")
	src.info["cluster_enabled"] = "1"
	src.info["redis_version"] = "7.4.1"
	src.set("k", "v", -1)
	src.noDump = true
	dst.set("there", "already", -1)
	s := rdSet(src, dst, testWorkspace(t))
	p, err := s.Plan(ctx)
	if err != nil || !p.Blocked {
		t.Fatalf("plan %+v %v", p, err)
	}
	for _, want := range []string{findingSameDatabase, findingRedisCluster, findingTargetOlder, findingPrerequisite, findingNotEmpty} {
		if !slices.Contains(p.Findings, want) {
			t.Errorf("findings %v, want %s", p.Findings, want)
		}
	}
	if olderVersion("8.0.0", "7.4.1") || !olderVersion("7.2.4", "7.4.1") || olderVersion("7.4", "7.4.1") || olderVersion("x", "7") {
		t.Fatal("version order")
	}
}

func TestRedisBudget(t *testing.T) {
	ctx := context.Background()
	src, dst := newFakeRD("one"), newFakeRD("two")
	src.set("a", "1", -1)
	src.set("b", "2", -1)
	src.set("c", "3", -1)
	s := rdSet(src, dst, testWorkspace(t))
	p, err := s.Plan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Start(ctx, p, budget(1)); !errors.Is(err, transfer.ErrBudget) || len(dst.keys) != 0 {
		t.Fatalf("over the limit: %v, %d keys written", err, len(dst.keys))
	}
}

func TestRedisURL(t *testing.T) {
	u, err := parseRedisURL("rediss://mover:secret@cache.example.com:6380/2")
	if err != nil || u.addr != "cache.example.com:6380" || u.db != 2 || u.user != "mover" || u.password != "secret" || u.tls.InsecureSkipVerify {
		t.Fatalf("url %+v %v", u, err)
	}
	if u, err := parseRedisURL("rediss://:secret@cache.example.com?skip_verify=true"); err != nil || !u.tls.InsecureSkipVerify || u.db != 0 || u.addr != "cache.example.com:6379" {
		t.Fatalf("skip_verify %+v %v", u, err)
	}
	if a, _ := parseRedisURL("rediss://cache.example.com/1"); a.id() == u.id() {
		t.Fatal("two databases read as one")
	}
	for _, bad := range []string{
		"redis://mover:secret@cache.example.com:6379/0",
		"rediss://cache.example.com/zero",
		"rediss://cache.example.com/0?skip_verify=false",
		"rediss://cache.example.com/0?protocol=3",
		"unix:///tmp/redis.sock",
	} {
		if _, err := parseRedisURL(bad); !errors.Is(err, ErrMove) {
			t.Errorf("%s: %v", bad, err)
		}
	}
}

func TestDatabaseKindsInAMoveFile(t *testing.T) {
	head := `{"version": 1, "source": "aws", "target": "gcp", "limits": {"max_objects": 10, "max_bytes": 10, "hours": 1}, "datasets": [`
	ok := []string{
		`{"name": "shop", "kind": "mysql", "from_env": "SOURCE_SHOP_URL", "to_env": "TARGET_SHOP_URL", "copy_schema": true}`,
		`{"name": "cache", "kind": "redis", "from_env": "SOURCE_CACHE_URL", "to_env": "TARGET_CACHE_URL"}`,
	}
	for _, d := range ok {
		m, err := ParseMove([]byte(head + d + `]}`))
		if err != nil {
			t.Fatalf("%s: %v", d, err)
		}
		if m.Reversible() {
			t.Fatalf("%s has a way back", d)
		}
	}
	for _, d := range []string{
		`{"name": "shop", "kind": "mysql", "from_env": "TARGET_SHOP_URL", "to_env": "SOURCE_SHOP_URL"}`,
		`{"name": "shop", "kind": "mysql", "from_env": "SOURCE_SHOP_URL", "to_env": "TARGET_SHOP_URL", "publication": "p"}`,
		`{"name": "shop", "kind": "mysql", "from_env": "SOURCE_SHOP_URL", "to_env": "TARGET_SHOP_URL", "back_env": "STANDBY_SHOP_URL", "back_from_env": "TARGET_SHOP_BACK"}`,
		`{"name": "cache", "kind": "redis", "from_env": "SOURCE_CACHE_URL", "to_env": "TARGET_CACHE_URL", "copy_schema": true}`,
		`{"name": "cache", "kind": "redis", "from": "s3://a", "to": "gs://b"}`,
	} {
		if _, err := ParseMove([]byte(head + d + `]}`)); err == nil {
			t.Errorf("%s was taken", d)
		}
	}
	// A way back on a Postgres database with a MySQL one beside it would
	// carry one back and not the other.
	both := head + `{"name": "pg", "kind": "postgres", "from_env": "SOURCE_PG", "to_env": "TARGET_PG", "publication": "p", "slot": "s",
		"back_env": "STANDBY_PG", "back_from_env": "TARGET_PG_BACK"},` + ok[0] + `]}`
	if _, err := ParseMove([]byte(both)); err == nil {
		t.Error("a way back on one database of two was taken")
	}
}
