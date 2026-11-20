package datasync

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zoctl/cli/internal/transfer"
	"github.com/zoctl/cli/internal/transfer/objects"
)

type bucket struct {
	mu   sync.Mutex
	data map[string]string
	puts int
}

func (b *bucket) List(_ context.Context, prefix string, fn func(objects.Object) error) error {
	b.mu.Lock()
	var all []objects.Object
	for k, v := range b.data {
		if strings.HasPrefix(k, prefix) {
			s := md5.Sum([]byte(v))
			all = append(all, objects.Object{Key: k, Size: int64(len(v)), MD5: hex.EncodeToString(s[:])})
		}
	}
	b.mu.Unlock()
	sort.Slice(all, func(i, j int) bool { return all[i].Key < all[j].Key })
	for _, o := range all {
		if err := fn(o); err != nil {
			return err
		}
	}
	return nil
}

func (b *bucket) Get(_ context.Context, key string) (io.ReadCloser, objects.Meta, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return io.NopCloser(bytes.NewReader([]byte(b.data[key]))), objects.Meta{}, nil
}

func (b *bucket) Put(_ context.Context, key string, _ int64, _ string, _ objects.Meta, body io.Reader) error {
	v, _ := io.ReadAll(body)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.data[key] = string(v)
	b.puts++
	return nil
}

const moveJSON = `{"version":1,"source":"aws","target":"gcp",
 "datasets":[{"name":"uploads","kind":"objects","from":"s3://src-bucket?region=us-east-1","to":"gs://dst-bucket"}],
 "limits":{"max_objects":100,"max_bytes":1000,"hours":1}}`

func testSync(t *testing.T, src, dst *bucket, now func() time.Time) *Sync {
	m, err := ParseMove([]byte(moveJSON))
	if err != nil {
		t.Fatal(err)
	}
	return &Sync{move: m, now: now, sets: []dataset{newObjectSet("pair", "", src, dst)}}
}

func workspace(t *testing.T) *transfer.Workspace {
	w, err := transfer.OpenWorkspace(filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	return w
}

func TestRunApprovedPlan(t *testing.T) {
	src := &bucket{data: map[string]string{"a": "one", "b": "two"}}
	dst := &bucket{data: map[string]string{"z": "target only"}}
	// A clock the loop moves forward, so the approval runs out after the
	// second catch-up round.
	var mu sync.Mutex
	clock := time.Unix(1000, 0)
	now := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return clock
	}
	s := testSync(t, src, dst, now)
	p, err := s.Plan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Datasets) != 1 || p.Datasets[0].Copy != 2 || p.Datasets[0].Extra != 1 || !transfer.ValidDigest(p.Digest) {
		t.Fatalf("plan %+v", p)
	}
	w := workspace(t)
	if _, err := s.Run(context.Background(), w, p, strings.Repeat("0", 64), RunOptions{Every: time.Second}); !errors.Is(err, ErrApproval) {
		t.Fatalf("wrong digest ran: %v", err)
	}
	var stages []string
	rounds := 0
	report := func(st Status) {
		stages = append(stages, st.Stage)
		if st.Stage == "running" {
			rounds++
			if rounds == 1 {
				src.mu.Lock()
				src.data["c"] = "three"
				src.mu.Unlock()
			}
			mu.Lock()
			clock = clock.Add(31 * time.Minute)
			mu.Unlock()
		}
	}
	st, err := s.Run(context.Background(), w, p, p.Digest, RunOptions{Every: time.Second, Report: report})
	if err != nil {
		t.Fatal(err)
	}
	if dst.data["c"] != "three" || dst.data["a"] != "one" || dst.data["z"] != "target only" {
		t.Fatalf("target %v", dst.data)
	}
	if src.puts != 0 {
		t.Fatal("the source was written")
	}
	if !st.Verified() || st.Stage != "verified" || st.Datasets[0].Copied != 3 || st.Datasets[0].Extra != 1 {
		t.Fatalf("status %+v %v", st, stages)
	}
	if stages[0] != "approved" || stages[len(stages)-1] != "verified" {
		t.Fatalf("stages %v", stages)
	}
	var saved Status
	if err := w.Load("status", &saved); err != nil || saved.Stage != "verified" {
		t.Fatalf("saved %+v %v", saved, err)
	}
	// The same approval doesn't run twice.
	if _, err := s.Run(context.Background(), w, p, p.Digest, RunOptions{Every: time.Second}); !errors.Is(err, transfer.ErrApprovalUsed) {
		t.Fatalf("second run: %v", err)
	}
}

func TestRunStopsAtTheBudget(t *testing.T) {
	src := &bucket{data: map[string]string{}}
	for i := 0; i < 150; i++ {
		src.data[string(rune('a'+i%26))+strings.Repeat("x", i)] = "v"
	}
	dst := &bucket{data: map[string]string{}}
	s := testSync(t, src, dst, time.Now)
	p, err := s.Plan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	st, err := s.Run(context.Background(), workspace(t), p, p.Digest, RunOptions{Every: time.Second})
	if !errors.Is(err, transfer.ErrBudget) && err != nil {
		t.Fatal(err)
	}
	if dst.puts != 100 || st.Stage != "paused" || st.Verified() || st.Datasets[0].Pending != 50 {
		t.Fatalf("puts %d status %+v", dst.puts, st)
	}
}

func TestRunCancelled(t *testing.T) {
	src := &bucket{data: map[string]string{"a": "1"}}
	dst := &bucket{data: map[string]string{}}
	s := testSync(t, src, dst, time.Now)
	p, _ := s.Plan(context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	st, err := s.Run(ctx, workspace(t), p, p.Digest, RunOptions{Every: time.Hour, Report: func(st Status) {
		if st.Stage == "running" {
			cancel()
		}
	}})
	if !errors.Is(err, context.Canceled) || st.Stage != "interrupted" || !st.Datasets[0].Verified {
		t.Fatalf("%+v %v", st, err)
	}
}

func TestMoveFile(t *testing.T) {
	if _, err := ParseMove([]byte(moveJSON)); err != nil {
		t.Fatal(err)
	}
	bad := []string{
		`{"version":2,"source":"aws","target":"gcp","datasets":[{"name":"a","kind":"objects","from":"x","to":"y"}],"limits":{"max_objects":1,"max_bytes":1,"hours":1}}`,
		`{"version":1,"source":"aws","target":"aws","datasets":[{"name":"a","kind":"objects","from":"x","to":"y"}],"limits":{"max_objects":1,"max_bytes":1,"hours":1}}`,
		`{"version":1,"source":"aws","target":"gcp","datasets":[{"name":"a","kind":"objects","from":"x","to":"y"}],"limits":{"max_objects":1,"max_bytes":1,"hours":0}}`,
		`{"version":1,"source":"aws","target":"gcp","datasets":[{"name":"a","kind":"objects","from":"x","to":"x"}],"limits":{"max_objects":1,"max_bytes":1,"hours":1}}`,
		`{"version":1,"source":"aws","target":"gcp","datasets":[{"name":"a","kind":"objects","from":"x","to":"y","secret":"k"}],"limits":{"max_objects":1,"max_bytes":1,"hours":1}}`,
		`{"version":1,"source":"aws","target":"gcp","datasets":[{"name":"a","kind":"postgres","from_env":"TARGET_DB","to_env":"SOURCE_DB","publication":"zoctl","slot":"zoctl"}],"limits":{"max_objects":1,"max_bytes":1,"hours":1}}`,
		`{"version":1,"source":"aws","target":"gcp","datasets":[{"name":"a","kind":"postgres","from_env":"SOURCE_DB","to_env":"TARGET_DB","publication":"z; drop","slot":"zoctl"}],"limits":{"max_objects":1,"max_bytes":1,"hours":1}}`,
		`{"version":1,"source":"aws","target":"gcp","datasets":[{"name":"a","kind":"objects","from":"x","to":"y"},{"name":"a","kind":"objects","from":"x","to":"z"}],"limits":{"max_objects":1,"max_bytes":1,"hours":1}}`,
		`{"version":1,"move":"XYZ","source":"aws","target":"gcp","datasets":[{"name":"a","kind":"objects","from":"x","to":"y"}],"limits":{"max_objects":1,"max_bytes":1,"hours":1}}`,
	}
	for _, b := range bad {
		if _, err := ParseMove([]byte(b)); !errors.Is(err, ErrMove) {
			t.Errorf("accepted %s", b)
		}
	}
}

func TestOpenReadsEachSidesKey(t *testing.T) {
	m, _ := ParseMove([]byte(moveJSON))
	asked := map[string]bool{}
	env := func(k string) string {
		asked[k] = true
		// A key that makes tokens wins over a token when it's set, and
		// "x" is no key.
		if strings.HasSuffix(k, "_SERVICE_ACCOUNT_KEY") {
			return ""
		}
		return "x"
	}
	s, err := Open(m, Options{Env: env})
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	if !asked["SOURCE_AWS_ACCESS_KEY_ID"] || !asked["TARGET_GOOGLE_OAUTH_ACCESS_TOKEN"] || asked["AWS_ACCESS_KEY_ID"] {
		t.Fatalf("asked %v", asked)
	}
	if _, err := Open(m, Options{Env: func(string) string { return "" }}); err == nil {
		t.Fatal("opened without keys")
	}
}
