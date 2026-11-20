package objects

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zoctl/cli/internal/transfer"
)

// memory is a bucket in memory. It checks a known MD5 the way S3 checks
// Content-MD5, and counts writes.
type memory struct {
	mu      sync.Mutex
	objects map[string][]byte
	metas   map[string]Meta
	noMD5   bool
	puts    int
	gets    int
	deletes int
	// onGet runs before a Get returns, to change the bucket under a copy.
	onGet func(key string)
}

func newMemory(objs map[string]string) *memory {
	m := &memory{objects: map[string][]byte{}, metas: map[string]Meta{}}
	for k, v := range objs {
		m.objects[k] = []byte(v)
	}
	return m
}

func sum(b []byte) string {
	h := md5.Sum(b)
	return hex.EncodeToString(h[:])
}

func (m *memory) List(_ context.Context, prefix string, fn func(Object) error) error {
	m.mu.Lock()
	var keys []string
	for k := range m.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	var objs []Object
	for _, k := range keys {
		o := Object{Key: k, Size: int64(len(m.objects[k]))}
		// A side with no MD5 still gives a tag that changes with the object,
		// as a multipart ETag does.
		if m.noMD5 {
			o.Tag = "tag-" + sum(m.objects[k])
		} else {
			o.MD5 = sum(m.objects[k])
		}
		objs = append(objs, o)
	}
	m.mu.Unlock()
	for _, o := range objs {
		if err := fn(o); err != nil {
			return err
		}
	}
	return nil
}

func (m *memory) Get(_ context.Context, key string) (io.ReadCloser, Meta, error) {
	if m.onGet != nil {
		m.onGet(key)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gets++
	b, ok := m.objects[key]
	if !ok {
		return nil, Meta{}, ErrStatus
	}
	return io.NopCloser(bytes.NewReader(append([]byte{}, b...))), m.metas[key], nil
}

func (m *memory) Put(_ context.Context, key string, size int64, md5hex string, meta Meta, body io.Reader) error {
	b, err := io.ReadAll(body)
	if err != nil {
		return ErrTransport
	}
	if int64(len(b)) != size {
		return ErrTransport
	}
	if md5hex != "" && sum(b) != md5hex {
		return errChanged
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[key] = b
	m.metas[key] = meta
	m.puts++
	return nil
}

func budget(objects, bytes int64) *transfer.Budget {
	return transfer.NewBudget(transfer.Limits{MaxObjects: objects, MaxBytes: bytes, Until: time.Now().Add(time.Hour)}, nil)
}

func TestSourceHasNoWrite(t *testing.T) {
	var s any = ReadOnly(newMemory(nil))
	if _, ok := s.(Store); ok {
		t.Fatal("a Source can be written")
	}
	if _, ok := s.(interface {
		Delete(context.Context, string) error
	}); ok {
		t.Fatal("a Source can delete")
	}
}

func TestPlanCopyVerify(t *testing.T) {
	ctx := context.Background()
	src := newMemory(map[string]string{"a/1": "one", "a/2": "two", "a/3": "three", "b/1": "other prefix"})
	dst := newMemory(map[string]string{"a/1": "one", "a/2": "TWO", "a/9": "only on the target"})
	p, err := MakePlan(ctx, ReadOnly(src), dst, "pair", "a/")
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Copy) != 2 || p.Copy[0].Key != "a/2" || p.Copy[1].Key != "a/3" || p.Same != 1 || p.Extra != 1 || p.CopyBytes != 8 {
		t.Fatalf("plan %+v", p)
	}
	if !transfer.ValidDigest(p.Digest) {
		t.Fatal("digest")
	}
	if q, _ := MakePlan(ctx, ReadOnly(src), dst, "other pair", "a/"); q.Digest == p.Digest {
		t.Fatal("the label isn't in the digest")
	}
	v, err := Verify(ctx, ReadOnly(src), dst, "a/")
	if err != nil || v.OK() || v.Missing != 1 || v.Different != 1 || v.Extra != 1 || v.Source != 3 || v.Target != 3 {
		t.Fatalf("verify before %+v %v", v, err)
	}
	prog, err := Copy(ctx, ReadOnly(src), dst, p.Copy, budget(10, 100))
	if err != nil || prog.Copied != 2 || prog.Bytes != 8 || prog.Failed != 0 {
		t.Fatalf("copy %+v %v", prog, err)
	}
	v, err = Verify(ctx, ReadOnly(src), dst, "a/")
	if err != nil || !v.OK() || v.Extra != 1 {
		t.Fatalf("verify after %+v %v", v, err)
	}
	if _, ok := dst.objects["a/9"]; !ok || dst.deletes != 0 {
		t.Fatal("a target-only object went away")
	}
	if src.puts != 0 {
		t.Fatal("the source was written")
	}
	// Nothing left to do.
	r, err := Round(ctx, ReadOnly(src), dst, "pair", "a/", budget(10, 100))
	if err != nil || r.Planned != 0 || r.Copied != 0 || r.Extra != 1 {
		t.Fatalf("round %+v %v", r, err)
	}
}

// slashing is a memory bucket that holds a backslash as a slash, the way
// Azure Blob Storage does whatever the request said.
type slashing struct{ *memory }

func (s slashing) Name(key string) string { return strings.ReplaceAll(key, `\`, "/") }

func (s slashing) Get(ctx context.Context, key string) (io.ReadCloser, Meta, error) {
	return s.memory.Get(ctx, s.Name(key))
}

func (s slashing) Put(ctx context.Context, key string, size int64, md5hex string, meta Meta, body io.Reader) error {
	return s.memory.Put(ctx, s.Name(key), size, md5hex, meta, body)
}

// A key with a backslash goes to a target that holds it as a slash, then
// verifies and stays put, both ways (20 November 2026: an S3 key
// `docs/caf\xc3\xa9.txt` was copied to Azure every round and never verified).
func TestABackslashIsTheTargetsSlash(t *testing.T) {
	ctx := context.Background()
	src := newMemory(map[string]string{`docs/caf\xc3\xa9.txt`: "unicode", "docs/a.txt": "plain"})
	dst := slashing{newMemory(nil)}
	p, err := MakePlan(ctx, ReadOnly(src), dst, "pair", "docs/")
	if err != nil || len(p.Copy) != 2 || p.Extra != 0 {
		t.Fatalf("plan %+v %v", p, err)
	}
	for _, o := range p.Copy {
		if o.Key == `docs/caf\xc3\xa9.txt` && o.To != "docs/caf/xc3/xa9.txt" || o.Key == "docs/a.txt" && o.To != "" {
			t.Fatalf("copy %q to %q", o.Key, o.To)
		}
	}
	if prog, err := Copy(ctx, ReadOnly(src), dst, p.Copy, budget(10, 100)); err != nil || prog.Copied != 2 {
		t.Fatalf("copy %+v %v", prog, err)
	}
	if _, ok := dst.objects["docs/caf/xc3/xa9.txt"]; !ok || len(dst.objects) != 2 {
		t.Fatalf("target holds %d objects", len(dst.objects))
	}
	if v, err := Verify(ctx, ReadOnly(src), dst, "docs/"); err != nil || !v.OK() || v.Extra != 0 {
		t.Fatalf("verify %+v %v", v, err)
	}
	if r, err := Round(ctx, ReadOnly(src), dst, "pair", "docs/", budget(10, 100)); err != nil || r.Planned != 0 || r.Extra != 0 {
		t.Fatalf("round %+v %v", r, err)
	}

	// The way back: the old side's backslash key is the target's slash one,
	// and a change made on the target goes back under the old side's key.
	dst.objects["docs/caf/xc3/xa9.txt"] = []byte("changed")
	back, err := MakePlan(ctx, ReadOnly(dst), src, "back", "docs/")
	if err != nil || len(back.Copy) != 1 || back.Copy[0].To != `docs/caf\xc3\xa9.txt` || back.Extra != 0 || back.Same != 1 {
		t.Fatalf("plan back %+v %v", back, err)
	}
	if prog, err := Copy(ctx, ReadOnly(dst), src, back.Copy, budget(10, 100)); err != nil || prog.Copied != 1 {
		t.Fatalf("copy back %+v %v", prog, err)
	}
	if got := string(src.objects[`docs/caf\xc3\xa9.txt`]); got != "changed" || len(src.objects) != 2 {
		t.Fatalf("old side holds %q in %d objects", got, len(src.objects))
	}
	if v, err := Verify(ctx, ReadOnly(dst), src, "docs/"); err != nil || !v.OK() || v.Extra != 0 {
		t.Fatalf("verify back %+v %v", v, err)
	}
}

// Two keys a target holds as one can't both move: the plan says so instead
// of copying one over the other every round, and names neither.
func TestKeysOneNameApartClash(t *testing.T) {
	ctx := context.Background()
	src := newMemory(map[string]string{`a\b`: "one", "a/b": "two"})
	dst := slashing{newMemory(nil)}
	if _, err := MakePlan(ctx, ReadOnly(src), dst, "pair", ""); !errors.Is(err, ErrClash) || strings.Contains(err.Error(), "a/b") {
		t.Fatalf("plan error %v", err)
	}
	if _, err := Verify(ctx, ReadOnly(src), dst, ""); !errors.Is(err, ErrClash) {
		t.Fatalf("verify error %v", err)
	}
	if dst.puts != 0 {
		t.Fatal("a clash was copied")
	}
}

func TestCopyChangedSource(t *testing.T) {
	ctx := context.Background()
	src := newMemory(map[string]string{"k": "before", "j": "stays"})
	dst := newMemory(nil)
	p, err := MakePlan(ctx, ReadOnly(src), dst, "pair", "")
	if err != nil {
		t.Fatal(err)
	}
	// Same size, new bytes: the MD5 catches it.
	src.onGet = func(key string) {
		if key == "k" {
			src.objects["k"] = []byte("BEFORE")
		}
	}
	prog, err := Copy(ctx, ReadOnly(src), dst, p.Copy, budget(10, 100))
	if err != nil || prog.Copied != 1 || prog.Failed != 1 {
		t.Fatalf("copy %+v %v", prog, err)
	}
	if _, ok := dst.objects["k"]; ok {
		t.Fatal("a changed object was written")
	}
	src.onGet = nil
	r, err := Round(ctx, ReadOnly(src), dst, "pair", "", budget(10, 100))
	if err != nil || r.Copied != 1 {
		t.Fatalf("catch-up %+v %v", r, err)
	}
	if string(dst.objects["k"]) != "BEFORE" {
		t.Fatal("catch-up didn't copy the new bytes")
	}
}

func TestCopyGrewWithoutMD5(t *testing.T) {
	ctx := context.Background()
	src := newMemory(map[string]string{"k": "short"})
	src.noMD5 = true
	dst := newMemory(nil)
	p, _ := MakePlan(ctx, ReadOnly(src), dst, "pair", "")
	src.onGet = func(string) { src.objects["k"] = []byte("much longer now") }
	prog, err := Copy(ctx, ReadOnly(src), dst, p.Copy, budget(10, 100))
	if err != nil || prog.Failed != 1 || prog.Copied != 0 {
		t.Fatalf("copy %+v %v", prog, err)
	}
}

func TestCopyBudget(t *testing.T) {
	ctx := context.Background()
	src := newMemory(map[string]string{"1": "aaaa", "2": "bbbb", "3": "cccc"})
	dst := newMemory(nil)
	p, _ := MakePlan(ctx, ReadOnly(src), dst, "pair", "")
	prog, err := Copy(ctx, ReadOnly(src), dst, p.Copy, budget(10, 9))
	if !errors.Is(err, transfer.ErrBudget) || prog.Copied != 2 || dst.puts != 2 {
		t.Fatalf("copy %+v %v", prog, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := Copy(canceled, ReadOnly(src), newMemory(nil), p.Copy, budget(10, 100)); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled copy: %v", err)
	}
}

func TestListingBound(t *testing.T) {
	defer func(n int) { maxListed = n }(maxListed)
	maxListed = 50
	big := listerFunc(func(fn func(Object) error) error {
		for i := 0; i < 1000; i++ {
			if err := fn(Object{Key: strconv.Itoa(i), Size: 1}); err != nil {
				return err
			}
		}
		return nil
	})
	_, err := MakePlan(context.Background(), ReadOnly(big), newMemory(nil), "pair", "")
	if !errors.Is(err, ErrTooMany) {
		t.Fatalf("%v", err)
	}
}

type listerFunc func(fn func(Object) error) error

func (l listerFunc) List(_ context.Context, _ string, fn func(Object) error) error { return l(fn) }
func (l listerFunc) Get(context.Context, string) (io.ReadCloser, Meta, error) {
	return nil, Meta{}, ErrStatus
}

// A source object written again at the same size, where a side lists no
// MD5: only reading the two tells them apart.
func TestSameSizeChangeWithNoMD5(t *testing.T) {
	ctx := context.Background()
	for _, side := range []string{"source", "target", "both"} {
		src := newMemory(map[string]string{"k": "AAAA", "same": "1234"})
		dst := newMemory(map[string]string{"k": "BBBB", "same": "1234"})
		src.noMD5 = side != "target"
		dst.noMD5 = side != "source"
		pair := NewPair(ReadOnly(src), dst, "pair", "")
		p, err := pair.Plan(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(p.Copy) != 1 || p.Copy[0].Key != "k" || p.Same != 1 {
			t.Fatalf("%s lists no MD5: plan copies %d, same %d", side, len(p.Copy), p.Same)
		}
		v, err := pair.Verify(ctx)
		if err != nil || v.OK() || v.Different != 1 {
			t.Fatalf("%s lists no MD5: verify %+v %v", side, v, err)
		}
		if _, err := pair.Copy(ctx, p.Copy, budget(10, 100)); err != nil {
			t.Fatal(err)
		}
		if v, err = pair.Verify(ctx); err != nil || !v.OK() {
			t.Fatalf("%s lists no MD5: after the copy %+v %v", side, v, err)
		}
		// A pair read and found the same isn't read again until it changes.
		reads := src.gets + dst.gets
		if v, err = pair.Verify(ctx); err != nil || !v.OK() || src.gets+dst.gets != reads {
			t.Fatalf("%s lists no MD5: read again, %d then %d", side, reads, src.gets+dst.gets)
		}
		src.mu.Lock()
		src.objects["same"] = []byte("4321")
		src.mu.Unlock()
		if v, err = pair.Verify(ctx); err != nil || v.OK() {
			t.Fatalf("%s lists no MD5: a change after the check went unseen: %+v %v", side, v, err)
		}
		if src.puts != 0 {
			t.Fatal("the source was written")
		}
	}
}

// A pair that can't be read to compare is copied by a plan and counts as
// different in a check.
func TestUnreadablePairIsNotCalledTheSame(t *testing.T) {
	ctx := context.Background()
	src := newMemory(map[string]string{"k": "AAAA"})
	src.noMD5 = true
	dst := newMemory(map[string]string{"k": "AAAA"})
	src.onGet = func(string) {
		src.mu.Lock()
		delete(src.objects, "k")
		src.mu.Unlock()
	}
	pair := NewPair(ReadOnly(src), dst, "pair", "")
	p, err := pair.Plan(ctx)
	if err != nil || len(p.Copy) != 1 {
		t.Fatalf("plan %+v %v", p, err)
	}
	src.onGet = nil
	src.objects["k"] = []byte("AAAA")
	src.onGet = func(string) {
		src.mu.Lock()
		delete(src.objects, "k")
		src.mu.Unlock()
	}
	if v, err := pair.Verify(ctx); err != nil || v.OK() {
		t.Fatalf("verify %+v %v", v, err)
	}
}

// A source object deleted between the listing and its read is one object,
// not the end of the copy, and what it reserved goes back to the budget.
func TestCopySkipsAnObjectThatIsGone(t *testing.T) {
	ctx := context.Background()
	src := newMemory(map[string]string{"a": "1", "b": "22", "c": "333"})
	src.onGet = func(key string) {
		if key == "a" {
			src.mu.Lock()
			delete(src.objects, "a")
			src.mu.Unlock()
		}
	}
	dst := newMemory(nil)
	p, err := MakePlan(ctx, ReadOnly(src), dst, "pair", "")
	if err != nil {
		t.Fatal(err)
	}
	b := budget(10, 100)
	prog, err := Copy(ctx, ReadOnly(src), dst, p.Copy, b)
	if err != nil || prog.Copied != 2 || prog.Failed != 1 || prog.Bytes != 5 {
		t.Fatalf("copy %+v %v", prog, err)
	}
	if n, bytes := b.Used(); n != 2 || bytes != 5 {
		t.Fatalf("budget spent %d objects, %d bytes", n, bytes)
	}
	if string(dst.objects["b"]) != "22" || string(dst.objects["c"]) != "333" {
		t.Fatalf("target %v", dst.objects)
	}
}

// A source that fails every read ends the copy after a few objects in a
// row, not after the first and not after all of them.
func TestCopyStopsWhenEveryReadFails(t *testing.T) {
	ctx := context.Background()
	objs := map[string]string{}
	for i := 0; i < 12; i++ {
		objs["k"+strconv.Itoa(i)] = "v"
	}
	src := newMemory(objs)
	dst := newMemory(nil)
	p, err := MakePlan(ctx, ReadOnly(src), dst, "pair", "")
	if err != nil {
		t.Fatal(err)
	}
	src.mu.Lock()
	src.objects = map[string][]byte{}
	src.mu.Unlock()
	b := budget(100, 100)
	prog, err := Copy(ctx, ReadOnly(src), dst, p.Copy, b)
	if !errors.Is(err, ErrStatus) || prog.Failed != maxFailedInARow || prog.Copied != 0 {
		t.Fatalf("copy %+v %v", prog, err)
	}
	if n, _ := b.Used(); n != 0 {
		t.Fatalf("budget spent %d objects on reads that failed", n)
	}
}
