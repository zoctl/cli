package transfer

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestLimitsValidate(t *testing.T) {
	ok := Limits{MaxObjects: 1, MaxBytes: 1, Until: time.Unix(1, 0)}
	if ok.Validate() != nil {
		t.Fatal("valid limits refused")
	}
	for _, l := range []Limits{{MaxBytes: 1, Until: time.Unix(1, 0)}, {MaxObjects: 1, Until: time.Unix(1, 0)}, {MaxObjects: 1, MaxBytes: 1}} {
		if !errors.Is(l.Validate(), ErrLimits) {
			t.Errorf("%+v accepted", l)
		}
	}
}

func TestBudget(t *testing.T) {
	now := time.Unix(100, 0)
	b := NewBudget(Limits{MaxObjects: 2, MaxBytes: 10, Until: time.Unix(200, 0)}, func() time.Time { return now })
	if err := b.Take(1, 6); err != nil {
		t.Fatal(err)
	}
	if err := b.Take(1, 5); !errors.Is(err, ErrBudget) {
		t.Fatalf("bytes over the limit: %v", err)
	}
	if err := b.Take(1, 4); err != nil {
		t.Fatal(err)
	}
	if err := b.Take(1, 0); !errors.Is(err, ErrBudget) {
		t.Fatalf("objects over the limit: %v", err)
	}
	if o, n := b.Used(); o != 2 || n != 10 {
		t.Fatalf("used %d %d", o, n)
	}
	late := NewBudget(Limits{MaxObjects: 2, MaxBytes: 10, Until: time.Unix(200, 0)}, func() time.Time { return time.Unix(200, 0) })
	if !late.Expired() || !errors.Is(late.Take(0, 0), ErrBudget) {
		t.Fatal("an expired approval still spends")
	}
	if !errors.Is(b.Take(-1, 0), ErrBudget) {
		t.Fatal("negative take accepted")
	}
}

func TestBudgetConcurrent(t *testing.T) {
	b := NewBudget(Limits{MaxObjects: 100, MaxBytes: 1 << 20, Until: time.Now().Add(time.Hour)}, nil)
	var wg sync.WaitGroup
	var mu sync.Mutex
	taken := 0
	for i := 0; i < 300; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if b.Take(1, 1) == nil {
				mu.Lock()
				taken++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if taken != 100 {
		t.Fatalf("took %d of 100", taken)
	}
}

func TestDigest(t *testing.T) {
	if Digest([]byte("ab"), []byte("c")) == Digest([]byte("a"), []byte("bc")) {
		t.Fatal("parts run together")
	}
	if !ValidDigest(Digest()) || ValidDigest("x") || ValidDigest(strings.Repeat("A", 64)) {
		t.Fatal("ValidDigest")
	}
}

func TestWorkspace(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "ws")
	w, err := OpenWorkspace(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	info, err := os.Stat(dir)
	if err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("workspace mode %v %v", info.Mode(), err)
	}
	d := Digest([]byte("plan"))
	if err := w.Spend(d); err != nil {
		t.Fatal(err)
	}
	if err := w.Spend(d); !errors.Is(err, ErrApprovalUsed) {
		t.Fatalf("second spend: %v", err)
	}
	// Another handle on the same directory, as after a restart.
	w2, err := OpenWorkspace(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	if err := w2.Spend(d); !errors.Is(err, ErrApprovalUsed) {
		t.Fatalf("spend after reopen: %v", err)
	}
	if err := w.Spend("nope"); !errors.Is(err, ErrWorkspace) {
		t.Fatal("bad digest spent")
	}

	type rec struct{ A int }
	if err := w.Save("plan", rec{7}); err != nil {
		t.Fatal(err)
	}
	var got rec
	if err := w.Load("plan", &got); err != nil || got.A != 7 {
		t.Fatalf("load %v %v", got, err)
	}
	if info, err := os.Stat(filepath.Join(dir, "plan.json")); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("plan mode %v %v", info.Mode(), err)
	}
	if err := w.Load("missing", &got); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("missing: %v", err)
	}
	if err := w.Save("../x", rec{}); !errors.Is(err, ErrWorkspace) {
		t.Fatal("path name saved")
	}
}

func TestWorkspaceRefused(t *testing.T) {
	base := t.TempDir()
	open := filepath.Join(base, "open")
	if err := os.Mkdir(open, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(open, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenWorkspace(open); !errors.Is(err, ErrWorkspace) {
		t.Fatal("shared directory accepted")
	}
	private := filepath.Join(base, "private")
	if err := os.Mkdir(private, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(base, "link")
	if err := os.Symlink(private, link); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenWorkspace(link); !errors.Is(err, ErrWorkspace) {
		t.Fatal("symlink accepted")
	}
	file := filepath.Join(base, "file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenWorkspace(file); !errors.Is(err, ErrWorkspace) {
		t.Fatal("file accepted")
	}
}

func TestWorkspaceLock(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "ws")
	w, err := OpenWorkspace(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	unlock, err := w.Lock()
	if err != nil {
		t.Fatal(err)
	}
	// A second holder is refused, through this handle or another one.
	if _, err := w.Lock(); !errors.Is(err, ErrBusy) {
		t.Fatalf("second lock: %v", err)
	}
	other, err := OpenWorkspace(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if _, err := other.Lock(); !errors.Is(err, ErrBusy) {
		t.Fatalf("second workspace: %v", err)
	}
	unlock()
	again, err := other.Lock()
	if err != nil {
		t.Fatalf("after unlock: %v", err)
	}
	again()
	// The lock isn't something Save or Load can name.
	if err := w.Save(".lock", 1); !errors.Is(err, ErrWorkspace) {
		t.Fatalf("saved over the lock: %v", err)
	}
}

func TestBudgetReturn(t *testing.T) {
	b := NewBudget(Limits{MaxObjects: 2, MaxBytes: 10, Until: time.Now().Add(time.Hour)}, nil)
	if err := b.Take(2, 10); err != nil {
		t.Fatal(err)
	}
	if err := b.Take(1, 1); !errors.Is(err, ErrBudget) {
		t.Fatalf("over the limit: %v", err)
	}
	b.Return(1, 4)
	b.Return(-1, -1)
	if n, bytes := b.Used(); n != 1 || bytes != 6 {
		t.Fatalf("used %d %d", n, bytes)
	}
	b.Return(5, 50)
	if n, bytes := b.Used(); n != 0 || bytes != 0 {
		t.Fatalf("went below zero: %d %d", n, bytes)
	}
}
