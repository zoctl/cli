// Package transfer holds what every data sync shares: the limits one approval
// covers, the budget that enforces them, and the private workspace that keeps
// a sync's plan, checkpoints and spent approvals.
//
// A sync copies to the target side only. Nothing here can write a source: the
// readers each sync takes have no write method.
package transfer

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"regexp"
	"sync"
	"time"
)

var (
	// ErrBudget means the approval's objects, bytes or time are used up. The
	// sync stops; another one needs a new plan and a new approval.
	ErrBudget = errors.New("transfer: approved limit reached")
	// ErrApprovalUsed means this approval already started a sync.
	ErrApprovalUsed = errors.New("transfer: approval already used")
	// ErrWorkspace means the workspace is missing, shared or unreadable.
	ErrWorkspace = errors.New("transfer: workspace refused")
	// ErrLimits means the limits are missing or out of range.
	ErrLimits = errors.New("transfer: limits refused")
	// ErrBusy means a sync or a cutover already holds the workspace.
	ErrBusy = errors.New("transfer: workspace in use")
)

// Limits bound one approved sync: how many objects and bytes it may write to
// the target, and until when. An approval covers these and nothing more.
type Limits struct {
	MaxObjects int64     `json:"max_objects"`
	MaxBytes   int64     `json:"max_bytes"`
	Until      time.Time `json:"until"`
}

// Validate refuses limits that would leave a sync unbounded.
func (l Limits) Validate() error {
	if l.MaxObjects <= 0 || l.MaxBytes <= 0 || l.Until.IsZero() {
		return ErrLimits
	}
	return nil
}

// Budget spends Limits. It is safe for concurrent use.
type Budget struct {
	mu      sync.Mutex
	limits  Limits
	now     func() time.Time
	objects int64
	bytes   int64
}

// NewBudget starts a budget at zero. A nil now uses time.Now.
func NewBudget(l Limits, now func() time.Time) *Budget {
	if now == nil {
		now = time.Now
	}
	return &Budget{limits: l, now: now}
}

// Take spends objects and bytes, or refuses with ErrBudget and spends
// nothing when either would pass its limit or the time is up.
func (b *Budget) Take(objects, bytes int64) error {
	if objects < 0 || bytes < 0 {
		return ErrBudget
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.now().Before(b.limits.Until) {
		return ErrBudget
	}
	if objects > b.limits.MaxObjects-b.objects || bytes > b.limits.MaxBytes-b.bytes {
		return ErrBudget
	}
	b.objects += objects
	b.bytes += bytes
	return nil
}

// Return gives back what a Take reserved for a copy that never reached the
// target, such as a source object that was gone when it was read.
func (b *Budget) Return(objects, bytes int64) {
	if objects < 0 || bytes < 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.objects = max(0, b.objects-objects)
	b.bytes = max(0, b.bytes-bytes)
}

// Expired reports whether the approval's time is up.
func (b *Budget) Expired() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return !b.now().Before(b.limits.Until)
}

// Used reports what has been spent.
func (b *Budget) Used() (objects, bytes int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.objects, b.bytes
}

// Digest is the hex SHA-256 of each part, each prefixed by its length, so no
// two different lists of parts share a digest.
func Digest(parts ...[]byte) string {
	h := sha256.New()
	var n [8]byte
	for _, p := range parts {
		binary.BigEndian.PutUint64(n[:], uint64(len(p)))
		h.Write(n[:])
		h.Write(p)
	}
	return hex.EncodeToString(h.Sum(nil))
}

var (
	digestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
	namePattern   = regexp.MustCompile(`^[a-z0-9-]{1,64}$`)
)

// ValidDigest reports whether s is a digest as Digest writes it.
func ValidDigest(s string) bool { return digestPattern.MatchString(s) }

const (
	approvalDir  = "approvals"
	lockName     = ".lock"
	maxLoadBytes = 256 << 20
)

// Workspace is one sync's private directory. It may hold object keys and
// table names, so it is the customer's own and stays 0700.
type Workspace struct {
	root *os.Root
}

// OpenWorkspace opens dir, making it 0700 when it is missing. A symlink, a
// file or a directory others can read is refused.
func OpenWorkspace(dir string) (*Workspace, error) {
	if dir == "" {
		return nil, ErrWorkspace
	}
	if err := os.Mkdir(dir, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
		return nil, ErrWorkspace
	}
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() || info.Mode()&fs.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 {
		return nil, ErrWorkspace
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, ErrWorkspace
	}
	return &Workspace{root: root}, nil
}

// Close releases the workspace.
func (w *Workspace) Close() error { return w.root.Close() }

// Spend records that the approval of digest has started its sync. The record
// is a file created with O_EXCL, so the second use fails whether it comes
// from this process, a restart or another process on the same workspace.
func (w *Workspace) Spend(digest string) error {
	if !ValidDigest(digest) {
		return ErrWorkspace
	}
	if err := w.root.Mkdir(approvalDir, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
		return ErrWorkspace
	}
	info, err := w.root.Lstat(approvalDir)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
		return ErrWorkspace
	}
	sum := sha256.Sum256([]byte("sync\x00" + digest))
	f, err := w.root.OpenFile(approvalDir+"/"+hex.EncodeToString(sum[:]), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if errors.Is(err, fs.ErrExist) {
			return ErrApprovalUsed
		}
		return ErrWorkspace
	}
	err = f.Sync()
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return ErrWorkspace
	}
	syncDir(w.root, approvalDir)
	return nil
}

// Save writes v as name.json, whole or not at all.
func (w *Workspace) Save(name string, v any) error {
	if !namePattern.MatchString(name) {
		return ErrWorkspace
	}
	b, err := json.Marshal(v)
	if err != nil {
		return ErrWorkspace
	}
	tmp := "." + name + ".tmp"
	_ = w.root.Remove(tmp)
	f, err := w.root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return ErrWorkspace
	}
	_, err = f.Write(b)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = w.root.Rename(tmp, name+".json")
	}
	if err != nil {
		_ = w.root.Remove(tmp)
		return ErrWorkspace
	}
	syncDir(w.root, ".")
	return nil
}

// Load reads name.json into v. A missing file returns an error that wraps
// fs.ErrNotExist.
func (w *Workspace) Load(name string, v any) error {
	if !namePattern.MatchString(name) {
		return ErrWorkspace
	}
	f, err := w.root.Open(name + ".json")
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("transfer: %s missing: %w", name, fs.ErrNotExist)
		}
		return ErrWorkspace
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > maxLoadBytes {
		return ErrWorkspace
	}
	b, err := io.ReadAll(io.LimitReader(f, maxLoadBytes+1))
	if err != nil || len(b) > maxLoadBytes {
		return ErrWorkspace
	}
	if json.Unmarshal(b, v) != nil {
		return ErrWorkspace
	}
	return nil
}

func syncDir(root *os.Root, name string) {
	if d, err := root.Open(name); err == nil {
		_ = d.Sync()
		d.Close()
	}
}
