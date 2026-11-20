// Package objects copies object storage from a source bucket to a target
// bucket and checks the two against each other.
//
// The source side is a Source, which has List and Get and nothing else, so no
// code here can write, overwrite or delete a source object. Objects only on
// the target are counted and left alone: nothing in this package deletes.
package objects

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/zoctl/cli/internal/transfer"
)

var (
	// ErrTooMany means a listing passed MaxObjects.
	ErrTooMany = errors.New("objects: listing too large")
	// ErrTransport means a storage call failed. It never carries the URL,
	// key or response, which may name customer data.
	ErrTransport = errors.New("objects: storage call failed")
	// ErrConfig means a bucket, endpoint or credential was refused before
	// any call.
	ErrConfig = errors.New("objects: configuration refused")
	// ErrStatus means the storage service answered with an error status.
	ErrStatus = errors.New("objects: storage refused the call")
	// ErrClash means two keys on one side are one name where a side
	// renames them, as Azure takes `a\b` and `a/b` for the same blob: one
	// would overwrite the other. It never carries either key.
	ErrClash = errors.New("objects: two keys would be one object")
)

// MaxObjects bounds one side of a listing.
const MaxObjects = 10_000_000

// maxListed is MaxObjects, lowered by tests.
var maxListed = MaxObjects

// Object is one stored object. MD5 is lowercase hex, or empty when the
// service doesn't give one, such as an S3 multipart ETag. Tag is whatever the
// listing gives that changes when the object is written again; it is only
// ever compared with an earlier Tag of the same object. Meta is set where
// the listing says what is stored with the object, as Google's and Azure's
// do; Amazon's doesn't, so its Get says.
type Object struct {
	Key  string `json:"key"`
	Size int64  `json:"size"`
	MD5  string `json:"md5,omitempty"`
	Tag  string `json:"-"`
	Meta *Meta  `json:"-"`
	// To is the key a plan writes the object to when that isn't Key: the
	// name a target that renames keys gives it, or the key the target
	// already holds it under.
	To string `json:"to,omitempty"`
}

// Namer is a bucket that holds some keys under another name. Azure Blob
// Storage takes each backslash in a blob's name as a slash, so `a\b` is
// written, listed and read as `a/b`. Name is the name it holds key under.
type Namer interface {
	Name(key string) string
}

// sameName is a pair's names where neither side renames a key.
func sameName(key string) string { return key }

// tag joins what a listing gives into an Object's Tag, or nothing when the
// listing gave nothing.
func tag(parts ...string) string {
	t := strings.Join(parts, "\x00")
	if strings.Trim(t, "\x00") == "" {
		return ""
	}
	return t
}

// Reader lists and reads a bucket. Get's Meta is what the read says is
// stored with the object; a listing that says it puts it on the Object.
type Reader interface {
	List(ctx context.Context, prefix string, fn func(Object) error) error
	Get(ctx context.Context, key string) (io.ReadCloser, Meta, error)
}

// Store is a bucket that may be written: the target. Put stores meta with
// the object, leaving out a name or value the service can't hold.
type Store interface {
	Reader
	Put(ctx context.Context, key string, size int64, md5 string, meta Meta, body io.Reader) error
}

// Source is the source side of a copy. It wraps a Reader and offers only
// calls that read, so a backend that can write is still read-only through it.
type Source struct {
	r Reader
}

// ReadOnly makes r a Source.
func ReadOnly(r Reader) Source { return Source{r: r} }

// List lists the source.
func (s Source) List(ctx context.Context, prefix string, fn func(Object) error) error {
	if s.r == nil {
		return ErrConfig
	}
	return s.r.List(ctx, prefix, fn)
}

// Get reads one source object.
func (s Source) Get(ctx context.Context, key string) (io.ReadCloser, Meta, error) {
	if s.r == nil {
		return nil, Meta{}, ErrConfig
	}
	return s.r.Get(ctx, key)
}

// NoMD5 asks the source whether an object's listed MD5 isn't one.
func (s Source) NoMD5(ctx context.Context, key string) (bool, error) {
	if s.r == nil {
		return false, ErrConfig
	}
	return noMD5(ctx, s.r, key)
}

// listAll lists one side by name: the key as a side that renames holds it.
// Two keys with one name are ErrClash.
func listAll(ctx context.Context, r Reader, prefix string, name func(string) string) (map[string]Object, error) {
	if name == nil {
		name = sameName
	}
	out := map[string]Object{}
	err := r.List(ctx, prefix, func(o Object) error {
		if len(out) >= maxListed {
			return ErrTooMany
		}
		k := name(o.Key)
		if _, ok := out[k]; ok {
			return ErrClash
		}
		out[k] = o
		return nil
	})
	return out, err
}

// Pair is one source bucket and the target bucket it is copied to. It
// remembers, for as long as the process lives, which objects it had to read
// to compare, and the MD5 of what it copied where the source listed none.
type Pair struct {
	src    Source
	dst    Store
	label  string
	prefix string
	// name is how the two sides' keys are compared: as the side that
	// renames keys holds them, in either direction, so the way back finds
	// `a\b` on the old side as the `a/b` the target holds.
	name func(string) string

	mu   sync.Mutex
	same map[string][2]string
	read map[string][2]string
}

// NewPair makes a pair. label names it in a plan's digest, so the same
// listing of a different pair of buckets gives a different plan.
func NewPair(src Source, dst Store, label, prefix string) *Pair {
	name := sameName
	if n, ok := dst.(Namer); ok {
		name = n.Name
	} else if n, ok := src.r.(Namer); ok {
		name = n.Name
	}
	return &Pair{src: src, dst: dst, label: label, prefix: prefix, name: name, same: map[string][2]string{}, read: map[string][2]string{}}
}

// prefixes are the prefix as each side holds it.
func (p *Pair) prefixes() (src, dst string) {
	src, dst = p.prefix, p.prefix
	if n, ok := p.src.r.(Namer); ok {
		src = n.Name(src)
	}
	if n, ok := p.dst.(Namer); ok {
		dst = n.Name(dst)
	}
	return src, dst
}

// to is where a copy of o goes: the key the target holds it under, the name
// a renaming target gives it, or its own key.
func (p *Pair) to(o Object, have Object, held bool) string {
	switch {
	case held:
		return have.Key
	}
	if n, ok := p.dst.(Namer); ok {
		return n.Name(o.Key)
	}
	return o.Key
}

// learn keeps the MD5 of what a copy read of o, by the writing of o it read.
func (p *Pair) learn(o Object, md5 string) {
	if v := version(o); v != "" {
		p.mu.Lock()
		p.read[o.Key] = [2]string{v, md5}
		p.mu.Unlock()
	}
}

// learned is the MD5 a copy read of this writing of o, when one did.
func (p *Pair) learned(o Object) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if got, ok := p.read[o.Key]; ok && got[0] == version(o) {
		return got[1]
	}
	return ""
}

// matches reports whether the target's t holds what the source's s does.
// Sizes that differ, or two MD5s that are the same, settle it from the
// listings. When a side lists no MD5 (an S3 multipart ETag, a composed
// Cloud Storage object, an Azure blob stored without one) the listings
// can't tell an object written again at the same size from one left alone,
// so that side is read and hashed, unless this process copied that writing
// of the source and knows its MD5 already. A listed MD5 that would count
// against the object is first checked to be one: Amazon's ETag for an
// object under a KMS key isn't, and is treated as no MD5. A pair found the
// same is remembered by both sides' tags and read again only once either
// changes.
func (p *Pair) matches(ctx context.Context, s, t Object) (bool, error) {
	if s.Size != t.Size {
		return false, nil
	}
	a, b := s.MD5, t.MD5
	if a != "" && a == b {
		return true, nil
	}
	tags := [2]string{version(s), version(t)}
	tagged := tags[0] != "" && tags[1] != ""
	if tagged {
		p.mu.Lock()
		got, ok := p.same[s.Key]
		p.mu.Unlock()
		if ok && got == tags {
			return true, nil
		}
	}
	if a != "" {
		no, err := p.src.NoMD5(ctx, s.Key)
		if err != nil {
			return false, err
		}
		if no {
			a = ""
		}
	}
	if b != "" {
		no, err := noMD5(ctx, p.dst, t.Key)
		if err != nil {
			return false, err
		}
		if no {
			b = ""
		}
	}
	if a != "" && b != "" {
		return false, nil
	}
	var err error
	if a == "" {
		a = p.learned(s)
	}
	if a == "" {
		if a, err = contentMD5(ctx, p.src, s); err != nil {
			return false, err
		}
	}
	if b == "" && a != "" {
		if b, err = contentMD5(ctx, p.dst, t); err != nil {
			return false, err
		}
	}
	if a == "" || a != b {
		return false, nil
	}
	if tagged {
		p.mu.Lock()
		p.same[s.Key] = tags
		p.mu.Unlock()
	}
	return true, nil
}

// version is what names this writing of o: its tag, or its MD5 when the
// listing gave no tag.
func version(o Object) string {
	if o.Tag != "" {
		return "t" + o.Tag
	}
	if o.MD5 != "" {
		return "m" + o.MD5
	}
	return ""
}

// contentMD5 reads o and hashes it. It is "" when what was read isn't the
// size the listing gave: the object changed since.
func contentMD5(ctx context.Context, r interface {
	Get(context.Context, string) (io.ReadCloser, Meta, error)
}, o Object) (string, error) {
	body, _, err := r.Get(ctx, o.Key)
	if err != nil {
		return "", err
	}
	defer body.Close()
	h := md5.New()
	n, err := io.Copy(h, io.LimitReader(body, o.Size+1))
	if err != nil {
		return "", ErrTransport
	}
	if n != o.Size {
		return "", nil
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// Plan is what one copy would do.
type Plan struct {
	Copy      []Object `json:"copy"`
	CopyBytes int64    `json:"copy_bytes"`
	Same      int64    `json:"same"`
	Extra     int64    `json:"extra"`
	Digest    string   `json:"digest"`
}

// MakePlan compares the two sides under prefix, as a Pair's Plan does.
func MakePlan(ctx context.Context, src Source, dst Store, label, prefix string) (Plan, error) {
	return NewPair(src, dst, label, prefix).Plan(ctx)
}

// Plan compares the two sides. An object is copied when the target lacks
// it, or has a different size or MD5. Where a side lists no MD5, both
// copies are read to compare them, and one that can't be read is copied.
func (pr *Pair) Plan(ctx context.Context) (Plan, error) {
	sp, dp := pr.prefixes()
	s, err := listAll(ctx, pr.src, sp, pr.name)
	if err != nil {
		return Plan{}, err
	}
	t, err := listAll(ctx, pr.dst, dp, pr.name)
	if err != nil {
		return Plan{}, err
	}
	var p Plan
	for k, o := range s {
		have, ok := t[k]
		if ok {
			same, err := pr.matches(ctx, o, have)
			if cerr := ctx.Err(); cerr != nil {
				return Plan{}, cerr
			}
			if err == nil && same {
				p.Same++
				continue
			}
		}
		if to := pr.to(o, have, ok); to != o.Key {
			o.To = to
		}
		p.Copy = append(p.Copy, o)
		p.CopyBytes += o.Size
	}
	for k := range t {
		if _, ok := s[k]; !ok {
			p.Extra++
		}
	}
	sort.Slice(p.Copy, func(i, j int) bool { return p.Copy[i].Key < p.Copy[j].Key })
	parts := [][]byte{[]byte("objects-plan-v1"), []byte(pr.label), []byte(pr.prefix)}
	for _, o := range p.Copy {
		parts = append(parts, []byte(o.Key), []byte(strconv.FormatInt(o.Size, 10)), []byte(o.MD5))
		if o.To != "" {
			parts = append(parts, []byte("to"), []byte(o.To))
		}
	}
	parts = append(parts, []byte(strconv.FormatInt(p.Same, 10)), []byte(strconv.FormatInt(p.Extra, 10)))
	p.Digest = transfer.Digest(parts...)
	return p, nil
}

// Progress counts a copy. Failed objects changed while they were read, were
// gone, or hit a storage call that failed. They are left for the next round,
// which sees them differ and copies them again.
type Progress struct {
	Copied int64 `json:"copied"`
	Bytes  int64 `json:"bytes"`
	Failed int64 `json:"failed"`
}

// checked counts and hashes what passes through it, and refuses to go past
// the size the listing gave.
type checked struct {
	r    io.Reader
	size int64
	n    int64
	h    interface {
		io.Writer
		Sum([]byte) []byte
	}
	over bool
}

var (
	errOver = errors.New("objects: object grew while it was read")
	// errChanged means what was read didn't match the listing, so nothing
	// was committed (or the service refused it). The next round copies the
	// object again.
	errChanged = errors.New("objects: object changed while it was read")
)

// hashed tees what a backend sends into an MD5, for the paths where the
// service doesn't check one: S3 parts and Azure blocks.
type hashed struct {
	r io.Reader
	h hash.Hash
}

func newHashed(r io.Reader) *hashed { return &hashed{r: r, h: md5.New()} }

func (h *hashed) Read(p []byte) (int, error) {
	n, err := h.r.Read(p)
	h.h.Write(p[:n])
	return n, err
}

// matches is true when no MD5 is known or the bytes sent had it.
func (h *hashed) matches(want string) bool {
	return want == "" || hex.EncodeToString(h.h.Sum(nil)) == want
}

func (c *checked) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	c.h.Write(p[:n])
	if c.n > c.size {
		c.over = true
		return n, errOver
	}
	return n, err
}

func (c *checked) matches(want string) bool {
	if c.over || c.n != c.size {
		return false
	}
	return want == "" || hex.EncodeToString(c.h.Sum(nil)) == want
}

// maxFailedInARow is how many objects in a row may fail on a storage call
// before a copy stops. One object that's gone is skipped; a service that's
// down isn't asked for every key.
const maxFailedInARow = 5

// Copy copies objs from src to dst, as a Pair's Copy does.
func Copy(ctx context.Context, src Source, dst Store, objs []Object, b *transfer.Budget) (Progress, error) {
	return NewPair(src, dst, "", "").Copy(ctx, objs, b)
}

// Copy copies objs to the target, taking each object from the budget before
// it starts. An object that fails is counted and the copy goes on to the
// next: it stops at the budget's end, a cancelled context, or
// maxFailedInARow storage calls failing one after another. It never tries
// an object twice.
func (pr *Pair) Copy(ctx context.Context, objs []Object, b *transfer.Budget) (Progress, error) {
	var p Progress
	row := 0
	for _, o := range objs {
		if err := ctx.Err(); err != nil {
			return p, err
		}
		if o.Size < 0 {
			return p, ErrTransport
		}
		if err := b.Take(1, o.Size); err != nil {
			return p, err
		}
		read, err := pr.copyOne(ctx, o)
		switch {
		case err == nil:
			p.Copied++
			p.Bytes += o.Size
			row = 0
		case errors.Is(err, errChanged):
			// The source changed under the copy. The next round copies it again.
			p.Failed++
			row = 0
		default:
			if cerr := ctx.Err(); cerr != nil {
				return p, cerr
			}
			p.Failed++
			if !read {
				// Nothing left the source, so nothing was spent.
				b.Return(1, o.Size)
			}
			if row++; row >= maxFailedInARow {
				return p, err
			}
		}
	}
	return p, nil
}

// copyOne copies one object and what is stored with it. read is whether
// the source answered with it.
func (pr *Pair) copyOne(ctx context.Context, o Object) (read bool, err error) {
	body, meta, err := pr.src.Get(ctx, o.Key)
	if err != nil {
		return false, err
	}
	want := o.MD5
	if meta.NoMD5 {
		// The listing's ETag isn't the bytes' MD5, so nothing is checked
		// against it.
		want = ""
	}
	if o.Meta != nil {
		meta = *o.Meta
	}
	c := &checked{r: body, size: o.Size, h: md5.New()}
	to := o.To
	if to == "" {
		to = o.Key
	}
	err = pr.dst.Put(ctx, to, o.Size, want, meta, c)
	body.Close()
	if c.over || errors.Is(err, errChanged) || err == nil && !c.matches(want) {
		return true, errChanged
	}
	if err == nil && want == "" {
		pr.learn(o, hex.EncodeToString(c.h.Sum(nil)))
	}
	return true, err
}

// RoundResult is one catch-up pass.
type RoundResult struct {
	Planned int64 `json:"planned"`
	Copied  int64 `json:"copied"`
	Bytes   int64 `json:"bytes"`
	Failed  int64 `json:"failed"`
	Extra   int64 `json:"extra"`
}

// Round compares the sides again and copies what differs.
func Round(ctx context.Context, src Source, dst Store, label, prefix string, b *transfer.Budget) (RoundResult, error) {
	return NewPair(src, dst, label, prefix).Round(ctx, b)
}

// Round compares the sides again and copies what differs.
func (pr *Pair) Round(ctx context.Context, b *transfer.Budget) (RoundResult, error) {
	p, err := pr.Plan(ctx)
	if err != nil {
		return RoundResult{}, err
	}
	r := RoundResult{Planned: int64(len(p.Copy)), Extra: p.Extra}
	prog, err := pr.Copy(ctx, p.Copy, b)
	r.Copied, r.Bytes, r.Failed = prog.Copied, prog.Bytes, prog.Failed
	return r, err
}

// Verification compares a source and a target: counts only.
type Verification struct {
	Source      int64 `json:"source"`
	Target      int64 `json:"target"`
	Missing     int64 `json:"missing"`
	Different   int64 `json:"different"`
	Extra       int64 `json:"extra"`
	SourceBytes int64 `json:"source_bytes"`
	// Extras names what the target has that the source doesn't: a digest
	// of each such object's key, size and the listing's version of it, so
	// one object swapped for another shows though the count is the same.
	// It is empty when there are none.
	Extras string `json:"-"`
}

// OK is true when every source object is on the target as it is at the source.
func (v Verification) OK() bool { return v.Missing == 0 && v.Different == 0 }

// Verify lists both sides and compares them, as a Pair's Verify does.
func Verify(ctx context.Context, src Source, dst Store, prefix string) (Verification, error) {
	return NewPair(src, dst, "", prefix).Verify(ctx)
}

// Verify lists both sides and compares them. It reads only. Taken while the
// source's writes are paused, an OK result is the barrier a cutover needs.
// Where a side lists no MD5 it reads both copies, and one it can't read
// counts as different.
func (pr *Pair) Verify(ctx context.Context) (Verification, error) {
	sp, dp := pr.prefixes()
	s, err := listAll(ctx, pr.src, sp, pr.name)
	if err != nil {
		return Verification{}, err
	}
	t, err := listAll(ctx, pr.dst, dp, pr.name)
	if err != nil {
		return Verification{}, err
	}
	v := Verification{Source: int64(len(s)), Target: int64(len(t))}
	var extra []string
	for k, o := range s {
		v.SourceBytes += o.Size
		have, ok := t[k]
		if !ok {
			v.Missing++
			continue
		}
		same, err := pr.matches(ctx, o, have)
		if cerr := ctx.Err(); cerr != nil {
			return Verification{}, cerr
		}
		if err != nil || !same {
			v.Different++
		}
	}
	for k := range t {
		if _, ok := s[k]; !ok {
			v.Extra++
			extra = append(extra, k)
		}
	}
	if len(extra) > 0 {
		sort.Strings(extra)
		parts := [][]byte{[]byte("objects-extras-v1")}
		for _, k := range extra {
			parts = append(parts, []byte(t[k].Key), []byte(strconv.FormatInt(t[k].Size, 10)), []byte(version(t[k])))
		}
		v.Extras = transfer.Digest(parts...)
	}
	return v, nil
}

// statusError is ErrStatus with the code, which names no customer data.
func statusError(code int) error { return fmt.Errorf("%w: %d", ErrStatus, code) }
