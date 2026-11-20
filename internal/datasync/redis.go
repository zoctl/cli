package datasync

// A redis dataset copies one Redis database key by key. A managed Redis
// doesn't let another server or a client become its replica, so every pass
// reads the whole keyspace: each key the target is missing, or holds
// otherwise, is copied with DUMP and RESTORE and keeps what's left of its
// time to live, and a key the source no longer has is removed from the
// target. Start is the first pass and each round another. Check is a pass
// that writes nothing.
//
// The source is only read. The target holds only what zoctl put there, so
// a pass can always run again, after a failure or on the next approval.

import (
	"context"
	"errors"
	"io/fs"
	"strconv"
	"strings"
	"time"

	"github.com/zoctl/cli/internal/transfer"
)

// ErrRedis means a Redis step failed. The server's message isn't kept: it
// can name keys and values.
var ErrRedis = errors.New("datasync: a redis step failed")

// Finding codes only a redis dataset reports.
const (
	// findingRedisCluster: either side is a cluster, whose keys are spread
	// over servers a single connection doesn't reach.
	findingRedisCluster = "redis_cluster_unsupported"
	// findingTargetOlder: the target runs an older Redis than the source,
	// which may not take the source's DUMP format.
	findingTargetOlder = "target_version_older"
)

// rdValue is one key as DUMP and PTTL give it. TTL is -1 for a key that
// doesn't expire.
type rdValue struct {
	Present bool
	Payload string
	TTL     time.Duration
}

// rdItem is a key to write.
type rdItem struct {
	Key string
	rdValue
}

// rdSide is what both sides are read for.
type rdSide interface {
	// Info reads INFO's server and cluster sections.
	Info(ctx context.Context) (map[string]string, error)
	Size(ctx context.Context) (int64, error)
	Scan(ctx context.Context, cursor uint64) ([]string, uint64, error)
	Dump(ctx context.Context, keys []string) ([]rdValue, error)
	Exists(ctx context.Context, keys []string) ([]bool, error)
	// Content is a key's value in a form that's the same for the same
	// value however each server holds it, for when two DUMPs differ.
	Content(ctx context.Context, key string) (string, error)
	Close()
}

// rdTarget is the side written.
type rdTarget interface {
	rdSide
	Restore(ctx context.Context, items []rdItem) error
	Delete(ctx context.Context, keys []string) error
}

// rdState is what the workspace keeps of a dataset: that this target holds
// this source's keys.
type rdState struct {
	Source string `json:"source"`
	Target string `json:"target"`
}

type redisSet struct {
	name         string
	src          rdSide
	dst          rdTarget
	srcID, dstID string // the URLs' server and database, hashed
	srcDB, dstDB int
	w            *transfer.Workspace
}

// rdBatch is how many keys go in one pipeline.
const rdBatch = 200

func (s *redisSet) stateName() string { return "redis-" + hashID(s.name) }

func (s *redisSet) Close() {
	s.src.Close()
	s.dst.Close()
}

func (s *redisSet) Plan(ctx context.Context) (DatasetPlan, error) {
	plan := DatasetPlan{Kind: "redis"}
	if s.w == nil {
		return plan, transfer.ErrWorkspace
	}
	block := func(code string) {
		for _, f := range plan.Findings {
			if f == code {
				return
			}
		}
		plan.Findings = append(plan.Findings, code)
		plan.Blocked = true
	}
	si, err := s.src.Info(ctx)
	if err != nil {
		return plan, err
	}
	di, err := s.dst.Info(ctx)
	if err != nil {
		return plan, err
	}
	// Two names for one server have the same run ID.
	if s.srcID == s.dstID || si["run_id"] != "" && si["run_id"] == di["run_id"] && s.srcDB == s.dstDB {
		block(findingSameDatabase)
	}
	if si["cluster_enabled"] == "1" || di["cluster_enabled"] == "1" {
		block(findingRedisCluster)
	}
	if olderVersion(di["redis_version"], si["redis_version"]) {
		block(findingTargetOlder)
	}
	// DUMP is what the source's role has to be allowed: try it on a key.
	keys, _, err := s.src.Scan(ctx, 0)
	if err != nil {
		block(findingPrerequisite)
	} else if len(keys) > 0 {
		if _, err := s.src.Dump(ctx, keys[:1]); err != nil {
			block(findingPrerequisite)
		}
	}
	n, err := s.src.Size(ctx)
	if err != nil {
		return plan, err
	}
	m, err := s.dst.Size(ctx)
	if err != nil {
		return plan, err
	}
	state := "new"
	var saved rdState
	switch err := s.w.Load(s.stateName(), &saved); {
	case err == nil && saved.Source == s.srcID && saved.Target == s.dstID:
		state = "resume"
	case err != nil && !errors.Is(err, fs.ErrNotExist):
		return plan, err
	}
	if m > 0 && state == "new" {
		block(findingNotEmpty)
	}
	plan.Copy, plan.Extra = n, m
	if state == "resume" {
		plan.Extra = 0
	}
	plan.Digest = transfer.Digest([]byte("redis-plan-v1"), []byte(state), []byte(s.srcID), []byte(s.dstID),
		[]byte(strconv.FormatInt(n, 10)), []byte(strconv.FormatInt(m, 10)))
	plan.work = state
	return plan, nil
}

// olderVersion reports whether version a is before b. Either unreadable
// reads as not older: the copy's RESTORE refuses a format it can't take.
func olderVersion(a, b string) bool {
	pa, pb := strings.Split(a, "."), strings.Split(b, ".")
	for i := 0; i < len(pa) && i < len(pb); i++ {
		x, err1 := strconv.Atoi(pa[i])
		y, err2 := strconv.Atoi(pb[i])
		if err1 != nil || err2 != nil {
			return false
		}
		if x != y {
			return x < y
		}
	}
	return false
}

func (s *redisSet) Start(ctx context.Context, plan DatasetPlan, b *transfer.Budget) (DatasetStatus, error) {
	if plan.work != "new" && plan.work != "resume" {
		return DatasetStatus{Kind: "redis"}, ErrMove
	}
	// Said before the first write, so a pass cut short is taken up again.
	if err := s.w.Save(s.stateName(), rdState{Source: s.srcID, Target: s.dstID}); err != nil {
		return DatasetStatus{Kind: "redis"}, err
	}
	return s.pass(ctx, b)
}

func (s *redisSet) Round(ctx context.Context, b *transfer.Budget) (DatasetStatus, error) {
	return s.pass(ctx, b)
}

func (s *redisSet) Check(ctx context.Context) (DatasetStatus, error) {
	st, err := s.pass(ctx, nil)
	if err != nil {
		return st, err
	}
	st.Verified = st.Pending == 0 && st.Extra == 0
	return st, nil
}

// Stop has nothing to end: a pass runs only inside a call.
func (s *redisSet) Stop(context.Context) error { return nil }

// pass compares every key, and with a budget writes the target: Copied and
// Bytes are what it wrote, Pending the keys that differed, and Extra the
// keys only the target had. Without one it only counts.
func (s *redisSet) pass(ctx context.Context, b *transfer.Budget) (DatasetStatus, error) {
	st := DatasetStatus{Kind: "redis"}
	write := b != nil
	var cursor uint64
	for {
		keys, next, err := s.src.Scan(ctx, cursor)
		if err != nil {
			return st, err
		}
		for len(keys) > 0 {
			n := min(len(keys), rdBatch)
			if err := s.compare(ctx, keys[:n], b, &st); err != nil {
				return st, err
			}
			keys = keys[n:]
		}
		if cursor = next; cursor == 0 {
			break
		}
	}
	if write {
		// Once the target holds what the source does, Pending says what
		// this pass had to fix.
		st.Pending = 0
	}
	// What the target has that the source no longer does.
	cursor = 0
	for {
		keys, next, err := s.dst.Scan(ctx, cursor)
		if err != nil {
			return st, err
		}
		for len(keys) > 0 {
			n := min(len(keys), rdBatch)
			there, err := s.src.Exists(ctx, keys[:n])
			if err != nil {
				return st, err
			}
			if len(there) != n {
				return st, ErrRedis
			}
			var gone []string
			for i, ok := range there {
				if !ok {
					gone = append(gone, keys[i])
				}
			}
			st.Extra += int64(len(gone))
			if write && len(gone) > 0 {
				if err := s.dst.Delete(ctx, gone); err != nil {
					return st, err
				}
			}
			keys = keys[n:]
		}
		if cursor = next; cursor == 0 {
			break
		}
	}
	return st, nil
}

// compare reads a batch of keys on both sides and, writing, copies the
// ones that differ.
func (s *redisSet) compare(ctx context.Context, keys []string, b *transfer.Budget, st *DatasetStatus) error {
	from, err := s.src.Dump(ctx, keys)
	if err != nil {
		return err
	}
	to, err := s.dst.Dump(ctx, keys)
	if err != nil {
		return err
	}
	if len(from) != len(keys) || len(to) != len(keys) {
		return ErrRedis
	}
	var copy []rdItem
	var bytes int64
	for i, k := range keys {
		v := from[i]
		// Gone since the scan, or expiring as it's read: the next pass
		// removes it from the target if the target still has it.
		if !v.Present || v.TTL == 0 {
			continue
		}
		if to[i].Present && (to[i].Payload == v.Payload || s.same(ctx, k)) {
			continue
		}
		st.Pending++
		copy = append(copy, rdItem{Key: k, rdValue: v})
		bytes += int64(len(v.Payload))
	}
	if b == nil || len(copy) == 0 {
		return nil
	}
	if err := b.Take(int64(len(copy)), bytes); err != nil {
		return err
	}
	if err := s.dst.Restore(ctx, copy); err != nil {
		b.Return(int64(len(copy)), bytes)
		return err
	}
	st.Copied += int64(len(copy))
	st.Bytes += bytes
	return nil
}

// same reports whether a key holds the same value on both sides when their
// DUMPs differ, as they do when the two servers keep it differently. One
// that can't be read reads as different, so it's copied again.
func (s *redisSet) same(ctx context.Context, key string) bool {
	a, err := s.src.Content(ctx, key)
	if err != nil {
		return false
	}
	b, err := s.dst.Content(ctx, key)
	return err == nil && a == b
}
