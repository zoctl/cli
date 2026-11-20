// Package datasync runs a move's data sync: the datasets a customer names in
// a move file, copied from the source to the target under one approval, then
// kept caught up until the approval's limits or time run out. After a
// cutover the same datasets can be synced the other way, from the target to
// the old side, which is how the old side gets what the target took and a
// return goes ahead (back.go).
//
// It runs on the customer's side, in their runner or on their machine. What
// it keeps is in the customer's private workspace; what it reports is counts.
package datasync

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"regexp"
	"strings"

	"github.com/zoctl/cli/internal/transfer"
)

// ErrMove means the move file was refused.
var ErrMove = errors.New("datasync: move file refused")

// moveError is ErrMove with the part of the file that was refused, in fixed
// words: nothing the file held is in them, so they can be shown and logged.
type moveError struct{ part string }

func (e *moveError) Error() string { return ErrMove.Error() }

func (e *moveError) Is(target error) bool { return target == ErrMove }

func refused(part string) error { return &moveError{part: part} }

// MovePart says which part of a move file a refusal was about, or nothing
// for any other error.
func MovePart(err error) string {
	var e *moveError
	if errors.As(err, &e) {
		return e.part
	}
	return ""
}

const (
	maxMoveBytes = 1 << 20
	maxDatasets  = 64
	maxHours     = 24 * 31
)

var (
	namePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,31}$`)
	envPattern  = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,63}$`)
	moveID      = regexp.MustCompile(`^[0-9a-f]{32}$`)
	identPat    = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)
)

// standbyPrefix starts every name a reverse sync reads the old side's write
// keys from.
const standbyPrefix = "STANDBY_"

// Move is a move file. It names the data and where it goes, never a
// credential: those come from the environment by the names it gives.
type Move struct {
	Version int `json:"version"`
	// ID is the console's move ID, when the runner reports to a console.
	ID       string    `json:"move,omitempty"`
	Source   string    `json:"source"`
	Target   string    `json:"target"`
	Datasets []Dataset `json:"datasets"`
	Limits   Limits    `json:"limits"`
	// Cutover is the DNS a cutover changes, when the move has one.
	Cutover *Cutover `json:"cutover,omitempty"`

	raw []byte
}

// Limits are what one approval covers, for every dataset together.
type Limits struct {
	MaxObjects int64 `json:"max_objects"`
	MaxBytes   int64 `json:"max_bytes"`
	Hours      int   `json:"hours"`
}

// Dataset is one thing to copy.
type Dataset struct {
	Name string `json:"name"`
	// Kind is "objects" or "postgres".
	Kind string `json:"kind"`

	// objects: bucket specs (internal/transfer/objects.Open) and a prefix.
	From   string `json:"from,omitempty"`
	To     string `json:"to,omitempty"`
	Prefix string `json:"prefix,omitempty"`

	// postgres: the environment names holding each side's connection URL,
	// and the publication and slot the customer made on the source.
	FromEnv     string `json:"from_env,omitempty"`
	ToEnv       string `json:"to_env,omitempty"`
	Publication string `json:"publication,omitempty"`
	Slot        string `json:"slot,omitempty"`
	// CopySchema makes the published tables' schema on the target before
	// the subscription starts, when the target doesn't have them.
	CopySchema bool `json:"copy_schema,omitempty"`

	// BackEnv is the environment name holding the old side's connection
	// URL for a role that may make a subscription there. It is what gives
	// the dataset a way back: with it, zoctl makes a publication and a slot
	// on the target at cutover, and a reverse sync replicates the target's
	// rows to the old side. The name starts with STANDBY_, which no other
	// command reads.
	BackEnv string `json:"back_env,omitempty"`
	// BackFromEnv is the environment name holding the URL the old side
	// reaches the target by, which the old side's subscription keeps. It
	// starts with TARGET_ and goes with BackEnv. A role that may only
	// replicate and read the tables is enough, and is what belongs there.
	BackFromEnv string `json:"back_from_env,omitempty"`
}

// LoadMove reads and checks a move file.
func LoadMove(path string) (Move, error) {
	f, err := os.Open(path)
	if err != nil {
		return Move{}, ErrMove
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, maxMoveBytes+1))
	if err != nil || len(b) > maxMoveBytes {
		return Move{}, ErrMove
	}
	return ParseMove(b)
}

// ParseMove checks a move file's bytes.
func ParseMove(b []byte) (Move, error) {
	var m Move
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if dec.Decode(&m) != nil || dec.More() {
		return Move{}, refused("json: one object, with no field this version doesn't know")
	}
	if err := m.check(); err != nil {
		return Move{}, err
	}
	m.raw = append([]byte{}, b...)
	return m, nil
}

func (m Move) check() error {
	if m.Version != 1 {
		return refused("version: 1 is the only one")
	}
	if m.ID != "" && !moveID.MatchString(m.ID) {
		return refused("id")
	}
	if m.Source == "" || m.Target == "" || m.Source == m.Target || !namePattern.MatchString(m.Source) || !namePattern.MatchString(m.Target) {
		return refused("source, target: two different vendors")
	}
	// A move with no data, apps and a domain only, still has DNS to switch:
	// it names no datasets and a cutover, and its limits, which only a
	// sync spends, can be left out.
	if len(m.Datasets) > maxDatasets || len(m.Datasets) == 0 && m.Cutover == nil {
		return refused("datasets: at least one, or a cutover")
	}
	if (len(m.Datasets) > 0 || m.Limits != Limits{}) && (m.Limits.MaxObjects <= 0 || m.Limits.MaxBytes <= 0 || m.Limits.Hours <= 0 || m.Limits.Hours > maxHours) {
		return refused("limits: max_objects, max_bytes and hours, each above zero")
	}
	if m.Cutover != nil {
		if err := m.Cutover.check(); err != nil {
			return err
		}
	}
	databases, back := 0, 0
	seen := map[string]bool{}
	for _, d := range m.Datasets {
		if !namePattern.MatchString(d.Name) || seen[d.Name] {
			return refused("dataset name: lower case, each once")
		}
		seen[d.Name] = true
		switch d.Kind {
		case "objects":
			if d.From == "" || d.To == "" || d.From == d.To || len(d.Prefix) > 1024 || d.FromEnv != "" || d.ToEnv != "" || d.Publication != "" || d.Slot != "" || d.CopySchema || d.BackEnv != "" || d.BackFromEnv != "" {
				return refused("dataset: objects take from and to, two different places, and no database field")
			}
		case "postgres":
			// The source URL is read from a SOURCE_ name and the target's
			// from a TARGET_ one, so a swapped file can't aim the writes at
			// the source.
			if !envPattern.MatchString(d.FromEnv) || !envPattern.MatchString(d.ToEnv) || d.FromEnv[:min(7, len(d.FromEnv))] != "SOURCE_" || d.ToEnv[:min(7, len(d.ToEnv))] != "TARGET_" {
				return refused("dataset: a database takes from_env, a SOURCE_ name, and to_env, a TARGET_ name")
			}
			if !identPat.MatchString(d.Publication) || !identPat.MatchString(d.Slot) || d.From != "" || d.To != "" || d.Prefix != "" {
				return refused("dataset: a database names its publication and slot, and nothing an objects dataset has")
			}
			databases++
			if d.BackEnv != "" || d.BackFromEnv != "" {
				// The old side's write URL has a prefix of its own, so
				// neither side's ordinary name can stand in for it.
				if !envPattern.MatchString(d.BackEnv) || !strings.HasPrefix(d.BackEnv, standbyPrefix) {
					return refused("dataset: back_env is a STANDBY_ name")
				}
				if !envPattern.MatchString(d.BackFromEnv) || !strings.HasPrefix(d.BackFromEnv, "TARGET_") {
					return refused("dataset: back_from_env is a TARGET_ name")
				}
				back++
			}
		case "mysql", "redis":
			// Read from the environment as a Postgres URL is. Neither has a
			// way back yet, so a move with one has none for its other
			// databases either.
			if !envPattern.MatchString(d.FromEnv) || !envPattern.MatchString(d.ToEnv) || !strings.HasPrefix(d.FromEnv, "SOURCE_") || !strings.HasPrefix(d.ToEnv, "TARGET_") {
				return refused("dataset: a database takes from_env, a SOURCE_ name, and to_env, a TARGET_ name")
			}
			if d.From != "" || d.To != "" || d.Prefix != "" || d.Publication != "" || d.Slot != "" || d.BackEnv != "" || d.BackFromEnv != "" || d.Kind == "redis" && d.CopySchema {
				return refused("dataset: mysql takes from_env, to_env and copy_schema, redis from_env and to_env, and nothing else")
			}
			databases++
		default:
			return refused("dataset kind: objects, postgres, mysql or redis")
		}
	}
	// A way back is for the whole move: a return that carried one database's
	// rows back and left another's behind would be worse than none.
	if back != 0 && back != databases {
		return refused("dataset: a way back on every database or on none")
	}
	return nil
}

// HasData reports whether the move names any data to sync. One that doesn't
// only switches DNS.
func (m Move) HasData() bool { return len(m.Datasets) > 0 }

// digest identifies the move file's exact bytes.
func (m Move) digest() string { return transfer.Digest([]byte("move-v1"), m.raw) }

// Reversible reports whether the move's data has a way back: every
// database names the old side's write URL. Buckets need nothing in the
// file, only STANDBY_ keys in the environment when a reverse sync runs.
func (m Move) Reversible() bool {
	for _, d := range m.Datasets {
		if d.Kind != "objects" && d.BackEnv == "" {
			return false
		}
	}
	return true
}
