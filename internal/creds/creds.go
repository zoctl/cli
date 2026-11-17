// Package creds keeps a terminal's console session in a file only its user
// can read. The token in it is never printed.
package creds

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// Saved is one signed-in console.
type Saved struct {
	Console string    `json:"console"`
	Org     string    `json:"org"`
	Token   string    `json:"token"`
	Expires time.Time `json:"expires"`
}

// ErrNone is no saved session.
var ErrNone = errors.New("no saved session")

// Store is the file the session is kept in.
type Store struct{ Path string }

// Default is the store in the user's configuration folder:
// ~/.config/zoctl on Linux, ~/Library/Application Support/zoctl on macOS,
// %AppData%\zoctl on Windows. ZOCTL_CONFIG_DIR moves it.
func Default(getenv func(string) string) (Store, error) {
	dir := getenv("ZOCTL_CONFIG_DIR")
	if dir == "" {
		base, err := os.UserConfigDir()
		if err != nil {
			return Store{}, err
		}
		dir = filepath.Join(base, "zoctl")
	}
	return Store{Path: filepath.Join(dir, "session.json")}, nil
}

// Load reads the saved session.
func (s Store) Load() (Saved, error) {
	b, err := os.ReadFile(s.Path)
	if errors.Is(err, fs.ErrNotExist) {
		return Saved{}, ErrNone
	}
	if err != nil {
		return Saved{}, err
	}
	var out Saved
	if err := json.Unmarshal(b, &out); err != nil || out.Token == "" {
		return Saved{}, ErrNone
	}
	return out, nil
}

// Save writes the session where only this user can read it, replacing the
// last one whole: a half-written file is never left behind.
func (s Store) Save(v Saved) error {
	dir := filepath.Dir(s.Path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".session-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err := f.Chmod(0o600); err != nil {
		f.Close()
		return err
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), s.Path)
}

// Remove forgets the saved session. None saved is fine.
func (s Store) Remove() error {
	err := os.Remove(s.Path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}
