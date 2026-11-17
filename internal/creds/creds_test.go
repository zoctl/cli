package creds

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestSaveLoadRemove(t *testing.T) {
	s := Store{Path: filepath.Join(t.TempDir(), "zoctl", "session.json")}
	if _, err := s.Load(); !errors.Is(err, ErrNone) {
		t.Fatalf("Load with nothing saved: %v", err)
	}
	want := Saved{Console: "https://app.zoctl.com", Org: "org", Token: "t", Expires: time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC)}
	if err := s.Save(want); err != nil {
		t.Fatal(err)
	}
	got, err := s.Load()
	if err != nil || got != want {
		t.Fatalf("Load: %+v, %v", got, err)
	}
	if runtime.GOOS != "windows" {
		fi, err := os.Stat(s.Path)
		if err != nil || fi.Mode().Perm() != 0o600 {
			t.Fatalf("file mode %v, %v", fi.Mode().Perm(), err)
		}
		di, err := os.Stat(filepath.Dir(s.Path))
		if err != nil || di.Mode().Perm() != 0o700 {
			t.Fatalf("folder mode %v, %v", di.Mode().Perm(), err)
		}
	}
	entries, _ := os.ReadDir(filepath.Dir(s.Path))
	if len(entries) != 1 {
		t.Fatalf("left behind: %v", entries)
	}
	if err := s.Remove(); err != nil {
		t.Fatal(err)
	}
	if err := s.Remove(); err != nil {
		t.Fatalf("second Remove: %v", err)
	}
	if _, err := s.Load(); !errors.Is(err, ErrNone) {
		t.Fatalf("Load after Remove: %v", err)
	}
}

func TestDefaultHonoursTheEnvironment(t *testing.T) {
	s, err := Default(func(k string) string {
		if k == "ZOCTL_CONFIG_DIR" {
			return "/somewhere"
		}
		return ""
	})
	if err != nil || s.Path != filepath.Join("/somewhere", "session.json") {
		t.Fatalf("Default: %q, %v", s.Path, err)
	}
}

func TestABrokenFileIsNoSession(t *testing.T) {
	s := Store{Path: filepath.Join(t.TempDir(), "session.json")}
	if err := os.WriteFile(s.Path, []byte("{nope"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Load(); !errors.Is(err, ErrNone) {
		t.Fatalf("Load: %v", err)
	}
}
