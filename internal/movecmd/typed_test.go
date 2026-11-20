package movecmd

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/zoctl/cli/internal/datasync"
)

var digestLine = regexp.MustCompile(`(?m)^[0-9a-f]{64}$`)

// typed is a terminal whose user types back the digest the command printed
// last. Every plan has a digest of its own, so one from an earlier run of
// the command approves nothing.
type typed struct {
	out *bytes.Buffer
	r   io.Reader
}

func typeBack(out *bytes.Buffer) io.Reader { return &typed{out: out} }

func (t *typed) Read(p []byte) (int, error) {
	if t.r == nil {
		line := ""
		if all := digestLine.FindAllString(t.out.String(), -1); len(all) > 0 {
			line = all[len(all)-1]
		}
		t.r = strings.NewReader(line + "\n")
	}
	return t.r.Read(p)
}

// Two buckets on one provider share their credential names, and the second
// dataset has to get the key the first one took out of the environment.
func TestMoveEnvSharedNames(t *testing.T) {
	t.Setenv("SOURCE_AWS_ACCESS_KEY_ID", "synthetic-key")
	t.Setenv("SOURCE_AWS_SECRET_ACCESS_KEY", "synthetic-secret")
	t.Setenv("TARGET_GOOGLE_OAUTH_ACCESS_TOKEN", "synthetic-token")
	m := datasync.Move{Datasets: []datasync.Dataset{
		{Name: "a", Kind: "objects", From: "s3://one?region=us-east-1", To: "gs://one"},
		{Name: "b", Kind: "objects", From: "s3://two?region=us-east-1", To: "gs://two"},
	}}
	env := moveEnv(m, false)
	for name, want := range map[string]string{
		"SOURCE_AWS_ACCESS_KEY_ID": "synthetic-key", "SOURCE_AWS_SECRET_ACCESS_KEY": "synthetic-secret", "TARGET_GOOGLE_OAUTH_ACCESS_TOKEN": "synthetic-token",
	} {
		if env(name) != want {
			t.Errorf("%s was lost to the second dataset", name)
		}
	}
	if _, err := datasync.Open(m, datasync.Options{Env: env}); err != nil {
		t.Fatalf("two datasets on one provider don't open: %v", err)
	}
}

// zoctl sync -check says which it found and exits non-zero when the sides
// differ, so a script can stop on it.
func TestSyncCheckFailsWhenTheSidesDiffer(t *testing.T) {
	fake := &s3Hosts{
		buckets: map[string]map[string][]byte{
			"old.example.test": {"a": []byte("one")},
			"new.example.test": {"a": []byte("one")},
		},
		writes: map[string]int{},
	}
	previous := syncClient
	syncClient = func() *http.Client { return &http.Client{Transport: fake} }
	t.Cleanup(func() { syncClient = previous })
	setEnv := func() {
		for k, v := range map[string]string{
			"SOURCE_S3_ACCESS_KEY_ID": "AKSOURCE", "SOURCE_S3_SECRET_ACCESS_KEY": "source-secret",
			"TARGET_S3_ACCESS_KEY_ID": "AKTARGET", "TARGET_S3_SECRET_ACCESS_KEY": "target-secret",
		} {
			t.Setenv(k, v)
		}
	}
	move := filepath.Join(t.TempDir(), "move.json")
	if err := os.WriteFile(move, []byte(`{"version":1,"source":"aws","target":"hetzner",
 "datasets":[{"name":"media","kind":"objects","from":"s3+https://old.example.test/bucket?region=auto","to":"s3+https://new.example.test/bucket?region=auto"}],
 "limits":{"max_objects":10,"max_bytes":1000,"hours":1}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	setEnv()
	if err := syncData([]string{"-move", move, "-check"}, &out, strings.NewReader("")); err != nil || !strings.Contains(out.String(), "the two sides match") {
		t.Fatalf("%v\n%s", err, out.String())
	}
	fake.mu.Lock()
	fake.buckets["old.example.test"]["a"] = []byte("two")
	fake.mu.Unlock()
	out.Reset()
	setEnv()
	if err := syncData([]string{"-move", move, "-check"}, &out, strings.NewReader("")); !errors.Is(err, errDiffer) || !strings.Contains(out.String(), "the two sides differ") {
		t.Fatalf("%v\n%s", err, out.String())
	}
	if fake.writes["new.example.test"] != 0 || fake.writes["old.example.test"] != 0 {
		t.Fatalf("a check wrote: %v", fake.writes)
	}
}
