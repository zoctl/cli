package movecmd

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zoctl/cli/internal/datasync"
)

// signers keeps which key signed each write, by host.
type signers struct {
	http.RoundTripper
	mu     sync.Mutex
	writes map[string][]string
}

func (s *signers) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead && strings.HasSuffix(r.URL.Host, ".example.test") {
		key := r.Header.Get("Authorization")
		if i := strings.Index(key, "Credential="); i >= 0 {
			key = key[i+len("Credential="):]
			key = key[:strings.IndexAny(key+"/", "/")]
		}
		s.mu.Lock()
		s.writes[r.URL.Host] = append(s.writes[r.URL.Host], key)
		s.mu.Unlock()
	}
	return s.RoundTripper.RoundTrip(r)
}

// A move there and back through the commands, with a bucket: nothing is
// written to the old side until the move has cut over, and then only by
// zoctl sync -back on the STANDBY_ keys.
func TestStandbyCommands(t *testing.T) {
	zone := &cfZone{
		withConsole: &withConsole{s3Hosts: &s3Hosts{
			buckets: map[string]map[string][]byte{"old.example.test": {"notes.txt": []byte("words"), "kept.txt": []byte("only here")}, "new.example.test": {}},
			writes:  map[string]int{},
		}},
		records: map[string]map[string]any{"reca": {"id": "reca", "name": "www.shop.example.test", "type": "A", "content": "198.51.100.7", "ttl": 300, "proxied": false}},
	}
	signed := &signers{RoundTripper: zone, writes: map[string][]string{}}
	previous, previousSleep := syncClient, cutoverSleep
	syncClient = func() *http.Client { return &http.Client{Transport: signed} }
	cutoverSleep = func(context.Context, time.Duration) error { return nil }
	t.Cleanup(func() { syncClient, cutoverSleep = previous, previousSleep })
	setEnv := func(standby bool) {
		for k, v := range map[string]string{
			"SOURCE_S3_ACCESS_KEY_ID": "AKSOURCE", "SOURCE_S3_SECRET_ACCESS_KEY": "source-secret",
			"TARGET_S3_ACCESS_KEY_ID": "AKTARGET", "TARGET_S3_SECRET_ACCESS_KEY": "target-secret",
			"DNS_CLOUDFLARE_API_TOKEN": "cf-secret-token",
		} {
			t.Setenv(k, v)
		}
		t.Setenv("STANDBY_S3_ACCESS_KEY_ID", "")
		t.Setenv("STANDBY_S3_SECRET_ACCESS_KEY", "")
		if standby {
			t.Setenv("STANDBY_S3_ACCESS_KEY_ID", "AKSTANDBY")
			t.Setenv("STANDBY_S3_SECRET_ACCESS_KEY", "standby-secret")
		}
	}
	dir := t.TempDir()
	move := filepath.Join(dir, "move.json")
	if err := os.WriteFile(move, []byte(`{"version":1,"source":"aws","target":"hetzner",
 "datasets":[{"name":"media","kind":"objects","from":"s3+https://old.example.test/bucket?region=auto","to":"s3+https://new.example.test/bucket?region=auto"}],
 "limits":{"max_objects":10,"max_bytes":1000,"hours":1},
 "cutover":{"zone":"cloudflare://0123456789abcdef0123456789abcdef","credentials":"DNS",
  "records":[{"name":"www.shop.example.test.","type":"A","ttl":60,"values":["203.0.113.9"]}]}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	ws := filepath.Join(dir, "ws")
	old, target := zone.buckets["old.example.test"], zone.buckets["new.example.test"]
	oldWrites := func() int { signed.mu.Lock(); defer signed.mu.Unlock(); return len(signed.writes["old.example.test"]) }
	var out bytes.Buffer
	all := ""
	step := func(want bool, what string, err error) {
		t.Helper()
		all += out.String()
		if (err == nil) != want {
			t.Fatalf("%s: %v\n%s", what, err, out.String())
		}
		out.Reset()
	}

	// Before a cutover the source is only read, whatever keys are given.
	setEnv(true)
	err := syncData([]string{"-move", move, "-workspace", ws, "-back"}, &out, typeBack(&out))
	if !errors.Is(err, datasync.ErrNotCutOver) || !strings.Contains(out.String(), "hasn't cut over") {
		t.Fatalf("a reverse sync before the cutover: %v\n%s", err, out.String())
	}
	if os.Getenv("STANDBY_S3_SECRET_ACCESS_KEY") == "" {
		t.Fatal("the old side's write key was read for a reverse sync that can't run")
	}
	step(false, "a reverse sync before the cutover", err)
	// The console's status has no word for a reverse sync.
	setEnv(true)
	step(false, "sync -back -report", syncData([]string{"-move", move, "-workspace", ws, "-back", "-report"}, &out, typeBack(&out)))

	setEnv(true)
	step(true, "sync", syncData([]string{"-move", move, "-workspace", ws, "-every", "1s", "-rounds", "1"}, &out, typeBack(&out)))
	if os.Getenv("STANDBY_S3_SECRET_ACCESS_KEY") == "" {
		t.Fatal("the move's sync read the old side's write key")
	}
	setEnv(true)
	step(true, "cutover", cutover([]string{"-move", move, "-workspace", ws}, &out, typeBack(&out)))
	if os.Getenv("STANDBY_S3_SECRET_ACCESS_KEY") == "" {
		t.Fatal("a cutover read the old side's write key")
	}
	if oldWrites() != 0 {
		t.Fatalf("the source was written before the move cut over: %v", signed.writes)
	}

	// The target takes writes, so the old side is behind and a return
	// would lose them.
	target["new.txt"] = []byte("written on the target")
	target["notes.txt"] = []byte("words, edited")
	delete(target, "kept.txt")
	setEnv(false)
	err = cutover([]string{"-move", move, "-workspace", ws, "-back"}, &out, typeBack(&out))
	if err == nil || !strings.Contains(out.String(), "standby_not_caught_up") {
		t.Fatalf("back with the old side behind: %v\n%s", err, out.String())
	}
	step(false, "back with the old side behind", err)
	if zone.records["reca"] != nil {
		t.Fatal("DNS went back")
	}

	// The source's read-only keys don't open the old side for writing.
	setEnv(false)
	step(false, "a reverse sync without STANDBY_ keys", syncData([]string{"-move", move, "-workspace", ws, "-back"}, &out, typeBack(&out)))
	setEnv(true)
	err = syncData([]string{"-move", move, "-workspace", ws, "-back"}, &out, strings.NewReader("no\n"))
	if err == nil || !strings.Contains(out.String(), "2 to copy back to the old side") || !strings.Contains(out.String(), "1 only on the old side and left alone") {
		t.Fatalf("a reverse plan: %v\n%s", err, out.String())
	}
	step(false, "a reverse sync that wasn't approved", err)
	if oldWrites() != 0 {
		t.Fatalf("the old side was written without an approval: %v", signed.writes)
	}

	setEnv(true)
	step(true, "sync -back", syncData([]string{"-move", move, "-workspace", ws, "-back", "-every", "1s", "-rounds", "1"}, &out, typeBack(&out)))
	if os.Getenv("STANDBY_S3_SECRET_ACCESS_KEY") != "" {
		t.Fatal("the old side's write key stayed in the environment")
	}
	if string(old["new.txt"]) != "written on the target" || string(old["notes.txt"]) != "words, edited" {
		t.Fatalf("the old side after the reverse sync: %q", old)
	}
	if string(old["kept.txt"]) != "only here" {
		t.Fatal("the reverse sync deleted an object from the old side")
	}
	if oldWrites() != 2 {
		t.Fatalf("writes to the old side: %v", signed.writes)
	}
	for _, key := range signed.writes["old.example.test"] {
		if key != "AKSTANDBY" {
			t.Fatalf("the old side was written with %s", key)
		}
	}
	setEnv(true)
	err = syncData([]string{"-move", move, "-workspace", ws, "-back", "-check"}, &out, strings.NewReader(""))
	if err != nil || !strings.Contains(out.String(), "the old side has everything the target has") {
		t.Fatalf("sync -back -check: %v\n%s", err, out.String())
	}
	step(true, "sync -back -check", err)

	// Now the return goes through, on the source's read-only keys.
	setEnv(false)
	step(true, "cutover -back", cutover([]string{"-move", move, "-workspace", ws, "-back"}, &out, typeBack(&out)))
	for _, rec := range zone.records {
		if rec["content"] != "198.51.100.7" {
			t.Fatalf("not back: %v", rec)
		}
	}
	// The source takes writes again, and nothing writes to it.
	setEnv(true)
	err = syncData([]string{"-move", move, "-workspace", ws, "-back"}, &out, typeBack(&out))
	if !errors.Is(err, datasync.ErrNotCutOver) {
		t.Fatalf("a reverse sync after the return: %v\n%s", err, out.String())
	}
	step(false, "a reverse sync after the return", err)
	if oldWrites() != 2 {
		t.Fatalf("the source was written after the return: %v", signed.writes)
	}
	for _, leak := range []string{"notes.txt", "new.txt", "old.example", "new.example", "secret", "AKSOURCE", "AKSTANDBY", "AKTARGET", "words"} {
		if strings.Contains(all, leak) {
			t.Fatalf("output names %q:\n%s", leak, all)
		}
	}
}
