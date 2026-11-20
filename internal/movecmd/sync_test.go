package movecmd

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
)

// s3Hosts is two S3-compatible services in memory, keyed by host.
type s3Hosts struct {
	mu      sync.Mutex
	buckets map[string]map[string][]byte
	writes  map[string]int
}

func (f *s3Hosts) RoundTrip(r *http.Request) (*http.Response, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	reply := func(code int, body string) (*http.Response, error) {
		return &http.Response{StatusCode: code, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	}
	b := f.buckets[r.URL.Host]
	key := strings.TrimPrefix(r.URL.Path, "/bucket/")
	switch {
	case r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2":
		var keys []string
		for k := range b {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var out strings.Builder
		out.WriteString("<ListBucketResult>")
		for _, k := range keys {
			fmt.Fprintf(&out, "<Contents><Key>%s</Key><Size>%d</Size><ETag>\"%x\"</ETag></Contents>", k, len(b[k]), md5.Sum(b[k]))
		}
		out.WriteString("<IsTruncated>false</IsTruncated></ListBucketResult>")
		return reply(200, out.String())
	case r.Method == http.MethodGet:
		return reply(200, string(b[key]))
	case r.Method == http.MethodPut:
		body, _ := io.ReadAll(r.Body)
		b[key] = body
		f.writes[r.URL.Host]++
		return reply(200, "")
	}
	return reply(400, "")
}

func TestSyncCommand(t *testing.T) {
	fake := &s3Hosts{
		buckets: map[string]map[string][]byte{
			"old.example.test": {"photos/cat.jpg": []byte("meow"), "notes.txt": []byte("secret customer words")},
			"new.example.test": {},
		},
		writes: map[string]int{},
	}
	previous := syncClient
	syncClient = func() *http.Client { return &http.Client{Transport: fake} }
	t.Cleanup(func() { syncClient = previous })
	for k, v := range map[string]string{
		"SOURCE_S3_ACCESS_KEY_ID": "AKSOURCE", "SOURCE_S3_SECRET_ACCESS_KEY": "source-secret",
		"TARGET_S3_ACCESS_KEY_ID": "AKTARGET", "TARGET_S3_SECRET_ACCESS_KEY": "target-secret",
	} {
		t.Setenv(k, v)
	}
	dir := t.TempDir()
	move := filepath.Join(dir, "move.json")
	if err := os.WriteFile(move, []byte(`{"version":1,"source":"aws","target":"hetzner",
 "datasets":[{"name":"media","kind":"objects","from":"s3+https://old.example.test/bucket?region=auto","to":"s3+https://new.example.test/bucket?region=auto"}],
 "limits":{"max_objects":10,"max_bytes":1000,"hours":1}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	ws := filepath.Join(dir, "ws")

	var out bytes.Buffer
	if err := run([]string{"sync", "-move", move, "-workspace", ws}, &out); err == nil {
		t.Fatal("ran without an approval")
	}
	if fake.writes["new.example.test"] != 0 {
		t.Fatal("copied before approval")
	}
	digest := regexp.MustCompile(`(?m)^[0-9a-f]{64}$`).FindString(out.String())
	if digest == "" || !strings.Contains(out.String(), "2 to copy") {
		t.Fatalf("no plan:\n%s", out.String())
	}
	// The keys were taken out of the environment once read.
	if os.Getenv("SOURCE_S3_SECRET_ACCESS_KEY") != "" {
		t.Fatal("the key stayed in the environment")
	}
	for k, v := range map[string]string{
		"SOURCE_S3_ACCESS_KEY_ID": "AKSOURCE", "SOURCE_S3_SECRET_ACCESS_KEY": "source-secret",
		"TARGET_S3_ACCESS_KEY_ID": "AKTARGET", "TARGET_S3_SECRET_ACCESS_KEY": "target-secret",
	} {
		t.Setenv(k, v)
	}
	out.Reset()
	err := syncData([]string{"-move", move, "-workspace", ws, "-every", "1s", "-rounds", "1"}, &out, typeBack(&out))
	if err != nil || !strings.Contains(out.String(), "verified; this approval") {
		t.Fatalf("%v\n%s", err, out.String())
	}
	if fake.writes["new.example.test"] != 2 || fake.writes["old.example.test"] != 0 {
		t.Fatalf("writes %v\n%s", fake.writes, out.String())
	}
	for _, leak := range []string{"photos", "cat.jpg", "notes.txt", "old.example", "new.example", "secret", "AKSOURCE", "meow"} {
		if strings.Contains(out.String(), leak) {
			t.Fatalf("output names %q:\n%s", leak, out.String())
		}
	}
}

// withConsole answers a console's runner endpoints next to the storage fake
// and keeps what was posted to them.
type withConsole struct {
	*s3Hosts
	posts []string
}

func (c *withConsole) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Host != "console.example.test" {
		return c.s3Hosts.RoundTrip(r)
	}
	body, _ := io.ReadAll(r.Body)
	c.mu.Lock()
	c.posts = append(c.posts, r.URL.Path+" "+r.Header.Get("Authorization")+" "+string(body))
	c.mu.Unlock()
	if r.URL.Path == "/v1/runner/pair" {
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"org":"o","runner":"r","token":"` + strings.Repeat("d", 64) + `"}`)), Request: r}, nil
	}
	return &http.Response{StatusCode: 204, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
}

func TestSyncReportsToAPairedConsole(t *testing.T) {
	fake := &withConsole{s3Hosts: &s3Hosts{
		buckets: map[string]map[string][]byte{"old.example.test": {"notes.txt": []byte("secret customer words")}, "new.example.test": {}},
		writes:  map[string]int{},
	}}
	previous := syncClient
	syncClient = func() *http.Client { return &http.Client{Transport: fake} }
	t.Cleanup(func() { syncClient = previous })
	for k, v := range map[string]string{
		"SOURCE_S3_ACCESS_KEY_ID": "AKSOURCE", "SOURCE_S3_SECRET_ACCESS_KEY": "source-secret",
		"TARGET_S3_ACCESS_KEY_ID": "AKTARGET", "TARGET_S3_SECRET_ACCESS_KEY": "target-secret",
	} {
		t.Setenv(k, v)
	}
	dir := t.TempDir()
	move := filepath.Join(dir, "move.json")
	if err := os.WriteFile(move, []byte(`{"version":1,"move":"`+strings.Repeat("a", 32)+`","source":"aws","target":"hetzner",
 "datasets":[{"name":"media","kind":"objects","from":"s3+https://old.example.test/bucket?region=auto","to":"s3+https://new.example.test/bucket?region=auto"}],
 "limits":{"max_objects":10,"max_bytes":1000,"hours":1}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	ws := filepath.Join(dir, "ws")
	var out bytes.Buffer
	code := strings.Repeat("c", 64)
	if err := pairRunner([]string{"-console", "https://console.example.test", "-contact", "https://runner.example.test", "-workspace", ws}, &out, strings.NewReader(code+"\n")); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	if strings.Contains(out.String(), strings.Repeat("d", 64)) {
		t.Fatal("printed the token")
	}
	out.Reset()
	if err := syncData([]string{"-move", move, "-workspace", ws, "-report"}, &out, strings.NewReader("no\n")); err == nil {
		t.Fatal("ran without an approval")
	}
	for k, v := range map[string]string{
		"SOURCE_S3_ACCESS_KEY_ID": "AKSOURCE", "SOURCE_S3_SECRET_ACCESS_KEY": "source-secret",
		"TARGET_S3_ACCESS_KEY_ID": "AKTARGET", "TARGET_S3_SECRET_ACCESS_KEY": "target-secret",
	} {
		t.Setenv(k, v)
	}
	if err := syncData([]string{"-move", move, "-workspace", ws, "-report", "-every", "1s", "-rounds", "1"}, &out, typeBack(&out)); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	all := strings.Join(fake.posts, "\n")
	for _, want := range []string{`"stage":"planned"`, `"stage":"running"`, `"stage":"verified"`, `"ready":true`, "Bearer " + strings.Repeat("d", 64)} {
		if !strings.Contains(all, want) {
			t.Fatalf("no %s in\n%s", want, all)
		}
	}
	for _, leak := range []string{"notes", "old.example", "new.example", "secret", "AKSOURCE", "media", "bucket"} {
		if strings.Contains(all, leak) {
			t.Fatalf("console got %q:\n%s", leak, all)
		}
	}
}
