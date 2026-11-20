package movecmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/zoctl/cli/internal/datasync"
)

// cfZone is a Cloudflare zone with one-value records, next to the storage
// and console fakes.
type cfZone struct {
	*withConsole
	records map[string]map[string]any // id -> record
	next    int
}

func (z *cfZone) RoundTrip(r *http.Request) (*http.Response, error) {
	switch r.URL.Host {
	case "api.cloudflare.com":
	case "new.shop.example.test":
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("ok")), Request: r}, nil
	default:
		return z.withConsole.RoundTrip(r)
	}
	z.mu.Lock()
	defer z.mu.Unlock()
	reply := func(v any) (*http.Response, error) {
		b, _ := json.Marshal(v)
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(b)), Request: r}, nil
	}
	if r.Method == http.MethodGet {
		var out []map[string]any
		for _, rec := range z.records {
			if rec["name"] == r.URL.Query().Get("name") && rec["type"] == r.URL.Query().Get("type") {
				out = append(out, rec)
			}
		}
		return reply(map[string]any{"success": true, "result": out})
	}
	var batch struct {
		Deletes []map[string]string `json:"deletes"`
		Posts   []map[string]any    `json:"posts"`
	}
	if json.NewDecoder(r.Body).Decode(&batch) != nil {
		return nil, io.ErrUnexpectedEOF
	}
	for _, d := range batch.Deletes {
		delete(z.records, d["id"])
	}
	for _, p := range batch.Posts {
		z.next++
		p["id"] = "rec" + string(rune('a'+z.next))
		z.records[p["id"].(string)] = p
	}
	return reply(map[string]any{"success": true})
}

// A move with apps and a domain and no data, such as a Vercel project's,
// still switches its DNS and back; a sync of it is refused and says so.
func TestCutoverCommandWithNoData(t *testing.T) {
	zone := &cfZone{
		withConsole: &withConsole{s3Hosts: &s3Hosts{buckets: map[string]map[string][]byte{}, writes: map[string]int{}}},
		records:     map[string]map[string]any{"reca": {"id": "reca", "name": "app.shop.example.test", "type": "CNAME", "content": "cname.vercel-dns.example.test", "ttl": 300, "proxied": false}},
	}
	previous, previousSleep := syncClient, cutoverSleep
	syncClient = func() *http.Client { return &http.Client{Transport: zone} }
	slept := false
	cutoverSleep = func(context.Context, time.Duration) error { slept = true; return nil }
	t.Cleanup(func() { syncClient, cutoverSleep = previous, previousSleep })
	dir := t.TempDir()
	move := filepath.Join(dir, "move.json")
	if err := os.WriteFile(move, []byte(`{"version":1,"source":"vercel","target":"azure","datasets":[],
 "cutover":{"zone":"cloudflare://0123456789abcdef0123456789abcdef","credentials":"DNS","health":"https://new.shop.example.test/healthz",
  "records":[{"name":"app.shop.example.test.","type":"CNAME","ttl":60,"values":["app-web.azurewebsites.example.test."]}]}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	ws := filepath.Join(dir, "ws")
	var out bytes.Buffer

	if err := syncData([]string{"-move", move, "-workspace", ws}, &out, strings.NewReader("\n")); !errors.Is(err, datasync.ErrNoData) || !strings.Contains(out.String(), "no data to sync") {
		t.Fatalf("sync of a move with no data: %v\n%s", err, out.String())
	}
	t.Setenv("DNS_CLOUDFLARE_API_TOKEN", "cf-secret-token")
	out.Reset()
	if err := cutover([]string{"-move", move, "-workspace", ws}, &out, typeBack(&out)); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "only DNS changes") || slept {
		t.Fatalf("slept %v\n%s", slept, out.String())
	}
	if len(zone.records) != 1 || zone.records["reca"] != nil {
		t.Fatalf("records %v", zone.records)
	}
	for _, rec := range zone.records {
		if rec["content"] != "app-web.azurewebsites.example.test" {
			t.Fatalf("%v", rec)
		}
	}
	if err := syncData([]string{"-move", move, "-workspace", ws, "-back"}, &out, strings.NewReader("\n")); !errors.Is(err, datasync.ErrNoData) {
		t.Fatalf("sync -back of a move with no data: %v", err)
	}
	t.Setenv("DNS_CLOUDFLARE_API_TOKEN", "cf-secret-token")
	out.Reset()
	if err := cutover([]string{"-move", move, "-workspace", ws, "-back"}, &out, typeBack(&out)); err != nil {
		t.Fatalf("back: %v\n%s", err, out.String())
	}
	for _, rec := range zone.records {
		if rec["content"] != "cname.vercel-dns.example.test" || rec["ttl"].(float64) != 300 {
			t.Fatalf("not back: %v", rec)
		}
	}
}

func TestCutoverCommand(t *testing.T) {
	zone := &cfZone{
		withConsole: &withConsole{s3Hosts: &s3Hosts{
			buckets: map[string]map[string][]byte{"old.example.test": {"notes.txt": []byte("words")}, "new.example.test": {"notes.txt": []byte("words")}},
			writes:  map[string]int{},
		}},
		records: map[string]map[string]any{"reca": {"id": "reca", "name": "www.shop.example.test", "type": "A", "content": "198.51.100.7", "ttl": 300, "proxied": false}},
	}
	previous, previousSleep := syncClient, cutoverSleep
	syncClient = func() *http.Client { return &http.Client{Transport: zone} }
	cutoverSleep = func(context.Context, time.Duration) error { return nil }
	t.Cleanup(func() { syncClient, cutoverSleep = previous, previousSleep })
	setEnv := func() {
		for k, v := range map[string]string{
			"SOURCE_S3_ACCESS_KEY_ID": "AKSOURCE", "SOURCE_S3_SECRET_ACCESS_KEY": "source-secret",
			"TARGET_S3_ACCESS_KEY_ID": "AKTARGET", "TARGET_S3_SECRET_ACCESS_KEY": "target-secret",
			"DNS_CLOUDFLARE_API_TOKEN": "cf-secret-token",
		} {
			t.Setenv(k, v)
		}
	}
	dir := t.TempDir()
	move := filepath.Join(dir, "move.json")
	if err := os.WriteFile(move, []byte(`{"version":1,"move":"`+strings.Repeat("a", 32)+`","source":"aws","target":"hetzner",
 "datasets":[{"name":"media","kind":"objects","from":"s3+https://old.example.test/bucket?region=auto","to":"s3+https://new.example.test/bucket?region=auto"}],
 "limits":{"max_objects":10,"max_bytes":1000,"hours":1},
 "cutover":{"zone":"cloudflare://0123456789abcdef0123456789abcdef","credentials":"DNS","health":"https://new.shop.example.test/healthz",
  "records":[{"name":"www.shop.example.test.","type":"A","ttl":60,"values":["203.0.113.9"]}]}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	ws := filepath.Join(dir, "ws")
	var out bytes.Buffer
	if err := pairRunner([]string{"-console", "https://console.example.test", "-contact", "https://runner.example.test", "-workspace", ws}, &out, strings.NewReader(strings.Repeat("c", 64)+"\n")); err != nil {
		t.Fatal(err)
	}
	digestOf := func(s string) string { return regexp.MustCompile(`(?m)^[0-9a-f]{64}$`).FindString(s) }

	setEnv()
	out.Reset()
	if err := cutover([]string{"-move", move, "-workspace", ws, "-report"}, &out, strings.NewReader("no\n")); err == nil {
		t.Fatal("cut over without an approval")
	}
	if zone.records["reca"] == nil || !strings.Contains(out.String(), "1 DNS record sets change") {
		t.Fatalf("changed before approval:\n%s", out.String())
	}
	setEnv()
	all := out.String()
	out.Reset()
	if err := cutover([]string{"-move", move, "-workspace", ws, "-report"}, &out, typeBack(&out)); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	if len(zone.records) != 1 || zone.records["reca"] != nil {
		t.Fatalf("records %v", zone.records)
	}
	for _, rec := range zone.records {
		if rec["content"] != "203.0.113.9" {
			t.Fatalf("%v", rec)
		}
	}
	all += out.String()

	// The console counts the move's 30 days from this report, so one it
	// missed is sent again from the workspace: same time, nothing changed.
	switched := regexp.MustCompile(`"switched":"[^"]+"`)
	first := switched.FindAllString(strings.Join(zone.posts, "\n"), -1)
	if len(first) != 1 {
		t.Fatalf("%d reports of the switch in\n%s", len(first), strings.Join(zone.posts, "\n"))
	}
	setEnv()
	out.Reset()
	if err := cutover([]string{"-move", move, "-workspace", ws, "-report"}, &out, strings.NewReader("\n")); !errors.Is(err, datasync.ErrCutOver) || !strings.Contains(out.String(), "already done") {
		t.Fatalf("a second cutover: %v\n%s", err, out.String())
	}
	if again := switched.FindAllString(strings.Join(zone.posts, "\n"), -1); len(again) != 2 || again[1] != first[0] || zone.records["reca"] != nil {
		t.Fatalf("the switch sent again as %v", again)
	}
	all += out.String()

	// A sync now would copy the source's old data over the target's.
	setEnv()
	out.Reset()
	if err := syncData([]string{"-move", move, "-workspace", ws}, &out, strings.NewReader("\n")); err == nil || !strings.Contains(out.String(), "cut over") {
		t.Fatalf("sync after cutover: %v\n%s", err, out.String())
	}

	setEnv()
	out.Reset()
	if err := cutover([]string{"-move", move, "-workspace", ws, "-back"}, &out, strings.NewReader("no\n")); err == nil {
		t.Fatal("switched back without an approval")
	}
	// The digest of that plan approves no other plan, not even the same one
	// made again.
	setEnv()
	stale := digestOf(out.String())
	all += out.String()
	out.Reset()
	if err := cutover([]string{"-move", move, "-workspace", ws, "-back"}, &out, strings.NewReader(stale+"\n")); err == nil || !strings.Contains(out.String(), "not approved") {
		t.Fatalf("an earlier plan's digest switched back: %v\n%s", err, out.String())
	}
	setEnv()
	all += out.String()
	out.Reset()
	if err := cutover([]string{"-move", move, "-workspace", ws, "-back"}, &out, typeBack(&out)); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	all += out.String()
	for _, rec := range zone.records {
		if rec["content"] != "198.51.100.7" || rec["ttl"].(float64) != 300 {
			t.Fatalf("not back: %v", rec)
		}
	}

	// That return ran without -report. Asked again with it, the console
	// is told the move went back, which stops its 30 days.
	setEnv()
	out.Reset()
	if err := cutover([]string{"-move", move, "-workspace", ws, "-back", "-report"}, &out, strings.NewReader("\n")); !errors.Is(err, datasync.ErrNoCutover) || !strings.Contains(out.String(), "already done") {
		t.Fatalf("a second return: %v\n%s", err, out.String())
	}
	all += out.String()

	posts := strings.Join(zone.posts, "\n")
	for _, want := range []string{`"stage":"verified"`, `"ready":true`, `"return_until":"`, `"stage":"paused"`, `"returned":"`} {
		if !strings.Contains(posts, want) {
			t.Fatalf("no %s in\n%s", want, posts)
		}
	}
	for _, leak := range []string{"shop.example", "203.0.113", "198.51.100", "cf-secret", "0123456789abcdef"} {
		if strings.Contains(all, leak) || strings.Contains(posts, leak) {
			t.Fatalf("%q in output or posts:\n%s\n%s", leak, all, posts)
		}
	}
}

// A move file zoctl refuses used to end in "command failed" and nothing else.
// Both commands that read one say which part, without quoting the file.
func TestSyncAndCutoverSayWhichPartOfAMoveFileWasRefused(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "move.json")
	body := `{"version": 1, "source": "aws", "target": "gcp",
 "datasets": [{"name": "uploads", "kind": "objects", "from": "s3://old-secret-bucket?region=us-east-1", "to": "gs://new"}],
 "limits": {"max_objects": 10, "max_bytes": 1000, "hours": 1},
 "cutover": {"zone": "route53://Z0123456789ABC", "credentials": "DNS",
  "records": [{"name": "app.shop.example.test", "type": "A", "ttl": 60, "values": ["198.51.100.20"]}]}}`
	if err := os.WriteFile(file, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, args := range map[string][]string{
		"sync":    {"sync", "-move", file, "-check"},
		"cutover": {"cutover", "-move", file, "-workspace", filepath.Join(dir, "w")},
	} {
		var out bytes.Buffer
		err := run(args, &out)
		if !errors.Is(err, datasync.ErrMove) {
			t.Fatalf("%s: %v, want the move file refused", name, err)
		}
		if !strings.Contains(out.String(), "move file was refused") || !strings.Contains(out.String(), "end with a dot") {
			t.Fatalf("%s didn't say why:\n%s", name, out.String())
		}
		if strings.Contains(out.String(), "shop.example") || strings.Contains(out.String(), "old-secret-bucket") {
			t.Fatalf("%s quoted the move file:\n%s", name, out.String())
		}
	}
	// A file that isn't there is refused too, with no part to name.
	var out bytes.Buffer
	if err := run([]string{"sync", "-move", filepath.Join(dir, "gone.json"), "-check"}, &out); !errors.Is(err, datasync.ErrMove) || !strings.Contains(out.String(), "move file was refused") {
		t.Fatalf("a missing move file: %v\n%s", err, out.String())
	}
}

// A finding's code can't say what to do about a cutover that stopped part
// way: a reverse sync is refused under one, and a name left with neither its
// old record nor its new one needs a person.
func TestBlockedCutoverSaysTheWayOutOfAHalfCutover(t *testing.T) {
	say := func(partWay bool, p datasync.CutPlan) string {
		var out bytes.Buffer
		sayBlocked(&out, partWay, p)
		return out.String()
	}
	behind := datasync.CutPlan{Back: true, Blocked: true, Findings: []string{"standby_not_caught_up"}}
	if got := say(true, behind); !strings.Contains(got, "zoctl cutover to finish") || !strings.Contains(got, "sync -back") {
		t.Fatalf("a return blocked under a half cutover:\n%s", got)
	}
	if got := say(false, behind); strings.Contains(got, "finish") || !strings.Contains(got, "sync -back") {
		t.Fatalf("a return blocked under a finished cutover:\n%s", got)
	}
	changed := datasync.CutPlan{Resume: true, Blocked: true, Findings: []string{"dns_changed"}}
	if got := say(true, changed); !strings.Contains(got, "by hand") {
		t.Fatalf("a name that is neither old nor new:\n%s", got)
	}
	if got := say(false, datasync.CutPlan{Blocked: true, Findings: []string{"target_unhealthy"}}); got != "" {
		t.Fatalf("a finding with nothing to add:\n%s", got)
	}
}
