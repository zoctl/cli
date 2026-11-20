package dns

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"
)

// zoneFake is one zone in memory behind four DNS hosts' APIs. Each refuses
// a stale change the way the real host does.
type zoneFake struct {
	mu     sync.Mutex
	sets   map[string]RecordSet // "name type"
	cfIDs  map[string][]string  // "name type" -> record IDs
	nextID int
	writes int
	auth   []string
	// refuse fails a call the way a host that is down does.
	refuse func(r *http.Request) bool
}

// clash reports a name that has a CNAME and something else, which no DNS
// host stores.
func (z *zoneFake) clash() bool {
	for k, s := range z.sets {
		if s.Type != "CNAME" {
			continue
		}
		for _, t := range []string{"A", "AAAA"} {
			if _, ok := z.sets[strings.TrimSuffix(k, "CNAME")+t]; ok {
				return true
			}
		}
	}
	return false
}

func newZone(sets ...RecordSet) *zoneFake {
	z := &zoneFake{sets: map[string]RecordSet{}, cfIDs: map[string][]string{}}
	for _, s := range sets {
		z.put(s)
	}
	return z
}

func (z *zoneFake) put(s RecordSet) {
	k := s.Name + " " + s.Type
	z.sets[k] = s
	z.cfIDs[k] = nil
	for range s.Values {
		z.nextID++
		z.cfIDs[k] = append(z.cfIDs[k], fmt.Sprintf("id%d", z.nextID))
	}
}

func (z *zoneFake) del(name, typ string) {
	delete(z.sets, name+" "+typ)
	delete(z.cfIDs, name+" "+typ)
}

func respond(code int, body string) (*http.Response, error) {
	return &http.Response{StatusCode: code, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}, nil
}

func (z *zoneFake) RoundTrip(r *http.Request) (*http.Response, error) {
	z.mu.Lock()
	defer z.mu.Unlock()
	z.auth = append(z.auth, r.Header.Get("Authorization"))
	var body []byte
	if r.Body != nil {
		body, _ = io.ReadAll(r.Body)
	}
	if z.refuse != nil && z.refuse(r) {
		return respond(500, "")
	}
	// A write that would leave a CNAME beside another type is refused
	// whole, as each host does.
	sets, ids, writes := map[string]RecordSet{}, map[string][]string{}, z.writes
	for k, v := range z.sets {
		sets[k], ids[k] = v, z.cfIDs[k]
	}
	resp, err := z.serve(r, body)
	if z.clash() {
		z.sets, z.cfIDs, z.writes = sets, ids, writes
		return respond(400, "")
	}
	return resp, err
}

func (z *zoneFake) serve(r *http.Request, body []byte) (*http.Response, error) {
	switch {
	case strings.HasPrefix(r.URL.Path, "/2013-04-01/"):
		return z.route53(r, body)
	case strings.HasPrefix(r.URL.Path, "/dns/v1/"):
		return z.cloudDNS(r, body)
	case strings.HasPrefix(r.URL.Path, "/subscriptions/"):
		return z.azure(r, body)
	case strings.HasPrefix(r.URL.Path, "/client/v4/"):
		return z.cloudflare(r, body)
	}
	return respond(400, "")
}

func (z *zoneFake) route53(r *http.Request, body []byte) (*http.Response, error) {
	if r.Method == http.MethodGet {
		q := r.URL.Query()
		// The list starts at the name: answer the next set when there's
		// none of that type, as Route 53 does.
		var keys []string
		for k := range z.sets {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var out strings.Builder
		out.WriteString(`<ListResourceRecordSetsResponse><ResourceRecordSets>`)
		for _, k := range keys {
			if k >= q.Get("name")+" "+q.Get("type") {
				s := z.sets[k]
				fmt.Fprintf(&out, "<ResourceRecordSet><Name>%s</Name><Type>%s</Type><TTL>%d</TTL><ResourceRecords>", s.Name, s.Type, s.TTL)
				for _, v := range s.Values {
					fmt.Fprintf(&out, "<ResourceRecord><Value>%s</Value></ResourceRecord>", v)
				}
				out.WriteString("</ResourceRecords></ResourceRecordSet>")
				break
			}
		}
		out.WriteString(`</ResourceRecordSets></ListResourceRecordSetsResponse>`)
		return respond(200, out.String())
	}
	var req struct {
		Changes []struct {
			Action string `xml:"Action"`
			Set    r53Set `xml:"ResourceRecordSet"`
		} `xml:"ChangeBatch>Changes>Change"`
	}
	if xml.Unmarshal(body, &req) != nil || !strings.HasSuffix(r.URL.Path, "/rrset/") {
		return respond(400, "")
	}
	conv := func(s r53Set) RecordSet {
		rs := RecordSet{Name: s.Name, Type: s.Type, TTL: s.TTL}
		for _, v := range s.Records {
			rs.Values = append(rs.Values, v.Value)
		}
		return rs
	}
	for _, c := range req.Changes {
		cur, ok := z.sets[c.Set.Name+" "+c.Set.Type]
		if c.Action == "DELETE" && (!ok || !cur.Equal(conv(c.Set))) || c.Action == "CREATE" && ok && !hasDelete(req.Changes, c.Set) {
			return respond(400, "<ErrorResponse><Error><Code>InvalidChangeBatch</Code></Error></ErrorResponse>")
		}
	}
	for _, c := range req.Changes {
		if c.Action == "DELETE" {
			z.del(c.Set.Name, c.Set.Type)
		} else {
			z.put(conv(c.Set))
		}
	}
	z.writes++
	return respond(200, "<ChangeResourceRecordSetsResponse/>")
}

func hasDelete(changes []struct {
	Action string `xml:"Action"`
	Set    r53Set `xml:"ResourceRecordSet"`
}, s r53Set) bool {
	for _, c := range changes {
		if c.Action == "DELETE" && c.Set.Name == s.Name && c.Set.Type == s.Type {
			return true
		}
	}
	return false
}

func (z *zoneFake) cloudDNS(r *http.Request, body []byte) (*http.Response, error) {
	if r.Method == http.MethodGet {
		var out struct {
			RRSets []gcpSet `json:"rrsets"`
		}
		if s, ok := z.sets[r.URL.Query().Get("name")+" "+r.URL.Query().Get("type")]; ok {
			out.RRSets = []gcpSet{{s.Name, s.Type, s.TTL, s.Values}}
		}
		b, _ := json.Marshal(out)
		return respond(200, string(b))
	}
	var req struct {
		Additions []gcpSet `json:"additions"`
		Deletions []gcpSet `json:"deletions"`
	}
	if json.Unmarshal(body, &req) != nil {
		return respond(400, "")
	}
	for _, d := range req.Deletions {
		cur, ok := z.sets[d.Name+" "+d.Type]
		if !ok || !cur.Equal(RecordSet{Name: d.Name, Type: d.Type, TTL: d.TTL, Values: d.RRDatas}) {
			return respond(412, `{"error":{"code":412}}`)
		}
	}
	for _, a := range req.Additions {
		if _, ok := z.sets[a.Name+" "+a.Type]; ok && len(req.Deletions) == 0 {
			return respond(409, `{"error":{"code":409}}`)
		}
	}
	for _, d := range req.Deletions {
		z.del(d.Name, d.Type)
	}
	for _, a := range req.Additions {
		z.put(RecordSet{Name: a.Name, Type: a.Type, TTL: a.TTL, Values: a.RRDatas})
	}
	z.writes++
	return respond(200, `{"status":"pending"}`)
}

func (z *zoneFake) etag(k string) string {
	s, ok := z.sets[k]
	if !ok {
		return ""
	}
	return fmt.Sprintf("%x", s.TTL+len(strings.Join(s.Values, ",")))
}

func (z *zoneFake) azure(r *http.Request, body []byte) (*http.Response, error) {
	parts := strings.Split(r.URL.Path, "/")
	zone, typ, rel := parts[len(parts)-3], parts[len(parts)-2], parts[len(parts)-1]
	name := zone + "."
	if rel != "@" {
		name = rel + "." + zone + "."
	}
	k := name + " " + typ
	s, ok := z.sets[k]
	switch r.Method {
	case http.MethodGet:
		if !ok {
			return respond(404, "")
		}
		props := map[string]any{"TTL": s.TTL}
		switch typ {
		case "A":
			var recs []map[string]string
			for _, v := range s.Values {
				recs = append(recs, map[string]string{"ipv4Address": v})
			}
			props["ARecords"] = recs
		case "CNAME":
			props["CNAMERecord"] = map[string]string{"cname": strings.TrimSuffix(s.Values[0], ".")}
		}
		b, _ := json.Marshal(map[string]any{"etag": z.etag(k), "properties": props})
		return respond(200, string(b))
	case http.MethodPut, http.MethodDelete:
		if m := r.Header.Get("If-Match"); m != "" && m != z.etag(k) || r.Header.Get("If-None-Match") == "*" && ok {
			return respond(412, "")
		}
		if r.Method == http.MethodDelete {
			z.del(name, typ)
			z.writes++
			return respond(200, "")
		}
		var in struct {
			Properties struct {
				TTL   int                 `json:"TTL"`
				A     []map[string]string `json:"ARecords"`
				CNAME map[string]string   `json:"CNAMERecord"`
			} `json:"properties"`
		}
		if json.Unmarshal(body, &in) != nil {
			return respond(400, "")
		}
		rs := RecordSet{Name: name, Type: typ, TTL: in.Properties.TTL}
		for _, a := range in.Properties.A {
			rs.Values = append(rs.Values, a["ipv4Address"])
		}
		if c := in.Properties.CNAME["cname"]; c != "" {
			rs.Values = []string{c}
		}
		z.put(rs)
		z.writes++
		return respond(200, "{}")
	}
	return respond(400, "")
}

func (z *zoneFake) cloudflare(r *http.Request, body []byte) (*http.Response, error) {
	if r.Method == http.MethodGet {
		q := r.URL.Query()
		k := q.Get("name") + ". " + q.Get("type")
		var out []cfRecord
		if s, ok := z.sets[k]; ok {
			for i, v := range s.Values {
				out = append(out, cfRecord{ID: z.cfIDs[k][i], Name: q.Get("name"), Type: s.Type, Content: strings.TrimSuffix(v, "."), TTL: s.TTL, Proxied: s.Proxied})
			}
		}
		b, _ := json.Marshal(map[string]any{"success": true, "result": out})
		return respond(200, string(b))
	}
	var batch struct {
		Deletes []map[string]string `json:"deletes"`
		Posts   []cfRecord          `json:"posts"`
	}
	if json.Unmarshal(body, &batch) != nil || !strings.HasSuffix(r.URL.Path, "/dns_records/batch") {
		return respond(400, "")
	}
	byID := map[string]string{}
	for k, ids := range z.cfIDs {
		for _, id := range ids {
			byID[id] = k
		}
	}
	for _, d := range batch.Deletes {
		if _, ok := byID[d["id"]]; !ok {
			return respond(404, `{"success":false}`)
		}
	}
	for _, d := range batch.Deletes {
		k := byID[d["id"]]
		s := z.sets[k]
		z.del(s.Name, s.Type)
	}
	if len(batch.Posts) > 0 {
		p := batch.Posts[0]
		rs := RecordSet{Name: p.Name + ".", Type: p.Type, TTL: p.TTL, Proxied: p.Proxied}
		for _, q := range batch.Posts {
			v := q.Content
			if q.Type == "CNAME" {
				v += "."
			}
			rs.Values = append(rs.Values, v)
		}
		z.put(rs)
	}
	z.writes++
	return respond(200, `{"success":true}`)
}

var hosts = map[string]string{
	"route53":    "route53://Z0123456789ABC",
	"clouddns":   "clouddns://my-project/shop-zone",
	"azuredns":   "azuredns://00000000-0000-0000-0000-000000000000/rg/shop.example.test",
	"cloudflare": "cloudflare://0123456789abcdef0123456789abcdef",
}

func env(prefix string) func(string) string {
	vals := map[string]string{
		"AWS_ACCESS_KEY_ID": "AKID", "AWS_SECRET_ACCESS_KEY": "secret", "GOOGLE_OAUTH_ACCESS_TOKEN": "gtok",
		"AZURE_ACCESS_TOKEN": "atok", "CLOUDFLARE_API_TOKEN": "ctok",
	}
	return func(k string) string {
		if !strings.HasPrefix(k, prefix+"_") {
			return ""
		}
		return vals[strings.TrimPrefix(k, prefix+"_")]
	}
}

func openFake(t *testing.T, spec string, z *zoneFake) Zone {
	t.Helper()
	zone, err := Open(spec, "TARGET", env("TARGET"), &http.Client{Transport: z})
	if err != nil {
		t.Fatal(err)
	}
	switch h := zone.(type) {
	case *Route53:
		h.Endpoint = "https://route53.test"
	case *CloudDNS:
		h.Endpoint = "https://dns.test"
	case *AzureDNS:
		h.Endpoint = "https://azure.test"
	case *Cloudflare:
		h.Endpoint = "https://cf.test"
	}
	return zone
}

func TestReplaceOnEveryHost(t *testing.T) {
	old := RecordSet{Name: "www.shop.example.test.", Type: "A", TTL: 300, Values: []string{"198.51.100.7", "198.51.100.8"}}
	cname := RecordSet{Name: "api.shop.example.test.", Type: "CNAME", TTL: 60, Values: []string{"lb.old.example.test."}}
	for host, spec := range hosts {
		z := newZone(old, cname)
		zone := openFake(t, spec, z)
		ctx := context.Background()
		got, ok, err := zone.Get(ctx, old.Name, "A")
		if err != nil || !ok || !got.Equal(old) {
			t.Fatalf("%s get: %+v %v %v", host, got, ok, err)
		}
		if _, ok, err := zone.Get(ctx, old.Name, "AAAA"); ok || err != nil {
			t.Fatalf("%s: AAAA found %v %v", host, ok, err)
		}
		repl := RecordSet{Name: old.Name, Type: "A", TTL: 60, Values: []string{"203.0.113.9"}}
		if err := zone.Replace(ctx, &old, &repl); err != nil {
			t.Fatalf("%s replace: %v", host, err)
		}
		if got, _, _ := zone.Get(ctx, old.Name, "A"); !got.Equal(repl) {
			t.Fatalf("%s after: %+v", host, got)
		}
		// The plan saw old; someone changed it since, so it's refused.
		writes := z.writes
		if err := zone.Replace(ctx, &old, &repl); !errors.Is(err, ErrChanged) {
			t.Fatalf("%s stale replace: %v", host, err)
		}
		if z.writes != writes {
			t.Fatalf("%s wrote on a stale replace", host)
		}
		newCname := RecordSet{Name: cname.Name, Type: "CNAME", TTL: 60, Values: []string{"lb.new.example.test."}}
		if err := zone.Replace(ctx, &cname, &newCname); err != nil {
			t.Fatalf("%s cname: %v", host, err)
		}
		if got, _, _ := zone.Get(ctx, cname.Name, "CNAME"); !got.Equal(newCname) {
			t.Fatalf("%s cname after: %+v", host, got)
		}
		// A set that wasn't there is created, and removed again.
		fresh := RecordSet{Name: "new.shop.example.test.", Type: "A", TTL: 60, Values: []string{"203.0.113.10"}}
		if err := zone.Replace(ctx, nil, &fresh); err != nil {
			t.Fatalf("%s create: %v", host, err)
		}
		if err := zone.Replace(ctx, &fresh, nil); err != nil {
			t.Fatalf("%s delete: %v", host, err)
		}
		if _, ok, _ := zone.Get(ctx, fresh.Name, "A"); ok {
			t.Fatalf("%s: still there", host)
		}
		for _, a := range z.auth {
			if a == "" {
				t.Fatalf("%s: a call without credentials", host)
			}
		}
	}
}

// A name that was a CNAME can end as an address and go back, in one change
// where the host has one.
func TestReplaceChangesTheType(t *testing.T) {
	ctx := context.Background()
	cname := RecordSet{Name: "www.shop.example.test.", Type: "CNAME", TTL: 300, Values: []string{"lb.old.example.test."}}
	addr := RecordSet{Name: cname.Name, Type: "A", TTL: 60, Values: []string{"203.0.113.9"}}
	for host, spec := range hosts {
		z := newZone(cname)
		zone := openFake(t, spec, z)
		// The address can't simply be added beside the CNAME.
		if err := zone.Replace(ctx, nil, &addr); err == nil {
			t.Fatalf("%s: an A was made beside a CNAME", host)
		}
		if err := zone.Replace(ctx, &cname, &addr); err != nil {
			t.Fatalf("%s: %v", host, err)
		}
		if _, ok, _ := zone.Get(ctx, cname.Name, "CNAME"); ok {
			t.Fatalf("%s: the CNAME is still there", host)
		}
		if got, ok, _ := zone.Get(ctx, addr.Name, "A"); !ok || !got.Equal(addr) {
			t.Fatalf("%s: %+v", host, got)
		}
		want := 1
		if host == "azuredns" {
			want = 2
		}
		if z.writes != want {
			t.Fatalf("%s: %d writes", host, z.writes)
		}
		// The plan saw a CNAME that someone has since changed.
		if err := zone.Replace(ctx, &cname, &addr); !errors.Is(err, ErrChanged) {
			t.Fatalf("%s stale: %v", host, err)
		}
		if err := zone.Replace(ctx, &addr, &cname); err != nil {
			t.Fatalf("%s back: %v", host, err)
		}
		if got, ok, _ := zone.Get(ctx, cname.Name, "CNAME"); !ok || !got.Equal(cname) {
			t.Fatalf("%s back: %+v", host, got)
		}
		if _, ok, _ := zone.Get(ctx, addr.Name, "A"); ok {
			t.Fatalf("%s: the A is still there", host)
		}
	}

	// Azure deletes and then creates. A create that fails puts the old
	// set back, so the name isn't left with nothing.
	z := newZone(cname)
	zone := openFake(t, hosts["azuredns"], z)
	z.refuse = func(r *http.Request) bool { return r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/A/") }
	if err := zone.Replace(ctx, &cname, &addr); !errors.Is(err, ErrProvider) {
		t.Fatalf("a failed create: %v", err)
	}
	if got, ok, _ := zone.Get(ctx, cname.Name, "CNAME"); !ok || !got.Equal(cname) {
		t.Fatalf("the CNAME wasn't put back: %+v %v", got, ok)
	}
}

func TestRoute53SignsForRoute53(t *testing.T) {
	z := newZone()
	zone := openFake(t, hosts["route53"], z)
	if _, _, err := zone.Get(context.Background(), "www.shop.example.test.", "A"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(z.auth[0], "/us-east-1/route53/aws4_request") || !strings.HasPrefix(z.auth[0], "AWS4-HMAC-SHA256 Credential=AKID/") {
		t.Fatalf("%s", z.auth[0])
	}
}

func TestOpenRefusesSourceKeys(t *testing.T) {
	for _, spec := range hosts {
		if _, err := Open(spec, "SOURCE", env("SOURCE"), nil); !errors.Is(err, ErrSpec) {
			t.Errorf("%s with SOURCE keys: %v", spec, err)
		}
		if _, err := Open(spec, "DNS", env("TARGET"), nil); !errors.Is(err, ErrSpec) {
			t.Errorf("%s without its keys: %v", spec, err)
		}
		for _, n := range CredentialNames(spec, "DNS") {
			if !strings.HasPrefix(n, "DNS_") {
				t.Errorf("%s", n)
			}
		}
	}
	for _, bad := range []string{"route53://lower", "clouddns://p", "azuredns://s/rg", "cloudflare://nothex", "https://x", "route53://Z1?x=1"} {
		if _, err := Open(bad, "TARGET", env("TARGET"), nil); err == nil {
			t.Errorf("%s accepted", bad)
		}
	}
}

func TestValid(t *testing.T) {
	good := []RecordSet{
		{Name: "www.example.test.", Type: "A", TTL: 60, Values: []string{"203.0.113.1"}},
		{Name: "example.test.", Type: "AAAA", TTL: 60, Values: []string{"2001:db8::1"}},
		{Name: "api.example.test.", Type: "CNAME", TTL: 60, Values: []string{"lb.example.test."}},
	}
	for _, r := range good {
		if !r.Valid() {
			t.Errorf("%+v refused", r)
		}
	}
	bad := []RecordSet{
		{Name: "www.example.test", Type: "A", TTL: 60, Values: []string{"203.0.113.1"}},
		{Name: "www.example.test.", Type: "MX", TTL: 60, Values: []string{"203.0.113.1"}},
		{Name: "www.example.test.", Type: "A", TTL: 0, Values: []string{"203.0.113.1"}},
		{Name: "www.example.test.", Type: "A", TTL: 60, Values: []string{"lb.example.test."}},
		{Name: "www.example.test.", Type: "A", TTL: 60, Values: []string{"203.0.113.1", "203.0.113.1"}},
		{Name: "api.example.test.", Type: "CNAME", TTL: 60, Values: []string{"a.test.", "b.test."}},
		{Name: "WWW.example.test.", Type: "A", TTL: 60, Values: []string{"203.0.113.1"}},
		{Name: "x.example.test.", Type: "A", TTL: 60},
	}
	for _, r := range bad {
		if r.Valid() {
			t.Errorf("%+v accepted", r)
		}
	}
}
