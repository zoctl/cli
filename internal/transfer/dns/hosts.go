package dns

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/zoctl/cli/internal/transfer/objects"
)

// Route53 is a hosted zone. A change batch deletes the exact old set and
// creates the new one in one transaction, so Route 53 itself refuses it if
// the old set changed.
type Route53 struct {
	Zone                                       string
	AccessKeyID, SecretAccessKey, SessionToken string
	Client                                     *http.Client
	Endpoint                                   string // tests only
	now                                        func() time.Time
}

const route53NS = "https://route53.amazonaws.com/doc/2013-04-01/"

type r53Set struct {
	Name    string `xml:"Name"`
	Type    string `xml:"Type"`
	TTL     int    `xml:"TTL,omitempty"`
	Records []struct {
		Value string `xml:"Value"`
	} `xml:"ResourceRecords>ResourceRecord"`
	Alias         *struct{} `xml:"AliasTarget"`
	SetIdentifier string    `xml:"SetIdentifier"`
}

func (r *Route53) do(ctx context.Context, method, path, query string, body []byte) ([]byte, error) {
	endpoint := r.Endpoint
	if endpoint == "" {
		endpoint = "https://route53.amazonaws.com"
	}
	req, err := http.NewRequest(method, endpoint+path, bytes.NewReader(body))
	if err != nil {
		return nil, ErrSpec
	}
	req.URL.RawQuery = query
	sum := sha256.Sum256(body)
	if len(body) > 0 {
		req.Header.Set("Content-Type", "application/xml")
	}
	now := time.Now
	if r.now != nil {
		now = r.now
	}
	objects.SignV4(req, objects.Credentials{AccessKeyID: r.AccessKeyID, SecretAccessKey: r.SecretAccessKey, SessionToken: r.SessionToken},
		"route53", "us-east-1", hex.EncodeToString(sum[:]), now())
	b, _, err := call(ctx, r.Client, req, http.StatusBadRequest)
	return b, err
}

func (r *Route53) Get(ctx context.Context, name, typ string) (RecordSet, bool, error) {
	b, err := r.do(ctx, http.MethodGet, "/2013-04-01/hostedzone/"+r.Zone+"/rrset", "maxitems=1&name="+url.QueryEscape(name)+"&type="+typ, nil)
	if err != nil {
		if err == ErrChanged {
			err = ErrProvider
		}
		return RecordSet{}, false, err
	}
	var out struct {
		Sets []r53Set `xml:"ResourceRecordSets>ResourceRecordSet"`
	}
	if xml.Unmarshal(b, &out) != nil {
		return RecordSet{}, false, ErrProvider
	}
	// The list starts at name and type; the first set may be the next one.
	if len(out.Sets) == 0 || !strings.EqualFold(unescapeR53(out.Sets[0].Name), name) || out.Sets[0].Type != typ {
		return RecordSet{}, false, nil
	}
	s := out.Sets[0]
	if s.Alias != nil || s.SetIdentifier != "" {
		return RecordSet{}, false, ErrUnsupported
	}
	rs := RecordSet{Name: name, Type: typ, TTL: s.TTL}
	for _, v := range s.Records {
		rs.Values = append(rs.Values, v.Value)
	}
	return rs, true, nil
}

// unescapeR53 turns Route 53's octal escapes back, for the one it uses
// in ordinary names: \052 for a wildcard.
func unescapeR53(s string) string { return strings.ReplaceAll(s, `\052`, "*") }

func (r *Route53) Replace(ctx context.Context, old, new *RecordSet) error {
	var changes strings.Builder
	add := func(action string, s *RecordSet) {
		if s == nil {
			return
		}
		changes.WriteString("<Change><Action>" + action + "</Action><ResourceRecordSet><Name>")
		xml.EscapeText(&changes, []byte(s.Name))
		changes.WriteString("</Name><Type>" + s.Type + "</Type><TTL>")
		changes.WriteString(strconv.Itoa(s.TTL))
		changes.WriteString("</TTL><ResourceRecords>")
		for _, v := range s.Values {
			changes.WriteString("<ResourceRecord><Value>")
			xml.EscapeText(&changes, []byte(v))
			changes.WriteString("</Value></ResourceRecord>")
		}
		changes.WriteString("</ResourceRecords></ResourceRecordSet></Change>")
	}
	add("DELETE", old)
	add("CREATE", new)
	body := `<?xml version="1.0" encoding="UTF-8"?><ChangeResourceRecordSetsRequest xmlns="` + route53NS + `"><ChangeBatch><Comment>zoctl cutover</Comment><Changes>` +
		changes.String() + `</Changes></ChangeBatch></ChangeResourceRecordSetsRequest>`
	_, err := r.do(ctx, http.MethodPost, "/2013-04-01/hostedzone/"+r.Zone+"/rrset/", "", []byte(body))
	return err
}

// CloudDNS is a Google Cloud DNS managed zone. A change's deletions must
// match exactly, so Google refuses it if the old set changed.
type CloudDNS struct {
	Project, ManagedZone, Token string
	Tokens                      *objects.Tokens
	Client                      *http.Client
	Endpoint                    string // tests only
}

type gcpSet struct {
	Name    string   `json:"name"`
	Type    string   `json:"type"`
	TTL     int      `json:"ttl"`
	RRDatas []string `json:"rrdatas"`
}

func (g *CloudDNS) url(path string) string {
	e := g.Endpoint
	if e == "" {
		e = "https://dns.googleapis.com"
	}
	return e + "/dns/v1/projects/" + url.PathEscape(g.Project) + "/managedZones/" + url.PathEscape(g.ManagedZone) + path
}

func (g *CloudDNS) Get(ctx context.Context, name, typ string) (RecordSet, bool, error) {
	token, err := bearer(ctx, g.Token, g.Tokens)
	if err != nil {
		return RecordSet{}, false, err
	}
	req, _ := http.NewRequest(http.MethodGet, g.url("/rrsets?"+url.Values{"name": {name}, "type": {typ}}.Encode()), nil)
	req.Header.Set("Authorization", "Bearer "+token)
	b, code, err := call(ctx, g.Client, req)
	if err != nil || code == http.StatusNotFound {
		return RecordSet{}, false, ErrProvider
	}
	var out struct {
		RRSets []gcpSet `json:"rrsets"`
	}
	if json.Unmarshal(b, &out) != nil || len(out.RRSets) > 1 {
		return RecordSet{}, false, ErrProvider
	}
	if len(out.RRSets) == 0 {
		return RecordSet{}, false, nil
	}
	s := out.RRSets[0]
	if !strings.EqualFold(s.Name, name) || s.Type != typ {
		return RecordSet{}, false, ErrProvider
	}
	return RecordSet{Name: name, Type: typ, TTL: s.TTL, Values: s.RRDatas}, true, nil
}

func (g *CloudDNS) Replace(ctx context.Context, old, new *RecordSet) error {
	var body struct {
		Additions []gcpSet `json:"additions,omitempty"`
		Deletions []gcpSet `json:"deletions,omitempty"`
	}
	if old != nil {
		body.Deletions = []gcpSet{{old.Name, old.Type, old.TTL, old.Values}}
	}
	if new != nil {
		body.Additions = []gcpSet{{new.Name, new.Type, new.TTL, new.Values}}
	}
	token, err := bearer(ctx, g.Token, g.Tokens)
	if err != nil {
		return err
	}
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, g.url("/changes"), bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	_, code, err := call(ctx, g.Client, req, http.StatusConflict, http.StatusPreconditionFailed)
	if err == nil && code == http.StatusNotFound {
		return ErrProvider
	}
	return err
}

// AzureDNS is an Azure DNS zone. Replacing sends the old set's ETag, so
// Azure refuses it if the set changed. Azure keeps each type of a name as
// a resource of its own and has no call that changes two, so a replace
// that changes the type deletes the old set and then makes the new one:
// for the moment between, the name has no record.
type AzureDNS struct {
	Subscription, ResourceGroup, ZoneName, Token string
	Tokens                                       *objects.Tokens
	Client                                       *http.Client
	Endpoint                                     string // tests only
}

const azureDNSVersion = "2018-05-01"

func (a *AzureDNS) url(name, typ string) (string, bool) {
	zone := strings.ToLower(a.ZoneName) + "."
	n := strings.ToLower(name)
	rel := "@"
	if n != zone {
		if !strings.HasSuffix(n, "."+zone) {
			return "", false
		}
		rel = strings.TrimSuffix(n, "."+zone)
	}
	e := a.Endpoint
	if e == "" {
		e = "https://management.azure.com"
	}
	return e + "/subscriptions/" + url.PathEscape(a.Subscription) + "/resourceGroups/" + url.PathEscape(a.ResourceGroup) +
		"/providers/Microsoft.Network/dnsZones/" + url.PathEscape(a.ZoneName) + "/" + typ + "/" + url.PathEscape(rel) + "?api-version=" + azureDNSVersion, true
}

type azureSet struct {
	ETag       string `json:"etag,omitempty"`
	Properties struct {
		TTL       int                 `json:"TTL"`
		A         []map[string]string `json:"ARecords,omitempty"`
		AAAA      []map[string]string `json:"AAAARecords,omitempty"`
		CNAME     map[string]string   `json:"CNAMERecord,omitempty"`
		TargetRes map[string]any      `json:"targetResource,omitempty"`
	} `json:"properties"`
}

func (a *AzureDNS) get(ctx context.Context, name, typ string) (RecordSet, string, bool, error) {
	u, ok := a.url(name, typ)
	if !ok {
		return RecordSet{}, "", false, ErrSpec
	}
	token, err := bearer(ctx, a.Token, a.Tokens)
	if err != nil {
		return RecordSet{}, "", false, err
	}
	req, _ := http.NewRequest(http.MethodGet, u, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	b, code, err := call(ctx, a.Client, req)
	if err != nil {
		return RecordSet{}, "", false, err
	}
	if code == http.StatusNotFound {
		return RecordSet{}, "", false, nil
	}
	var s azureSet
	if json.Unmarshal(b, &s) != nil {
		return RecordSet{}, "", false, ErrProvider
	}
	if len(s.Properties.TargetRes) > 0 && s.Properties.TargetRes["id"] != nil {
		return RecordSet{}, "", false, ErrUnsupported
	}
	rs := RecordSet{Name: name, Type: typ, TTL: s.Properties.TTL}
	switch typ {
	case "A":
		for _, r := range s.Properties.A {
			rs.Values = append(rs.Values, r["ipv4Address"])
		}
	case "AAAA":
		for _, r := range s.Properties.AAAA {
			rs.Values = append(rs.Values, r["ipv6Address"])
		}
	case "CNAME":
		if c := s.Properties.CNAME["cname"]; c != "" {
			rs.Values = []string{strings.TrimSuffix(c, ".") + "."}
		}
	}
	return rs, s.ETag, true, nil
}

func (a *AzureDNS) Get(ctx context.Context, name, typ string) (RecordSet, bool, error) {
	rs, _, ok, err := a.get(ctx, name, typ)
	return rs, ok, err
}

func (a *AzureDNS) Replace(ctx context.Context, old, new *RecordSet) error {
	set := old
	if set == nil {
		set = new
	}
	if set == nil || (old != nil && new != nil && !strings.EqualFold(old.Name, new.Name)) {
		return ErrSpec
	}
	if old != nil && new != nil && old.Type != new.Type {
		if _, _, there, err := a.get(ctx, new.Name, new.Type); err != nil {
			return err
		} else if there {
			return ErrChanged
		}
		if err := a.Replace(ctx, old, nil); err != nil {
			return err
		}
		err := a.Replace(ctx, nil, new)
		if err != nil {
			// The name has nothing now, so the old set goes back, on a
			// context an interrupt hasn't ended.
			back, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
			defer cancel()
			_ = a.Replace(back, nil, old)
		}
		return err
	}
	cur, etag, exists, err := a.get(ctx, set.Name, set.Type)
	if err != nil {
		return err
	}
	if exists != (old != nil) || (old != nil && !cur.Equal(*old)) {
		return ErrChanged
	}
	token, err := bearer(ctx, a.Token, a.Tokens)
	if err != nil {
		return err
	}
	u, _ := a.url(set.Name, set.Type)
	var req *http.Request
	if new == nil {
		req, _ = http.NewRequest(http.MethodDelete, u, nil)
	} else {
		props := map[string]any{"TTL": new.TTL}
		switch new.Type {
		case "A", "AAAA":
			key, field := "ARecords", "ipv4Address"
			if new.Type == "AAAA" {
				key, field = "AAAARecords", "ipv6Address"
			}
			var recs []map[string]string
			for _, v := range new.Values {
				recs = append(recs, map[string]string{field: v})
			}
			props[key] = recs
		case "CNAME":
			props["CNAMERecord"] = map[string]string{"cname": new.Values[0]}
		}
		b, _ := json.Marshal(map[string]any{"properties": props})
		req, _ = http.NewRequest(http.MethodPut, u, bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if old != nil {
		req.Header.Set("If-Match", etag)
	} else {
		req.Header.Set("If-None-Match", "*")
	}
	_, code, err := call(ctx, a.Client, req, http.StatusPreconditionFailed)
	if err == nil && code == http.StatusNotFound {
		return ErrProvider
	}
	return err
}

// Cloudflare is a Cloudflare zone, one record per value. A replace reads
// the records again, refuses if they differ from old, then sends one batch
// that deletes them by ID and posts the new ones, which Cloudflare applies
// all or nothing, so the new set can be of another type than the old.
// Cloudflare has no condition on a record's content, so an edit between
// that read and the batch isn't caught; a deleted record fails the batch.
type Cloudflare struct {
	ZoneID, Token string
	Client        *http.Client
	Endpoint      string // tests only
}

type cfRecord struct {
	ID       string `json:"id,omitempty"`
	Name     string `json:"name"`
	Type     string `json:"type"`
	Content  string `json:"content"`
	TTL      int    `json:"ttl"`
	Proxied  bool   `json:"proxied"`
	Comment  string `json:"comment,omitempty"`
	Priority *int   `json:"priority,omitempty"`
}

func (f *Cloudflare) url(path string) string {
	e := f.Endpoint
	if e == "" {
		e = "https://api.cloudflare.com"
	}
	return e + "/client/v4/zones/" + f.ZoneID + "/dns_records" + path
}

func (f *Cloudflare) list(ctx context.Context, name, typ string) ([]cfRecord, error) {
	req, _ := http.NewRequest(http.MethodGet, f.url("?"+url.Values{"name": {strings.TrimSuffix(name, ".")}, "type": {typ}, "per_page": {"100"}}.Encode()), nil)
	req.Header.Set("Authorization", "Bearer "+f.Token)
	b, code, err := call(ctx, f.Client, req)
	if err != nil || code == http.StatusNotFound {
		return nil, ErrProvider
	}
	var out struct {
		Success bool       `json:"success"`
		Result  []cfRecord `json:"result"`
	}
	if json.Unmarshal(b, &out) != nil || !out.Success || len(out.Result) >= 100 {
		return nil, ErrProvider
	}
	return out.Result, nil
}

func (f *Cloudflare) Get(ctx context.Context, name, typ string) (RecordSet, bool, error) {
	recs, err := f.list(ctx, name, typ)
	if err != nil || len(recs) == 0 {
		return RecordSet{}, false, err
	}
	rs := RecordSet{Name: name, Type: typ, TTL: recs[0].TTL, Proxied: recs[0].Proxied}
	for _, r := range recs {
		if r.TTL != rs.TTL || r.Proxied != rs.Proxied || !strings.EqualFold(r.Name+".", name) || r.Type != typ {
			return RecordSet{}, false, ErrUnsupported
		}
		v := r.Content
		if typ == "CNAME" {
			v = strings.TrimSuffix(v, ".") + "."
		}
		rs.Values = append(rs.Values, v)
	}
	return rs, true, nil
}

func (f *Cloudflare) Replace(ctx context.Context, old, new *RecordSet) error {
	set := old
	if set == nil {
		set = new
	}
	if set == nil {
		return ErrSpec
	}
	recs, err := f.list(ctx, set.Name, set.Type)
	if err != nil {
		return err
	}
	var cur *RecordSet
	if len(recs) > 0 {
		c, _, err := f.Get(ctx, set.Name, set.Type)
		if err != nil {
			return err
		}
		cur = &c
	}
	if (cur == nil) != (old == nil) || (old != nil && !cur.Equal(*old)) {
		return ErrChanged
	}
	var batch struct {
		Deletes []map[string]string `json:"deletes,omitempty"`
		Posts   []cfRecord          `json:"posts,omitempty"`
	}
	for _, r := range recs {
		batch.Deletes = append(batch.Deletes, map[string]string{"id": r.ID})
	}
	if new != nil {
		// Cloudflare sets a proxied record's TTL itself, and says 1 for it.
		ttl := new.TTL
		if new.Proxied {
			ttl = 1
		}
		for _, v := range new.Values {
			batch.Posts = append(batch.Posts, cfRecord{Name: strings.TrimSuffix(new.Name, "."), Type: new.Type,
				Content: strings.TrimSuffix(v, "."), TTL: ttl, Proxied: new.Proxied})
		}
	}
	b, _ := json.Marshal(batch)
	req, _ := http.NewRequest(http.MethodPost, f.url("/batch"), bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer "+f.Token)
	req.Header.Set("Content-Type", "application/json")
	_, code, err := call(ctx, f.Client, req)
	if err == nil && code == http.StatusNotFound {
		return ErrChanged
	}
	return err
}
