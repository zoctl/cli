// Package dns reads and replaces the record sets a cutover moves. A zone is
// on the target or with a DNS host the customer controls, never in the
// source account: credentials come only from TARGET_ or DNS_ names, so a
// SOURCE_ key can't reach it. A replace names the record set it expects to
// find, and fails with ErrChanged when someone else changed it first.
package dns

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/zoctl/cli/internal/transfer/objects"
)

// bearer is the token a call goes out with: tokens' when there is one. A
// token that can't be had is the host refusing the call.
func bearer(ctx context.Context, fixed string, tokens *objects.Tokens) (string, error) {
	if tokens == nil {
		return fixed, nil
	}
	t, err := tokens.Token(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", ErrProvider
	}
	return t, nil
}

var (
	// ErrSpec means a zone spec, record or credential was refused.
	ErrSpec = errors.New("dns: refused")
	// ErrChanged means the record set wasn't what the plan saw.
	ErrChanged = errors.New("dns: the record changed since the plan")
	// ErrUnsupported means a record set this package won't touch, such as
	// a Route 53 alias or a Cloudflare record with more settings than it
	// keeps.
	ErrUnsupported = errors.New("dns: record set not supported")
	// ErrProvider means the DNS host refused or failed a call.
	ErrProvider = errors.New("dns: the DNS host refused the call")
)

const (
	maxResponse = 1 << 20
	timeout     = 30 * time.Second
)

// RecordSet is every value of one name and type. Name is fully qualified
// with the trailing dot. Proxied is Cloudflare's own setting, kept so a
// replace doesn't change it.
type RecordSet struct {
	Name    string   `json:"name"`
	Type    string   `json:"type"`
	TTL     int      `json:"ttl"`
	Values  []string `json:"values"`
	Proxied bool     `json:"proxied,omitempty"`
}

// Equal compares two sets, values in any order.
func (r RecordSet) Equal(o RecordSet) bool {
	if !strings.EqualFold(r.Name, o.Name) || r.Type != o.Type || r.TTL != o.TTL || r.Proxied != o.Proxied || len(r.Values) != len(o.Values) {
		return false
	}
	a, b := sorted(r.Values), sorted(o.Values)
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// SameValues compares only what the set points at.
func (r RecordSet) SameValues(values []string) bool {
	return r.Equal(RecordSet{Name: r.Name, Type: r.Type, TTL: r.TTL, Proxied: r.Proxied, Values: values})
}

func sorted(v []string) []string {
	out := append([]string{}, v...)
	sort.Strings(out)
	return out
}

var (
	namePattern = regexp.MustCompile(`^([a-z0-9_]([a-z0-9_-]{0,61}[a-z0-9_])?\.)+$`)
	ipv4        = regexp.MustCompile(`^(\d{1,3}\.){3}\d{1,3}$`)
	ipv6        = regexp.MustCompile(`^[0-9a-f:.]{2,45}$`)
)

// Types are the record types a cutover changes: where a name points.
var Types = map[string]bool{"A": true, "AAAA": true, "CNAME": true}

// ValidName checks the name and type of a record set.
func ValidName(name, typ string) bool {
	return namePattern.MatchString(name) && len(name) <= 254 && Types[typ]
}

// Valid checks a record set before it goes to a DNS host.
func (r RecordSet) Valid() bool {
	if !namePattern.MatchString(r.Name) || len(r.Name) > 254 || !Types[r.Type] || r.TTL < 1 || r.TTL > 86400 || len(r.Values) == 0 || len(r.Values) > 20 {
		return false
	}
	if r.Type == "CNAME" && len(r.Values) != 1 {
		return false
	}
	seen := map[string]bool{}
	for _, v := range r.Values {
		ok := false
		switch r.Type {
		case "A":
			ok = ipv4.MatchString(v)
		case "AAAA":
			ok = ipv6.MatchString(v) && strings.Contains(v, ":")
		case "CNAME":
			ok = namePattern.MatchString(v)
		}
		if !ok || seen[v] {
			return false
		}
		seen[v] = true
	}
	return true
}

// Zone is one DNS zone. Get returns false when the name has no set of that
// type. Replace changes old to new atomically where the host can: either
// may be nil, for a set that isn't there before or after.
type Zone interface {
	Get(ctx context.Context, name, typ string) (RecordSet, bool, error)
	Replace(ctx context.Context, old, new *RecordSet) error
}

// Open makes a zone from a spec, with credentials read from prefix_NAME:
//
//	route53://ZONEID              AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN
//	clouddns://project/zone       GOOGLE_OAUTH_ACCESS_TOKEN, or GOOGLE_SERVICE_ACCOUNT_KEY
//	azuredns://subscription/group/zone.example.com   AZURE_ACCESS_TOKEN, or AZURE_TENANT_ID, AZURE_CLIENT_ID and AZURE_CLIENT_SECRET
//	cloudflare://zoneid           CLOUDFLARE_API_TOKEN
//
// prefix is TARGET or DNS. A token from Google or Microsoft ends within the
// hour; a service account's JSON key or an application's secret makes a
// new one as each ends, and is used when it is set.
func Open(spec, prefix string, env func(string) string, c *http.Client) (Zone, error) {
	return open(spec, prefix, env, c, true)
}

// Check reports whether spec names a zone Open would take, given its keys.
func Check(spec, prefix string) error {
	_, err := open(spec, prefix, func(string) string { return "" }, nil, false)
	return err
}

const (
	googleDNSScope = "https://www.googleapis.com/auth/ndev.clouddns.readwrite"
	azureDNSScope  = "https://management.azure.com/.default"
)

func open(spec, prefix string, env func(string) string, c *http.Client, keys bool) (Zone, error) {
	if prefix != "TARGET" && prefix != "DNS" {
		return nil, ErrSpec
	}
	u, err := url.Parse(spec)
	if err != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, ErrSpec
	}
	get := func(n string) string { return env(prefix + "_" + n) }
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if u.Path == "" {
		parts = nil
	}
	if c == nil {
		c = &http.Client{}
	}
	switch u.Scheme {
	case "route53":
		r := &Route53{Zone: u.Host, Client: c, AccessKeyID: get("AWS_ACCESS_KEY_ID"), SecretAccessKey: get("AWS_SECRET_ACCESS_KEY"), SessionToken: get("AWS_SESSION_TOKEN")}
		if !route53ID.MatchString(r.Zone) || len(parts) != 0 || keys && (r.AccessKeyID == "" || r.SecretAccessKey == "") {
			return nil, ErrSpec
		}
		return r, nil
	case "clouddns":
		g := &CloudDNS{Project: u.Host, Client: c, Token: get("GOOGLE_OAUTH_ACCESS_TOKEN")}
		if len(parts) != 1 || !segment.MatchString(g.Project) || !segment.MatchString(parts[0]) {
			return nil, ErrSpec
		}
		if key := get("GOOGLE_SERVICE_ACCOUNT_KEY"); key != "" {
			if g.Tokens, err = objects.GoogleKey(key, googleDNSScope, c); err != nil {
				return nil, ErrSpec
			}
		}
		if keys && g.Token == "" && g.Tokens == nil {
			return nil, ErrSpec
		}
		g.ManagedZone = parts[0]
		return g, nil
	case "azuredns":
		a := &AzureDNS{Subscription: u.Host, Client: c, Token: get("AZURE_ACCESS_TOKEN")}
		if len(parts) != 2 || !segment.MatchString(a.Subscription) || !segment.MatchString(parts[0]) || !namePattern.MatchString(parts[1]+".") {
			return nil, ErrSpec
		}
		if secret := get("AZURE_CLIENT_SECRET"); secret != "" {
			if a.Tokens, err = objects.AzureSecret(get("AZURE_TENANT_ID"), get("AZURE_CLIENT_ID"), secret, azureDNSScope, c); err != nil {
				return nil, ErrSpec
			}
		}
		if keys && a.Token == "" && a.Tokens == nil {
			return nil, ErrSpec
		}
		a.ResourceGroup, a.ZoneName = parts[0], parts[1]
		return a, nil
	case "cloudflare":
		f := &Cloudflare{ZoneID: u.Host, Client: c, Token: get("CLOUDFLARE_API_TOKEN")}
		if len(parts) != 0 || !hex32.MatchString(f.ZoneID) || keys && f.Token == "" {
			return nil, ErrSpec
		}
		return f, nil
	}
	return nil, ErrSpec
}

// CredentialNames lists the environment names Open reads for spec.
func CredentialNames(spec, prefix string) []string {
	scheme, _, _ := strings.Cut(spec, "://")
	var names []string
	switch scheme {
	case "route53":
		names = []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"}
	case "clouddns":
		names = []string{"GOOGLE_OAUTH_ACCESS_TOKEN", "GOOGLE_SERVICE_ACCOUNT_KEY"}
	case "azuredns":
		names = []string{"AZURE_ACCESS_TOKEN", "AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET"}
	case "cloudflare":
		names = []string{"CLOUDFLARE_API_TOKEN"}
	}
	for i := range names {
		names[i] = prefix + "_" + names[i]
	}
	return names
}

var (
	route53ID = regexp.MustCompile(`^[A-Z0-9]{1,32}$`)
	segment   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._()-]{0,89}$`)
	hex32     = regexp.MustCompile(`^[0-9a-f]{32}$`)
)

// call does one request and reads a bounded body. Codes in changed map to
// ErrChanged; any other non-2xx is ErrProvider. A redirect isn't followed.
func call(ctx context.Context, c *http.Client, r *http.Request, changed ...int) ([]byte, int, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cc := *c
	cc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := cc.Do(r.WithContext(ctx))
	if err != nil {
		return nil, 0, ErrProvider
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxResponse))
	if err != nil {
		return nil, resp.StatusCode, ErrProvider
	}
	for _, code := range changed {
		if resp.StatusCode == code {
			return b, resp.StatusCode, ErrChanged
		}
	}
	if resp.StatusCode/100 != 2 && resp.StatusCode != http.StatusNotFound {
		return b, resp.StatusCode, ErrProvider
	}
	return b, resp.StatusCode, nil
}
