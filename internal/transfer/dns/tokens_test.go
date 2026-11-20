package dns

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"strings"
	"testing"
)

// minting answers Google's and Microsoft's token addresses and hands every
// other call to the zone.
type minting struct {
	zone   *zoneFake
	scopes []string
	down   bool
}

func (m *minting) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Host != "oauth2.googleapis.com" && r.URL.Host != "login.microsoftonline.com" {
		return m.zone.RoundTrip(r)
	}
	if m.down {
		return respond(500, `{"error_description":"key AKIA-not-for-a-log refused"}`)
	}
	if err := r.ParseForm(); err != nil {
		return respond(400, "")
	}
	scope := r.PostForm.Get("scope")
	if assertion := r.PostForm.Get("assertion"); assertion != "" {
		scope = "a signed request"
	}
	m.scopes = append(m.scopes, scope)
	return respond(200, fmt.Sprintf(`{"access_token":"minted-%d","expires_in":3600}`, len(m.scopes)))
}

// A zone opens on a service account's key or an application's secret in
// place of a token, and each call goes out with a token made from it.
func TestOpenMakesTokensFromAKey(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	account, _ := json.Marshal(map[string]string{
		"type": "service_account", "client_email": "mover@example.iam.gserviceaccount.com",
		"private_key": string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})),
	})
	old := RecordSet{Name: "www.shop.example.test.", Type: "A", TTL: 300, Values: []string{"198.51.100.7"}}
	repl := RecordSet{Name: old.Name, Type: "A", TTL: 60, Values: []string{"203.0.113.9"}}
	for host, vals := range map[string]map[string]string{
		"clouddns": {"DNS_GOOGLE_SERVICE_ACCOUNT_KEY": string(account), "DNS_GOOGLE_OAUTH_ACCESS_TOKEN": "stale"},
		"azuredns": {
			"DNS_AZURE_TENANT_ID": "11111111-1111-1111-1111-111111111111", "DNS_AZURE_CLIENT_ID": "22222222-2222-2222-2222-222222222222",
			"DNS_AZURE_CLIENT_SECRET": "s3cret", "DNS_AZURE_ACCESS_TOKEN": "stale",
		},
	} {
		m := &minting{zone: newZone(old)}
		zone, err := Open(hosts[host], "DNS", func(k string) string { return vals[k] }, &http.Client{Transport: m})
		if err != nil {
			t.Fatalf("%s: %v", host, err)
		}
		switch h := zone.(type) {
		case *CloudDNS:
			h.Endpoint = "https://dns.test"
		case *AzureDNS:
			h.Endpoint = "https://azure.test"
		}
		ctx := context.Background()
		if err := zone.Replace(ctx, &old, &repl); err != nil {
			t.Fatalf("%s: %v", host, err)
		}
		if got, _, _ := zone.Get(ctx, old.Name, "A"); !got.Equal(repl) {
			t.Fatalf("%s: %+v", host, got)
		}
		// One token, for managing DNS, on every call; the fixed token the
		// key wins over went nowhere.
		if len(m.scopes) != 1 || host == "azuredns" && m.scopes[0] != "https://management.azure.com/.default" {
			t.Fatalf("%s: tokens for %q", host, m.scopes)
		}
		for _, a := range m.zone.auth {
			if a != "Bearer minted-1" {
				t.Fatalf("%s: a call went out with %q", host, a)
			}
		}

		// A key the provider refuses fails the call and names nothing.
		m = &minting{zone: newZone(old), down: true}
		zone, err = Open(hosts[host], "DNS", func(k string) string { return vals[k] }, &http.Client{Transport: m})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := zone.Get(ctx, old.Name, "A"); err == nil || strings.Contains(err.Error(), "AKIA") || len(m.zone.auth) != 0 {
			t.Fatalf("%s: %v, %d calls", host, err, len(m.zone.auth))
		}
	}

	// A key that isn't one is refused when the zone opens, and a zone
	// still needs one or the other.
	for _, vals := range []map[string]string{
		{"DNS_GOOGLE_SERVICE_ACCOUNT_KEY": "{}", "DNS_GOOGLE_OAUTH_ACCESS_TOKEN": "tok"},
		{},
	} {
		if _, err := Open(hosts["clouddns"], "DNS", func(k string) string { return vals[k] }, nil); err == nil {
			t.Fatalf("opened with %v", len(vals))
		}
	}
	if _, err := Open(hosts["azuredns"], "DNS", func(k string) string {
		return map[string]string{"DNS_AZURE_CLIENT_SECRET": "s3cret", "DNS_AZURE_TENANT_ID": "not-a-tenant"}[k]
	}, nil); err == nil {
		t.Fatal("opened with a bad tenant")
	}
	for _, spec := range hosts {
		if err := Check(spec, "DNS"); err != nil {
			t.Fatalf("%s: %v", spec, err)
		}
	}
	if Check("clouddns://my-project", "DNS") == nil || Check(hosts["route53"], "SOURCE") == nil {
		t.Fatal("a bad zone checked out")
	}
}
