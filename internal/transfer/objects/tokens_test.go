package objects

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

// issuer answers token requests and counts them.
type issuer struct {
	t      *testing.T
	check  func(r *http.Request, form url.Values)
	issued int
	status int
}

func (i *issuer) RoundTrip(r *http.Request) (*http.Response, error) {
	b, _ := io.ReadAll(r.Body)
	form, _ := url.ParseQuery(string(b))
	if r.Method != http.MethodPost || r.URL.Scheme != "https" {
		i.t.Errorf("token asked for with %s %s", r.Method, r.URL)
	}
	i.check(r, form)
	if i.status != 0 {
		return respond(r, i.status, `{"error":"invalid_grant","error_description":"SECRET-DETAIL"}`, nil), nil
	}
	i.issued++
	return respond(r, 200, fmt.Sprintf(`{"access_token":"token-%d","expires_in":3600,"token_type":"Bearer"}`, i.issued), nil), nil
}

func serviceAccount(t *testing.T) (string, *rsa.PrivateKey) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	doc, _ := json.Marshal(map[string]string{
		"type": "service_account", "client_email": "sync@example.iam.gserviceaccount.com",
		"private_key": string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})),
		// A key file's own address is never where the key goes.
		"token_uri": "https://elsewhere.example.test/token",
	})
	return string(doc), key
}

func TestGoogleKeyMakesTokensAsTheyEnd(t *testing.T) {
	ctx := context.Background()
	doc, key := serviceAccount(t)
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	for _, c := range []struct {
		scope string
	}{{GoogleStorageRead}, {GoogleStorageWrite}} {
		iss := &issuer{t: t}
		iss.check = func(r *http.Request, form url.Values) {
			if r.URL.String() != googleTokenURL || form.Get("grant_type") != "urn:ietf:params:oauth:grant-type:jwt-bearer" {
				t.Errorf("asked %s with %q", r.URL, form.Get("grant_type"))
			}
			parts := strings.Split(form.Get("assertion"), ".")
			if len(parts) != 3 {
				t.Fatal("no assertion")
			}
			sig, _ := base64.RawURLEncoding.DecodeString(parts[2])
			sum := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
			if rsa.VerifyPKCS1v15(&key.PublicKey, crypto.SHA256, sum[:], sig) != nil {
				t.Error("the assertion isn't signed with the key")
			}
			var claims struct {
				Iss, Scope, Aud string
				Iat, Exp        int64
			}
			raw, _ := base64.RawURLEncoding.DecodeString(parts[1])
			if json.Unmarshal(raw, &claims) != nil || claims.Iss != "sync@example.iam.gserviceaccount.com" || claims.Scope != c.scope || claims.Aud != googleTokenURL || claims.Exp-claims.Iat != 3600 {
				t.Errorf("claims %+v", claims)
			}
		}
		tok, err := GoogleKey(doc, c.scope, &http.Client{Transport: iss})
		if err != nil {
			t.Fatal(err)
		}
		tok.now = func() time.Time { return now }
		a, err := tok.Token(ctx)
		b, _ := tok.Token(ctx)
		if err != nil || a != "token-1" || b != a || iss.issued != 1 {
			t.Fatalf("%q %q %v after %d requests", a, b, err, iss.issued)
		}
		tok.now = func() time.Time { return now.Add(54 * time.Minute) }
		if a, _ = tok.Token(ctx); a != "token-1" {
			t.Fatal("replaced a token with six minutes left")
		}
		tok.now = func() time.Time { return now.Add(56 * time.Minute) }
		if a, err = tok.Token(ctx); err != nil || a != "token-2" || iss.issued != 2 {
			t.Fatalf("a token four minutes from its end was kept: %q %v", a, err)
		}
		// A refusal is tried again later, and says nothing Google said.
		iss.status = 400
		tok.now = func() time.Time { return now.Add(3 * time.Hour) }
		if _, err = tok.Token(ctx); !errors.Is(err, ErrStatus) || strings.Contains(err.Error(), "SECRET") || strings.Contains(err.Error(), "invalid_grant") {
			t.Fatalf("refused: %v", err)
		}
		iss.status = 0
		if a, err = tok.Token(ctx); err != nil || a != "token-3" {
			t.Fatalf("after a refusal: %q %v", a, err)
		}
	}
	for _, bad := range []string{"", "{}", `{"type":"authorized_user","client_email":"a","private_key":"b"}`, `{"type":"service_account","client_email":"a","private_key":"not a key"}`} {
		if _, err := GoogleKey(bad, GoogleStorageRead, nil); !errors.Is(err, ErrConfig) {
			t.Errorf("%q: %v", bad, err)
		}
	}
}

const (
	tenant = "11111111-2222-3333-4444-555555555555"
	appID  = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
)

func TestAzureSecretMakesTokens(t *testing.T) {
	iss := &issuer{t: t}
	iss.check = func(r *http.Request, form url.Values) {
		if r.URL.String() != "https://login.microsoftonline.com/"+tenant+"/oauth2/v2.0/token" {
			t.Errorf("asked %s", r.URL)
		}
		if form.Get("grant_type") != "client_credentials" || form.Get("client_id") != appID || form.Get("client_secret") != "s3cret" || form.Get("scope") != "https://storage.azure.com/.default" {
			t.Errorf("form %v", form)
		}
		if strings.Contains(r.URL.RawQuery, "s3cret") {
			t.Error("the secret is in the URL")
		}
	}
	tok, err := AzureSecret(tenant, appID, "s3cret", AzureStorage, &http.Client{Transport: iss})
	if err != nil {
		t.Fatal(err)
	}
	if a, err := tok.Token(context.Background()); err != nil || a != "token-1" {
		t.Fatalf("%q %v", a, err)
	}
	for _, bad := range [][3]string{{"common", appID, "s"}, {tenant, "app", "s"}, {tenant, appID, ""}, {tenant + "/x", appID, "s"}} {
		if _, err := AzureSecret(bad[0], bad[1], bad[2], AzureStorage, nil); !errors.Is(err, ErrConfig) {
			t.Errorf("%v accepted", bad)
		}
	}
}

// both sends token requests to an issuer and everything else to a bucket.
type both struct {
	iss    http.RoundTripper
	bucket http.RoundTripper
	host   string
}

func (b both) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Host == b.host {
		return b.iss.RoundTrip(r)
	}
	return b.bucket.RoundTrip(r)
}

// A bucket opened with what makes tokens keeps working past the hour.
func TestOpenWithAKeyOutlivesAToken(t *testing.T) {
	ctx := context.Background()
	doc, _ := serviceAccount(t)
	iss := &issuer{t: t, check: func(*http.Request, url.Values) {}}
	var seen []string
	bucket := roundTrip(func(r *http.Request) (*http.Response, error) {
		seen = append(seen, r.Header.Get("Authorization"))
		return respond(r, 200, `{"items":[]}`, nil), nil
	})
	c := &http.Client{Transport: both{iss: iss, bucket: bucket, host: "oauth2.googleapis.com"}}
	env := map[string]string{"SOURCE_GOOGLE_SERVICE_ACCOUNT_KEY": doc}
	s, err := Open("gs://my-bucket", "SOURCE", func(k string) string { return env[k] }, c)
	if err != nil {
		t.Fatal(err)
	}
	g := s.(*GCS)
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	g.Tokens.now = func() time.Time { return now }
	list := func() {
		t.Helper()
		if err := s.List(ctx, "", func(Object) error { return nil }); err != nil {
			t.Fatal(err)
		}
	}
	list()
	list()
	now = now.Add(2 * time.Hour)
	list()
	if strings.Join(seen, ",") != "Bearer token-1,Bearer token-1,Bearer token-2" {
		t.Fatalf("calls went out with %v", seen)
	}

	env = map[string]string{"TARGET_AZURE_TENANT_ID": tenant, "TARGET_AZURE_CLIENT_ID": appID, "TARGET_AZURE_CLIENT_SECRET": "s"}
	if _, err := Open("azblob://acct/container", "TARGET", func(k string) string { return env[k] }, nil); err != nil {
		t.Fatal(err)
	}
	delete(env, "TARGET_AZURE_TENANT_ID")
	if _, err := Open("azblob://acct/container", "TARGET", func(k string) string { return env[k] }, nil); !errors.Is(err, ErrConfig) {
		t.Fatalf("a secret with no tenant: %v", err)
	}
	if _, err := Open("gs://my-bucket", "SOURCE", func(string) string { return "{}" }, nil); !errors.Is(err, ErrConfig) {
		t.Fatalf("a key that isn't one: %v", err)
	}
}

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
