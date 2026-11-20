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
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"
)

// An access token from Google or Microsoft lasts about an hour and a sync
// can run for a month, so a side can be given what makes tokens instead:
// a Google service account's key, or a Microsoft Entra application's
// secret. A Tokens asks for a new token before the one it holds ends.

const (
	googleTokenURL = "https://oauth2.googleapis.com/token"
	azureTokenHost = "https://login.microsoftonline.com/"
	// GoogleStorageRead and GoogleStorageWrite are the scopes a bucket is
	// read and written with, and AzureStorage the one for a container.
	GoogleStorageRead  = "https://www.googleapis.com/auth/devstorage.read_only"
	GoogleStorageWrite = "https://www.googleapis.com/auth/devstorage.read_write"
	AzureStorage       = "https://storage.azure.com/.default"
	// tokenMargin is how long before its end a token is replaced.
	tokenMargin   = 5 * time.Minute
	maxTokenBytes = 64 << 10
)

var (
	guidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}$`)
)

// headerToken reports whether a token can go in a header as it is.
func headerToken(v string) bool {
	return v != "" && len(v) <= 16<<10 && printable(v) && !strings.Contains(v, " ")
}

// Tokens hands out an access token and replaces it before it ends.
type Tokens struct {
	client *http.Client
	now    func() time.Time
	// grant builds the form a token is asked for with.
	grant func(now time.Time) (endpoint string, form url.Values, err error)

	mu    sync.Mutex
	token string
	until time.Time
}

// Token is a token good for at least tokenMargin. A refused or failed
// request is ErrStatus or ErrTransport, which a sync tries again; neither
// carries the answer.
func (t *Tokens) Token(ctx context.Context) (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	if t.now != nil {
		now = t.now()
	}
	if t.token != "" && now.Add(tokenMargin).Before(t.until) {
		return t.token, nil
	}
	endpoint, form, err := t.grant(now)
	if err != nil {
		return "", err
	}
	body := form.Encode()
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(body))
	if err != nil {
		return "", ErrConfig
	}
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := client(t.client).Do(r)
	if err != nil {
		if cerr := ctx.Err(); cerr != nil {
			return "", cerr
		}
		return "", ErrTransport
	}
	defer drain(resp)
	if resp.StatusCode != http.StatusOK {
		return "", statusError(resp.StatusCode)
	}
	var got struct {
		Token   string `json:"access_token"`
		Seconds int64  `json:"expires_in"`
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxTokenBytes+1))
	if err != nil || len(b) > maxTokenBytes || json.Unmarshal(b, &got) != nil || !headerToken(got.Token) || got.Seconds <= 0 {
		return "", ErrTransport
	}
	t.token, t.until = got.Token, now.Add(time.Duration(got.Seconds)*time.Second)
	return t.token, nil
}

func b64url(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

// GoogleKey makes tokens for one scope from a service account's JSON key.
// The key signs a request to Google's token address and nowhere else,
// whatever address the file names.
func GoogleKey(keyJSON, scope string, c *http.Client) (*Tokens, error) {
	var k struct {
		Type  string `json:"type"`
		Email string `json:"client_email"`
		Key   string `json:"private_key"`
	}
	if json.Unmarshal([]byte(keyJSON), &k) != nil || k.Type != "service_account" || k.Email == "" {
		return nil, ErrConfig
	}
	block, _ := pem.Decode([]byte(k.Key))
	if block == nil {
		return nil, ErrConfig
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, ErrConfig
	}
	key, ok := parsed.(*rsa.PrivateKey)
	if !ok {
		return nil, ErrConfig
	}
	if !strings.HasPrefix(scope, "https://www.googleapis.com/auth/") {
		return nil, ErrConfig
	}
	return &Tokens{client: c, grant: func(now time.Time) (string, url.Values, error) {
		claims, err := json.Marshal(map[string]any{"iss": k.Email, "scope": scope, "aud": googleTokenURL, "iat": now.Unix(), "exp": now.Add(time.Hour).Unix()})
		if err != nil {
			return "", nil, ErrConfig
		}
		signing := b64url([]byte(`{"alg":"RS256","typ":"JWT"}`)) + "." + b64url(claims)
		sum := sha256.Sum256([]byte(signing))
		sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, sum[:])
		if err != nil {
			return "", nil, ErrConfig
		}
		return googleTokenURL, url.Values{"grant_type": {"urn:ietf:params:oauth:grant-type:jwt-bearer"}, "assertion": {signing + "." + b64url(sig)}}, nil
	}}, nil
}

// AzureSecret makes tokens for one scope for a Microsoft Entra application,
// from its tenant, client ID and client secret.
func AzureSecret(tenant, clientID, secret, scope string, c *http.Client) (*Tokens, error) {
	if !guidPattern.MatchString(tenant) || !guidPattern.MatchString(clientID) || secret == "" || !strings.HasPrefix(scope, "https://") || !strings.HasSuffix(scope, "/.default") {
		return nil, ErrConfig
	}
	endpoint := azureTokenHost + strings.ToLower(tenant) + "/oauth2/v2.0/token"
	return &Tokens{client: c, grant: func(time.Time) (string, url.Values, error) {
		return endpoint, url.Values{"grant_type": {"client_credentials"}, "client_id": {clientID}, "client_secret": {secret}, "scope": {scope}}, nil
	}}, nil
}
