package objects

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

// Credentials sign S3 calls. They key the signature and are never put on a
// request themselves, apart from the session token, which S3 requires.
type Credentials struct {
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
}

const (
	unsignedPayload = "UNSIGNED-PAYLOAD"
	emptyPayload    = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

// signed lists the headers signed when a request carries them, besides
// host and every x-amz- header, which AWS requires.
var signed = []string{"cache-control", "content-disposition", "content-encoding", "content-language", "content-md5", "content-type", "range"}

// uriEncode is SigV4's encoding: unreserved characters stay, everything
// else is %XX in upper case, and a slash stays only in a path.
func uriEncode(s string, path bool) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '.' || c == '_' || c == '~' || path && c == '/' {
			b.WriteByte(c)
			continue
		}
		b.WriteString("%")
		b.WriteString(strings.ToUpper(hex.EncodeToString([]byte{c})))
	}
	return b.String()
}

// canonicalQuery sorts and encodes a query the way SigV4 signs it. The
// request is sent with the same string, so what is signed is what is sent.
func canonicalQuery(q url.Values) string {
	type kv struct{ k, v string }
	var all []kv
	for k, vs := range q {
		for _, v := range vs {
			all = append(all, kv{uriEncode(k, false), uriEncode(v, false)})
		}
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].k != all[j].k {
			return all[i].k < all[j].k
		}
		return all[i].v < all[j].v
	})
	parts := make([]string, len(all))
	for i, p := range all {
		parts[i] = p.k + "=" + p.v
	}
	return strings.Join(parts, "&")
}

func hmacSHA256(key []byte, s string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(s))
	return m.Sum(nil)
}

// SignV4 signs r for service in region with SigV4, as AWS's APIs
// outside S3 take it too. payload is the body's SHA-256 in hex.
func SignV4(r *http.Request, c Credentials, service, region, payload string, now time.Time) {
	signFor(r, c, service, region, payload, now)
}

// signV4 signs r for S3 in region. The request's path must already be in
// SigV4 encoding (RawPath) and its query in canonical order.
func signV4(r *http.Request, c Credentials, region, payload string, now time.Time) {
	signFor(r, c, "s3", region, payload, now)
}

func signFor(r *http.Request, c Credentials, service, region, payload string, now time.Time) {
	stamp := now.UTC().Format("20060102T150405Z")
	day := stamp[:8]
	r.Header.Set("x-amz-date", stamp)
	r.Header.Set("x-amz-content-sha256", payload)
	if c.SessionToken != "" {
		r.Header.Set("x-amz-security-token", c.SessionToken)
	}
	headers := [][2]string{{"host", r.URL.Host}}
	for k, vs := range r.Header {
		// x-amz-date, the payload hash, the session token, and an object's
		// x-amz-meta- names.
		if name := strings.ToLower(k); strings.HasPrefix(name, "x-amz-") && len(vs) > 0 {
			headers = append(headers, [2]string{name, strings.Join(vs, ",")})
		}
	}
	for _, h := range signed {
		if v := r.Header.Get(h); v != "" {
			headers = append(headers, [2]string{h, v})
		}
	}
	sort.Slice(headers, func(i, j int) bool { return headers[i][0] < headers[j][0] })
	var canon, names strings.Builder
	for i, h := range headers {
		// SigV4 trims a value and folds its runs of spaces into one.
		canon.WriteString(h[0] + ":" + strings.Join(strings.Fields(h[1]), " ") + "\n")
		if i > 0 {
			names.WriteString(";")
		}
		names.WriteString(h[0])
	}
	path := r.URL.EscapedPath()
	if path == "" {
		path = "/"
	}
	canonical := strings.Join([]string{r.Method, path, r.URL.RawQuery, canon.String(), names.String(), payload}, "\n")
	sum := sha256.Sum256([]byte(canonical))
	scope := day + "/" + region + "/" + service + "/aws4_request"
	toSign := "AWS4-HMAC-SHA256\n" + stamp + "\n" + scope + "\n" + hex.EncodeToString(sum[:])
	key := hmacSHA256([]byte("AWS4"+c.SecretAccessKey), day)
	key = hmacSHA256(key, region)
	key = hmacSHA256(key, service)
	key = hmacSHA256(key, "aws4_request")
	sig := hex.EncodeToString(hmacSHA256(key, toSign))
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+c.AccessKeyID+"/"+scope+", SignedHeaders="+names.String()+", Signature="+sig)
}
