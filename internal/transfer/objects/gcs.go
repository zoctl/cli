package objects

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

var gcsBucketPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]$`)

// GCS is a Google Cloud Storage bucket through its JSON API, with an OAuth
// access token, or with Tokens, which makes one as each ends. Base is for
// tests; empty means storage.googleapis.com.
type GCS struct {
	Bucket string
	Token  string
	Tokens *Tokens
	Base   string
	Client *http.Client
}

// bearer is the token a call goes out with: Tokens' when there is one.
func bearer(ctx context.Context, fixed string, t *Tokens) (string, error) {
	if t != nil {
		return t.Token(ctx)
	}
	return fixed, nil
}

func (g *GCS) base() string {
	if g.Base != "" {
		return g.Base
	}
	return "https://storage.googleapis.com"
}

func (g *GCS) check() error {
	if !gcsBucketPattern.MatchString(g.Bucket) || g.Token == "" && g.Tokens == nil || !validEndpoint(g.base()) {
		return ErrConfig
	}
	return nil
}

func (g *GCS) do(ctx context.Context, method, rawURL string, body io.Reader, size int64, hdr map[string]string) (*http.Response, error) {
	if err := g.check(); err != nil {
		return nil, err
	}
	r, err := http.NewRequestWithContext(ctx, method, rawURL, nil)
	if err != nil {
		return nil, ErrConfig
	}
	if body != nil && size > 0 {
		r.Body = io.NopCloser(body)
		r.ContentLength = size
	} else if method != http.MethodGet {
		r.Body = http.NoBody
	}
	for k, v := range hdr {
		r.Header.Set(k, v)
	}
	token, err := bearer(ctx, g.Token, g.Tokens)
	if err != nil {
		return nil, err
	}
	r.Header.Set("Authorization", "Bearer "+token)
	if r.Header.Get("Accept-Encoding") == "" {
		r.Header.Set("Accept-Encoding", "identity")
	}
	resp, err := client(g.Client).Do(r)
	if err != nil {
		if cerr := ctx.Err(); cerr != nil {
			return nil, cerr
		}
		return nil, ErrTransport
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		drain(resp)
		return nil, statusError(resp.StatusCode)
	}
	return resp, nil
}

func readJSON(resp *http.Response, v any) error {
	defer drain(resp)
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxXMLBytes+1))
	if err != nil || len(b) > maxXMLBytes || json.Unmarshal(b, v) != nil {
		return ErrTransport
	}
	return nil
}

// b64MD5 turns the base64 MD5 Google and Azure list into hex.
func b64MD5(s string) string {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil || len(b) != 16 {
		return ""
	}
	return hex.EncodeToString(b)
}

type gcsObject struct {
	Name       string `json:"name"`
	Size       string `json:"size,omitempty"`
	MD5Hash    string `json:"md5Hash,omitempty"`
	Generation string `json:"generation,omitempty"`

	ContentType        string            `json:"contentType,omitempty"`
	ContentEncoding    string            `json:"contentEncoding,omitempty"`
	ContentLanguage    string            `json:"contentLanguage,omitempty"`
	ContentDisposition string            `json:"contentDisposition,omitempty"`
	CacheControl       string            `json:"cacheControl,omitempty"`
	Metadata           map[string]string `json:"metadata,omitempty"`
}

// gcsFields is what a listing asks for of each object.
const gcsFields = "name,size,md5Hash,generation,contentType,contentEncoding,contentLanguage,contentDisposition,cacheControl,metadata"

func (o gcsObject) object() (Object, error) {
	n, err := strconv.ParseInt(o.Size, 10, 64)
	if err != nil || n < 0 {
		return Object{}, ErrTransport
	}
	m := &Meta{ContentType: o.ContentType, ContentEncoding: o.ContentEncoding, ContentLanguage: o.ContentLanguage, ContentDisposition: o.ContentDisposition, CacheControl: o.CacheControl, User: o.Metadata}
	return Object{Key: o.Name, Size: n, MD5: b64MD5(o.MD5Hash), Tag: tag(o.Generation), Meta: m}, nil
}

func (g *GCS) bucketURL() string {
	return g.base() + "/storage/v1/b/" + url.PathEscape(g.Bucket) + "/o"
}

// List lists the bucket under prefix.
func (g *GCS) List(ctx context.Context, prefix string, fn func(Object) error) error {
	token := ""
	for page := 0; page < maxPages; page++ {
		q := url.Values{"fields": {"items(" + gcsFields + "),nextPageToken"}}
		if prefix != "" {
			q.Set("prefix", prefix)
		}
		if token != "" {
			q.Set("pageToken", token)
		}
		resp, err := g.do(ctx, http.MethodGet, g.bucketURL()+"?"+q.Encode(), nil, 0, nil)
		if err != nil {
			return err
		}
		var l struct {
			Items []gcsObject `json:"items"`
			Next  string      `json:"nextPageToken"`
		}
		if err := readJSON(resp, &l); err != nil {
			return err
		}
		for _, it := range l.Items {
			o, err := it.object()
			if err != nil {
				return err
			}
			if err := fn(o); err != nil {
				return err
			}
		}
		if l.Next == "" {
			return nil
		}
		if l.Next == token {
			return ErrTransport
		}
		token = l.Next
	}
	return ErrTooMany
}

// Get reads one object's bytes as stored. Asking for gzip is how Google
// is told not to unzip an object stored with Content-Encoding: gzip on
// the way out; it never zips one that isn't. What is stored with the
// object comes from the listing, not from here.
func (g *GCS) Get(ctx context.Context, key string) (io.ReadCloser, Meta, error) {
	if key == "" {
		return nil, Meta{}, ErrConfig
	}
	resp, err := g.do(ctx, http.MethodGet, g.bucketURL()+"/"+url.PathEscape(key)+"?alt=media", nil, 0, map[string]string{"Accept-Encoding": "gzip"})
	if err != nil {
		return nil, Meta{}, err
	}
	return resp.Body, Meta{}, nil
}

// Put uploads one object in a single multipart upload, which carries what
// is stored with the object ahead of its bytes and which Cloud Storage
// takes at any size. Google answers with the MD5 it stored, which has to
// match a known one.
func (g *GCS) Put(ctx context.Context, key string, size int64, md5hex string, meta Meta, body io.Reader) error {
	if key == "" || size < 0 {
		return ErrConfig
	}
	kind := meta.ContentType
	if kind == "" || !printable(kind) {
		kind = "application/octet-stream"
	}
	doc, err := json.Marshal(gcsObject{Name: key, ContentType: kind, ContentEncoding: meta.ContentEncoding, ContentLanguage: meta.ContentLanguage, ContentDisposition: meta.ContentDisposition, CacheControl: meta.CacheControl, Metadata: meta.User})
	if err != nil {
		return ErrTransport
	}
	// The boundary is random, so the object's bytes can't hold it.
	boundary := "zoctl-" + rand.Text()
	head := "--" + boundary + "\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n" + string(doc) +
		"\r\n--" + boundary + "\r\nContent-Type: " + kind + "\r\n\r\n"
	tail := "\r\n--" + boundary + "--\r\n"
	q := url.Values{"uploadType": {"multipart"}, "fields": {"name,size,md5Hash"}}
	u := g.base() + "/upload/storage/v1/b/" + url.PathEscape(g.Bucket) + "/o?" + q.Encode()
	// The body isn't cut at size: one that runs long fails the call, and
	// nothing is stored.
	all := io.MultiReader(strings.NewReader(head), body, strings.NewReader(tail))
	resp, err := g.do(ctx, http.MethodPost, u, all, int64(len(head))+size+int64(len(tail)), map[string]string{"Content-Type": "multipart/related; boundary=" + boundary})
	if err != nil {
		return err
	}
	var got gcsObject
	if err := readJSON(resp, &got); err != nil {
		return err
	}
	o, err := got.object()
	if err != nil {
		return err
	}
	if o.Size != size || md5hex != "" && o.MD5 != md5hex {
		// Stored with its own MD5, so the next round sees it differ.
		return errChanged
	}
	return nil
}
