package objects

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"io"
	"mime"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	defaultPartSize    = 64 << 20
	defaultSingleLimit = 5 << 30
	maxParts           = 10000
	maxPages           = 100000
	maxXMLBytes        = 16 << 20
)

var (
	bucketPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)
	regionPattern = regexp.MustCompile(`^[a-z0-9-]{1,32}$`)
)

// S3 is a bucket on Amazon S3 or on any service that speaks its API:
// Cloudflare R2, Hetzner and OVHcloud object storage, Tigris, MinIO. An
// empty Endpoint means AWS; any other endpoint is addressed path-style.
type S3 struct {
	Endpoint    string
	Region      string
	Bucket      string
	Credentials Credentials
	Client      *http.Client
	Now         func() time.Time
	// PartSize and SingleLimit default to 64 MiB and 5 GiB. An object
	// over SingleLimit goes up in parts.
	PartSize    int64
	SingleLimit int64
}

func (s *S3) check() error {
	if !bucketPattern.MatchString(s.Bucket) || !regionPattern.MatchString(s.Region) || s.Credentials.AccessKeyID == "" || s.Credentials.SecretAccessKey == "" {
		return ErrConfig
	}
	if s.Endpoint != "" && !validEndpoint(s.Endpoint) {
		return ErrConfig
	}
	return nil
}

// validEndpoint takes an https origin and nothing else.
func validEndpoint(e string) bool {
	u, err := url.Parse(e)
	return err == nil && u.Scheme == "https" && u.Host != "" && u.User == nil && (u.Path == "" || u.Path == "/") && u.RawQuery == "" && u.Fragment == "" && !u.ForceQuery
}

func (s *S3) url(key string, q url.Values) *url.URL {
	var u url.URL
	u.Scheme = "https"
	path := "/" + key
	switch {
	case s.Endpoint != "":
		e, _ := url.Parse(s.Endpoint)
		u.Host = e.Host
		path = "/" + s.Bucket + path
	case strings.Contains(s.Bucket, "."):
		// A dotted name doesn't match the wildcard certificate.
		u.Host = "s3." + s.Region + ".amazonaws.com"
		path = "/" + s.Bucket + path
	default:
		u.Host = s.Bucket + ".s3." + s.Region + ".amazonaws.com"
	}
	u.Path = path
	u.RawPath = uriEncode(path, true)
	u.RawQuery = canonicalQuery(q)
	return &u
}

func client(c *http.Client) *http.Client {
	out := http.Client{}
	if c != nil {
		out = *c
	}
	// A signed or authorized call is never sent on somewhere else.
	out.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &out
}

func (s *S3) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// do sends one signed call. body may be nil; size is its exact length.
func (s *S3) do(ctx context.Context, method, key string, q url.Values, body io.Reader, size int64, payload string, hdr map[string]string) (*http.Response, error) {
	if err := s.check(); err != nil {
		return nil, err
	}
	r, err := http.NewRequestWithContext(ctx, method, "https://placeholder/", nil)
	if err != nil {
		return nil, ErrTransport
	}
	r.URL = s.url(key, q)
	r.Host = r.URL.Host
	switch {
	case body == nil || size == 0:
		r.Body = http.NoBody
		r.ContentLength = 0
	default:
		r.Body = io.NopCloser(body)
		r.ContentLength = size
	}
	r.GetBody = nil
	for k, v := range hdr {
		r.Header.Set(k, v)
	}
	// Ask for the bytes as stored, so Go never unzips an object on the way.
	r.Header.Set("Accept-Encoding", "identity")
	signV4(r, s.Credentials, s.Region, payload, s.now())
	resp, err := client(s.Client).Do(r)
	if err != nil {
		if cerr := ctx.Err(); cerr != nil {
			return nil, cerr
		}
		return nil, ErrTransport
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		if resp.StatusCode == http.StatusBadRequest {
			// BadDigest is S3 refusing bytes that don't match Content-MD5.
			var e struct {
				Code string `xml:"Code"`
			}
			if readXML(resp, &e) == nil && e.Code == "BadDigest" {
				return nil, errChanged
			}
			return nil, statusError(resp.StatusCode)
		}
		drain(resp)
		return nil, statusError(resp.StatusCode)
	}
	return resp, nil
}

func drain(resp *http.Response) {
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	resp.Body.Close()
}

func readXML(resp *http.Response, v any) error {
	defer drain(resp)
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxXMLBytes+1))
	if err != nil || len(b) > maxXMLBytes {
		return ErrTransport
	}
	if xml.Unmarshal(b, v) != nil {
		return ErrTransport
	}
	return nil
}

// etagMD5 is the MD5 an ETag carries, when it looks like one. A multipart
// ETag (with a dash) doesn't. The ETag of an object encrypted with a KMS
// key or the customer's own does and isn't, which only a read or a HEAD
// of the object shows: see encrypted.
func etagMD5(etag string) string {
	e := strings.ToLower(strings.Trim(etag, `"`))
	if len(e) != 32 {
		return ""
	}
	if _, err := hex.DecodeString(e); err != nil {
		return ""
	}
	return e
}

type listResult struct {
	Truncated bool   `xml:"IsTruncated"`
	Next      string `xml:"NextContinuationToken"`
	Contents  []struct {
		Key      string `xml:"Key"`
		Size     int64  `xml:"Size"`
		ETag     string `xml:"ETag"`
		Modified string `xml:"LastModified"`
	} `xml:"Contents"`
}

// List lists the bucket under prefix with ListObjectsV2.
func (s *S3) List(ctx context.Context, prefix string, fn func(Object) error) error {
	token := ""
	for page := 0; page < maxPages; page++ {
		q := url.Values{"list-type": {"2"}}
		if prefix != "" {
			q.Set("prefix", prefix)
		}
		if token != "" {
			q.Set("continuation-token", token)
		}
		resp, err := s.do(ctx, http.MethodGet, "", q, nil, 0, emptyPayload, nil)
		if err != nil {
			return err
		}
		var l listResult
		if err := readXML(resp, &l); err != nil {
			return err
		}
		for _, c := range l.Contents {
			if c.Size < 0 {
				return ErrTransport
			}
			if err := fn(Object{Key: c.Key, Size: c.Size, MD5: etagMD5(c.ETag), Tag: tag(c.ETag, c.Modified)}); err != nil {
				return err
			}
		}
		if !l.Truncated {
			return nil
		}
		if l.Next == "" || l.Next == token {
			return ErrTransport
		}
		token = l.Next
	}
	return ErrTooMany
}

// encrypted reports whether an answer about an object says it is encrypted
// with anything but S3's own keys, which is when its ETag isn't an MD5.
func encrypted(h http.Header) bool {
	sse := h.Get("x-amz-server-side-encryption")
	return sse != "" && sse != "AES256" || h.Get("x-amz-server-side-encryption-customer-algorithm") != ""
}

// Get reads one object and the headers and metadata stored with it.
func (s *S3) Get(ctx context.Context, key string) (io.ReadCloser, Meta, error) {
	if key == "" {
		return nil, Meta{}, ErrConfig
	}
	resp, err := s.do(ctx, http.MethodGet, key, nil, nil, 0, emptyPayload, nil)
	if err != nil {
		return nil, Meta{}, err
	}
	m := headerMeta(resp.Header, "x-amz-meta-")
	m.NoMD5 = encrypted(resp.Header)
	return resp.Body, m, nil
}

// NoMD5 asks S3, with a HEAD, whether the object's ETag isn't its MD5.
func (s *S3) NoMD5(ctx context.Context, key string) (bool, error) {
	if key == "" {
		return false, ErrConfig
	}
	resp, err := s.do(ctx, http.MethodHead, key, nil, nil, 0, emptyPayload, nil)
	if err != nil {
		return false, err
	}
	drain(resp)
	return encrypted(resp.Header), nil
}

// metaHeaders is meta as S3 takes it on a write. A metadata name that
// can't be a header name is left out, and so is a value with a control
// character; a value that isn't ASCII goes as an RFC 2047 word, which S3
// stores decoded.
func metaHeaders(meta Meta) map[string]string {
	hdr := map[string]string{}
	for _, f := range meta.standard() {
		hdr[f[0]] = f[1]
	}
	for _, name := range meta.names() {
		v := meta.User[name]
		if !tokenPattern.MatchString(name) || strings.ContainsFunc(v, unicode.IsControl) || !utf8.ValidString(v) {
			continue
		}
		if !printable(v) {
			v = mime.BEncoding.Encode("UTF-8", v)
		}
		hdr["x-amz-meta-"+strings.ToLower(name)] = v
	}
	return hdr
}

func md5Header(md5hex string) (string, bool) {
	b, err := hex.DecodeString(md5hex)
	if err != nil || len(b) != 16 {
		return "", false
	}
	return base64.StdEncoding.EncodeToString(b), true
}

// Put writes one object, in one call up to SingleLimit and in parts above
// it. A known MD5 goes as Content-MD5, so S3 refuses bytes that don't match.
func (s *S3) Put(ctx context.Context, key string, size int64, md5hex string, meta Meta, body io.Reader) error {
	if key == "" || size < 0 {
		return ErrConfig
	}
	limit := s.SingleLimit
	if limit <= 0 {
		limit = defaultSingleLimit
	}
	if size > limit {
		return s.putParts(ctx, key, size, md5hex, meta, body)
	}
	hdr := metaHeaders(meta)
	if h, ok := md5Header(md5hex); ok {
		hdr["Content-MD5"] = h
	}
	resp, err := s.do(ctx, http.MethodPut, key, nil, body, size, unsignedPayload, hdr)
	if err != nil {
		return err
	}
	drain(resp)
	return nil
}

type completePart struct {
	Number int    `xml:"PartNumber"`
	ETag   string `xml:"ETag"`
}

func (s *S3) putParts(ctx context.Context, key string, size int64, md5hex string, meta Meta, src io.Reader) (err error) {
	body := newHashed(src)
	part := s.PartSize
	if part <= 0 {
		part = defaultPartSize
	}
	if (size+part-1)/part > maxParts {
		part = (size + maxParts - 1) / maxParts
	}
	// What is stored with the object goes on the call that starts it.
	resp, err := s.do(ctx, http.MethodPost, key, url.Values{"uploads": {""}}, nil, 0, emptyPayload, metaHeaders(meta))
	if err != nil {
		return err
	}
	var start struct {
		UploadID string `xml:"UploadId"`
	}
	if err := readXML(resp, &start); err != nil {
		return err
	}
	if start.UploadID == "" {
		return ErrTransport
	}
	upload := url.Values{"uploadId": {start.UploadID}}
	defer func() {
		if err == nil {
			return
		}
		// Abort even when the caller gave up, so no parts stay billed.
		actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		if r, aerr := s.do(actx, http.MethodDelete, key, upload, nil, 0, emptyPayload, nil); aerr == nil {
			drain(r)
		}
	}()
	var parts []completePart
	for n, left := 1, size; left > 0; n++ {
		length := min(part, left)
		q := url.Values{"partNumber": {strconv.Itoa(n)}, "uploadId": {start.UploadID}}
		resp, err := s.do(ctx, http.MethodPut, key, q, io.LimitReader(body, length), length, unsignedPayload, nil)
		if err != nil {
			return err
		}
		etag := resp.Header.Get("ETag")
		drain(resp)
		if etag == "" {
			return ErrTransport
		}
		parts = append(parts, completePart{n, etag})
		left -= length
	}
	// The body must end where the listing said it would.
	if n, _ := io.ReadFull(body, make([]byte, 1)); n != 0 {
		return errOver
	}
	// S3 checks no MD5 across parts, so check it here before completing:
	// an aborted upload leaves the target as it was.
	if !body.matches(md5hex) {
		return errChanged
	}
	doc, err := xml.Marshal(struct {
		XMLName xml.Name       `xml:"CompleteMultipartUpload"`
		Parts   []completePart `xml:"Part"`
	}{Parts: parts})
	if err != nil {
		return ErrTransport
	}
	sum := sha256.Sum256(doc)
	resp, err = s.do(ctx, http.MethodPost, key, upload, bytes.NewReader(doc), int64(len(doc)), hex.EncodeToString(sum[:]), map[string]string{"Content-Type": "application/xml"})
	if err != nil {
		return err
	}
	// S3 can answer 200 and put the failure in the body.
	var done struct {
		XMLName xml.Name
	}
	if err := readXML(resp, &done); err != nil {
		return err
	}
	if done.XMLName.Local != "CompleteMultipartUploadResult" {
		return ErrStatus
	}
	return nil
}
