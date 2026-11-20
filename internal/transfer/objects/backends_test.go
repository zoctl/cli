package objects

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func respond(r *http.Request, code int, body string, hdr map[string]string) *http.Response {
	h := http.Header{}
	for k, v := range hdr {
		h.Set(k, v)
	}
	return &http.Response{StatusCode: code, Header: h, Body: io.NopCloser(strings.NewReader(body)), Request: r, ContentLength: int64(len(body))}
}

func TestSigV4Vector(t *testing.T) {
	// AWS's "GET Object" example from the S3 SigV4 documentation.
	r, _ := http.NewRequest(http.MethodGet, "https://examplebucket.s3.amazonaws.com/test.txt", nil)
	r.Header.Set("Range", "bytes=0-9")
	c := Credentials{AccessKeyID: "AKIAIOSFODNN7EXAMPLE", SecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"}
	signV4(r, c, "us-east-1", emptyPayload, time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC))
	want := "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
	if got := r.Header.Get("Authorization"); got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

func TestURIEncode(t *testing.T) {
	if got := uriEncode("/a b/ü+=~", true); got != "/a%20b/%C3%BC%2B%3D~" {
		t.Fatal(got)
	}
	if got := uriEncode("a/b", false); got != "a%2Fb" {
		t.Fatal(got)
	}
}

// fakeS3 is one S3 bucket behind a RoundTripper.
type fakeS3 struct {
	t        *testing.T
	host     string
	prefix   string // the path before the key: "/bucket/" path-style, "/" otherwise
	mu       sync.Mutex
	objects  map[string][]byte
	parts    map[int][]byte
	pageSize int
	failPart int
	aborted  int
	calls    int
	gets     int
	heads    int
	// kms makes the bucket one encrypted with a KMS key: every ETag looks
	// like an MD5 and isn't, and only a read or a HEAD says so.
	kms bool
	// stored is what each object was written with, and starting what a
	// multipart upload was started with.
	stored   map[string]http.Header
	starting http.Header
}

// kept picks out of a write's headers what S3 stores with the object.
func kept(h http.Header) http.Header {
	out := http.Header{}
	for k, v := range h {
		switch name := strings.ToLower(k); {
		case strings.HasPrefix(name, "x-amz-meta-"), name == "content-type", name == "content-encoding", name == "content-language", name == "content-disposition", name == "cache-control":
			out[k] = v
		}
	}
	return out
}

// answer is the headers S3 gives back with an object.
func (f *fakeS3) answer(key string) map[string]string {
	out := map[string]string{}
	for k, v := range f.stored[key] {
		out[k] = v[0]
	}
	if f.kms {
		out["x-amz-server-side-encryption"] = "aws:kms"
	}
	return out
}

func newFakeS3(t *testing.T, host, prefix string) *fakeS3 {
	return &fakeS3{t: t, host: host, prefix: prefix, objects: map[string][]byte{}, parts: map[int][]byte{}, pageSize: 1000, stored: map[string]http.Header{}}
}

func (f *fakeS3) RoundTrip(r *http.Request) (*http.Response, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if r.URL.Host != f.host || r.URL.Scheme != "https" {
		f.t.Errorf("call to %s://%s", r.URL.Scheme, r.URL.Host)
	}
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "AWS4-HMAC-SHA256 Credential=AKID/") || !strings.Contains(auth, "host;") || r.Header.Get("x-amz-content-sha256") == "" {
		f.t.Errorf("unsigned call: %q", auth)
	}
	if strings.Contains(r.URL.RawQuery, "Signature") || strings.Contains(r.URL.RawQuery, "SECRET") {
		f.t.Error("credential in the URL")
	}
	for k, v := range r.Header {
		name := strings.ToLower(k)
		if strings.HasPrefix(name, "x-amz-") && !strings.Contains(auth, ";"+name) {
			f.t.Errorf("%s sent unsigned", name)
		}
		for i := 0; i < len(v[0]); i++ {
			if v[0][i] < 0x20 || v[0][i] > 0x7e {
				f.t.Errorf("%s isn't a header value", name)
			}
		}
	}
	var body []byte
	if r.Body != nil {
		body, _ = io.ReadAll(r.Body)
		r.Body.Close()
	}
	key := strings.TrimPrefix(r.URL.Path, f.prefix)
	q := r.URL.Query()
	switch {
	case r.Method == http.MethodGet && q.Get("list-type") == "2":
		var keys []string
		for k := range f.objects {
			if strings.HasPrefix(k, q.Get("prefix")) && k > q.Get("continuation-token") {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		var b strings.Builder
		b.WriteString("<ListBucketResult>")
		truncated := len(keys) > f.pageSize
		if truncated {
			keys = keys[:f.pageSize]
		}
		for _, k := range keys {
			etag := fmt.Sprintf("%x", md5.Sum(f.objects[k]))
			if f.kms {
				etag = fmt.Sprintf("%x", md5.Sum(append([]byte("under a key:"), f.objects[k]...)))
			}
			if len(f.objects[k]) > 10 {
				etag += "-2"
			}
			fmt.Fprintf(&b, "<Contents><Key>%s</Key><Size>%d</Size><ETag>&quot;%s&quot;</ETag><LastModified>2026-11-20T12:00:00.000Z</LastModified></Contents>", k, len(f.objects[k]), etag)
		}
		fmt.Fprintf(&b, "<IsTruncated>%v</IsTruncated>", truncated)
		if truncated {
			fmt.Fprintf(&b, "<NextContinuationToken>%s</NextContinuationToken>", keys[len(keys)-1])
		}
		b.WriteString("</ListBucketResult>")
		return respond(r, 200, b.String(), nil), nil
	case r.Method == http.MethodGet:
		b, ok := f.objects[key]
		if !ok {
			return respond(r, 404, "<Error><Code>NoSuchKey</Code></Error>", nil), nil
		}
		if r.Header.Get("Accept-Encoding") != "identity" {
			f.t.Error("a GET may be unzipped on the way")
		}
		f.gets++
		return respond(r, 200, string(b), f.answer(key)), nil
	case r.Method == http.MethodHead:
		if _, ok := f.objects[key]; !ok {
			return respond(r, 404, "", nil), nil
		}
		f.heads++
		return respond(r, 200, "", f.answer(key)), nil
	case r.Method == http.MethodPut && q.Has("partNumber"):
		n, _ := strconv.Atoi(q.Get("partNumber"))
		if n == f.failPart {
			return respond(r, 500, "", nil), nil
		}
		if int64(len(body)) != r.ContentLength {
			f.t.Errorf("part length %d, header %d", len(body), r.ContentLength)
		}
		f.parts[n] = body
		return respond(r, 200, "", map[string]string{"ETag": fmt.Sprintf(`"%x"`, md5.Sum(body))}), nil
	case r.Method == http.MethodPut:
		if h := r.Header.Get("Content-MD5"); h != "" {
			s := md5.Sum(body)
			if base64.StdEncoding.EncodeToString(s[:]) != h {
				return respond(r, 400, "<Error><Code>BadDigest</Code></Error>", nil), nil
			}
		}
		f.objects[key] = body
		f.stored[key] = kept(r.Header)
		return respond(r, 200, "", nil), nil
	case r.Method == http.MethodPost && q.Has("uploads"):
		f.parts = map[int][]byte{}
		f.starting = kept(r.Header)
		return respond(r, 200, "<InitiateMultipartUploadResult><UploadId>u-1</UploadId></InitiateMultipartUploadResult>", nil), nil
	case r.Method == http.MethodPost && q.Get("uploadId") == "u-1":
		var doc struct {
			Parts []completePart `xml:"Part"`
		}
		if xml.Unmarshal(body, &doc) != nil {
			return respond(r, 400, "", nil), nil
		}
		var all []byte
		for i, p := range doc.Parts {
			if p.Number != i+1 || p.ETag != fmt.Sprintf(`"%x"`, md5.Sum(f.parts[p.Number])) {
				return respond(r, 200, "<Error><Code>InvalidPart</Code></Error>", nil), nil
			}
			all = append(all, f.parts[p.Number]...)
		}
		f.objects[key] = all
		f.stored[key] = f.starting
		return respond(r, 200, "<CompleteMultipartUploadResult></CompleteMultipartUploadResult>", nil), nil
	case r.Method == http.MethodDelete && q.Get("uploadId") == "u-1":
		f.aborted++
		f.parts = map[int][]byte{}
		return respond(r, 204, "", nil), nil
	}
	f.t.Errorf("unexpected %s %s", r.Method, r.URL)
	return respond(r, 400, "", nil), nil
}

func s3For(f *fakeS3, endpoint string) *S3 {
	return &S3{Endpoint: endpoint, Region: "auto", Bucket: "bucket", Credentials: Credentials{AccessKeyID: "AKID", SecretAccessKey: "SECRET"}, Client: &http.Client{Transport: f}}
}

func TestS3CopyAndVerify(t *testing.T) {
	ctx := context.Background()
	f := newFakeS3(t, "r2.example.test", "/bucket/")
	f.pageSize = 2
	dst := s3For(f, "https://r2.example.test")
	src := newMemory(map[string]string{"a b/1": "one", "a b/ü": "two", "c/3": "three", "": ""})
	delete(src.objects, "")
	p, err := MakePlan(ctx, ReadOnly(src), dst, "pair", "")
	if err != nil || len(p.Copy) != 3 {
		t.Fatalf("plan %+v %v", p, err)
	}
	prog, err := Copy(ctx, ReadOnly(src), dst, p.Copy, budget(10, 100))
	if err != nil || prog.Copied != 3 {
		t.Fatalf("copy %+v %v", prog, err)
	}
	if string(f.objects["a b/ü"]) != "two" {
		t.Fatal("key not stored as itself")
	}
	v, err := Verify(ctx, ReadOnly(src), dst, "")
	if err != nil || !v.OK() || v.Target != 3 {
		t.Fatalf("verify %+v %v", v, err)
	}
	rc, _, err := dst.Get(ctx, "c/3")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(rc)
	rc.Close()
	if string(b) != "three" {
		t.Fatal(string(b))
	}
}

func TestS3AWSHost(t *testing.T) {
	f := newFakeS3(t, "bucket.s3.us-east-1.amazonaws.com", "/")
	s := s3For(f, "")
	s.Region = "us-east-1"
	if err := s.Put(context.Background(), "k", 1, "", Meta{}, strings.NewReader("x")); err != nil {
		t.Fatal(err)
	}
	dotted := newFakeS3(t, "s3.us-east-1.amazonaws.com", "/my.bucket/")
	s = s3For(dotted, "")
	s.Region, s.Bucket = "us-east-1", "my.bucket"
	if err := s.Put(context.Background(), "k", 1, "", Meta{}, strings.NewReader("x")); err != nil {
		t.Fatal(err)
	}
}

func TestS3BadDigestIsAFailedObject(t *testing.T) {
	ctx := context.Background()
	f := newFakeS3(t, "r2.example.test", "/bucket/")
	dst := s3For(f, "https://r2.example.test")
	src := newMemory(map[string]string{"k": "before"})
	p, _ := MakePlan(ctx, ReadOnly(src), dst, "pair", "")
	src.onGet = func(string) { src.objects["k"] = []byte("BEFORE") }
	prog, err := Copy(ctx, ReadOnly(src), dst, p.Copy, budget(10, 100))
	if err != nil || prog.Failed != 1 || len(f.objects) != 0 {
		t.Fatalf("copy %+v %v", prog, err)
	}
}

func TestS3Multipart(t *testing.T) {
	ctx := context.Background()
	f := newFakeS3(t, "r2.example.test", "/bucket/")
	dst := s3For(f, "https://r2.example.test")
	dst.SingleLimit, dst.PartSize = 10, 4
	src := newMemory(map[string]string{"big": "0123456789abcdefghij-"})
	p, _ := MakePlan(ctx, ReadOnly(src), dst, "pair", "")
	prog, err := Copy(ctx, ReadOnly(src), dst, p.Copy, budget(10, 100))
	if err != nil || prog.Copied != 1 || string(f.objects["big"]) != "0123456789abcdefghij-" || f.aborted != 0 {
		t.Fatalf("copy %+v %v %q", prog, err, f.objects["big"])
	}
	// The listing gives a multipart ETag, which is no MD5, so the check
	// reads the object: a source written again at the same size differs.
	v, _ := Verify(ctx, ReadOnly(src), dst, "")
	if !v.OK() {
		t.Fatalf("verify %+v", v)
	}
	src.objects["big"] = []byte("0123456789abcdefghij+")
	if v, _ = Verify(ctx, ReadOnly(src), dst, ""); v.OK() || v.Different != 1 {
		t.Fatalf("a same-size change behind a multipart ETag: %+v", v)
	}
	src.objects["big"] = []byte("0123456789abcdefghij-")

	g := newFakeS3(t, "r2.example.test", "/bucket/")
	g.failPart = 3
	dst = s3For(g, "https://r2.example.test")
	dst.SingleLimit, dst.PartSize = 10, 4
	// One failed object is counted and doesn't end the copy.
	prog, err = Copy(ctx, ReadOnly(src), dst, p.Copy, budget(10, 100))
	if err != nil || prog.Failed != 1 || prog.Copied != 0 || g.aborted != 1 || len(g.objects) != 0 {
		t.Fatalf("failed part: %+v %v aborted %d", prog, err, g.aborted)
	}
	g.failPart = 3
	err = dst.Put(ctx, "big", 21, "", Meta{}, strings.NewReader("0123456789abcdefghij-"))
	if err == nil || g.aborted != 2 {
		t.Fatalf("failed part: %v aborted %d", err, g.aborted)
	}
	if strings.Contains(err.Error(), "big") || strings.Contains(err.Error(), "r2.example") {
		t.Fatalf("error names the object: %v", err)
	}

	h := newFakeS3(t, "r2.example.test", "/bucket/")
	dst = s3For(h, "https://r2.example.test")
	dst.SingleLimit, dst.PartSize = 10, 4
	src.onGet = func(string) { src.objects["big"] = []byte("0123456789ABCDEFGHIJ-") }
	prog, err = Copy(ctx, ReadOnly(src), dst, p.Copy, budget(10, 100))
	if err != nil || prog.Failed != 1 || h.aborted != 1 || len(h.objects) != 0 {
		t.Fatalf("changed multipart %+v %v aborted %d", prog, err, h.aborted)
	}
}

func TestS3Config(t *testing.T) {
	f := newFakeS3(t, "x", "/")
	for _, s := range []*S3{
		{Region: "us-east-1", Bucket: "Bad_Bucket", Credentials: Credentials{"a", "b", ""}},
		{Region: "us-east-1", Bucket: "bucket", Endpoint: "http://plain.example", Credentials: Credentials{"a", "b", ""}},
		{Region: "us-east-1", Bucket: "bucket", Endpoint: "https://u:p@x.example", Credentials: Credentials{"a", "b", ""}},
		{Region: "us-east-1", Bucket: "bucket"},
	} {
		s.Client = &http.Client{Transport: f}
		if err := s.List(context.Background(), "", func(Object) error { return nil }); !errors.Is(err, ErrConfig) {
			t.Errorf("%+v: %v", s, err)
		}
	}
	if f.calls != 0 {
		t.Fatal("a refused configuration still called out")
	}
}

// fakeGCS is one Cloud Storage bucket behind a RoundTripper.
type fakeGCS struct {
	t       *testing.T
	objects map[string][]byte
	lie     bool
	// stored is the object resource each object was uploaded with.
	stored map[string]map[string]any
}

func (f *fakeGCS) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Header.Get("Authorization") != "Bearer TOKEN" {
		f.t.Error("no token")
	}
	meta := func(k string) string {
		s := md5.Sum(f.objects[k])
		if f.lie {
			s[0] ^= 1
		}
		res := map[string]any{}
		for name, v := range f.stored[k] {
			res[name] = v
		}
		res["name"], res["size"], res["generation"] = k, strconv.Itoa(len(f.objects[k])), "1763640000000000"
		res["md5Hash"] = base64.StdEncoding.EncodeToString(s[:])
		b, _ := json.Marshal(res)
		return string(b)
	}
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/storage/v1/b/bucket/o":
		var keys []string
		for k := range f.objects {
			if strings.HasPrefix(k, r.URL.Query().Get("prefix")) && k > r.URL.Query().Get("pageToken") {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		next := ""
		if len(keys) > 1 {
			keys, next = keys[:1], keys[0]
		}
		var items []string
		for _, k := range keys {
			items = append(items, meta(k))
		}
		return respond(r, 200, `{"items":[`+strings.Join(items, ",")+`],"nextPageToken":"`+next+`"}`, nil), nil
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/storage/v1/b/bucket/o/") && r.URL.Query().Get("alt") == "media":
		if r.Header.Get("Accept-Encoding") != "gzip" {
			f.t.Error("Google would unzip a gzip object on the way out")
		}
		return respond(r, 200, string(f.objects[strings.TrimPrefix(r.URL.Path, "/storage/v1/b/bucket/o/")]), nil), nil
	case r.Method == http.MethodPost && r.URL.Path == "/upload/storage/v1/b/bucket/o" && r.URL.Query().Get("uploadType") == "multipart":
		all, _ := io.ReadAll(r.Body)
		if int64(len(all)) != r.ContentLength {
			f.t.Errorf("upload of %d bytes says %d", len(all), r.ContentLength)
		}
		kind, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || kind != "multipart/related" {
			return respond(r, 400, "", nil), nil
		}
		parts := multipart.NewReader(bytes.NewReader(all), params["boundary"])
		first, err := parts.NextPart()
		if err != nil {
			return respond(r, 400, "", nil), nil
		}
		res := map[string]any{}
		if json.NewDecoder(first).Decode(&res) != nil {
			return respond(r, 400, "", nil), nil
		}
		second, err := parts.NextPart()
		if err != nil {
			return respond(r, 400, "", nil), nil
		}
		b, err := io.ReadAll(second)
		if _, end := parts.NextPart(); err != nil || end != io.EOF {
			return respond(r, 400, "", nil), nil
		}
		k, _ := res["name"].(string)
		f.objects[k] = b
		if f.stored == nil {
			f.stored = map[string]map[string]any{}
		}
		f.stored[k] = res
		return respond(r, 200, meta(k), nil), nil
	}
	f.t.Errorf("unexpected %s %s", r.Method, r.URL)
	return respond(r, 400, "", nil), nil
}

func TestGCS(t *testing.T) {
	ctx := context.Background()
	f := &fakeGCS{t: t, objects: map[string][]byte{}}
	dst := &GCS{Bucket: "bucket", Token: "TOKEN", Base: "https://gcs.example.test", Client: &http.Client{Transport: f}}
	src := newMemory(map[string]string{"a/1": "one", "a/2 two": "two", "a/3": "three"})
	r, err := Round(ctx, ReadOnly(src), dst, "pair", "a/", budget(10, 100))
	if err != nil || r.Copied != 3 {
		t.Fatalf("round %+v %v", r, err)
	}
	v, err := Verify(ctx, ReadOnly(src), dst, "a/")
	if err != nil || !v.OK() || v.Target != 3 {
		t.Fatalf("verify %+v %v", v, err)
	}
	rc, _, err := dst.Get(ctx, "a/2 two")
	if err != nil {
		t.Fatal(err)
	}
	if b, _ := io.ReadAll(rc); string(b) != "two" {
		t.Fatal(string(b))
	}
	f.lie = true
	prog, err := Copy(ctx, ReadOnly(src), dst, []Object{{Key: "a/1", Size: 3, MD5: sum([]byte("one"))}}, budget(10, 100))
	if err != nil || prog.Failed != 1 {
		t.Fatalf("stored MD5 differs: %+v %v", prog, err)
	}
}

// fakeAzure is one Azure container behind a RoundTripper.
type fakeAzure struct {
	t       *testing.T
	objects map[string][]byte
	md5s    map[string]string
	blocks  map[string][]byte
	other   bool
	// stored is the headers each blob was committed with, names as sent.
	stored map[string]http.Header
}

// properties is a blob's stored headers and metadata as a listing gives them.
func (f *fakeAzure) properties(k string) (props, meta string) {
	for name, v := range f.stored[k] {
		lower := strings.ToLower(name)
		switch {
		case strings.HasPrefix(lower, "x-ms-meta-"):
			meta += "<" + name[len("x-ms-meta-"):] + ">" + v[0] + "</" + name[len("x-ms-meta-"):] + ">"
		case lower == "x-ms-blob-content-md5" || lower == "x-ms-blob-type":
		case strings.HasPrefix(lower, "x-ms-blob-"):
			el := http.CanonicalHeaderKey(lower[len("x-ms-blob-"):])
			props += "<" + el + ">" + v[0] + "</" + el + ">"
		}
	}
	if meta != "" {
		meta = "<Metadata>" + meta + "</Metadata>"
	}
	return props, meta
}

func (f *fakeAzure) keep(k string, h http.Header) {
	if f.stored == nil {
		f.stored = map[string]http.Header{}
	}
	f.stored[k] = h.Clone()
}

func (f *fakeAzure) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Header.Get("Authorization") != "Bearer TOKEN" || r.Header.Get("x-ms-version") == "" {
		f.t.Error("no token or version")
	}
	q := r.URL.Query()
	key := strings.TrimPrefix(r.URL.Path, "/container/")
	var body []byte
	if r.Body != nil {
		body, _ = io.ReadAll(r.Body)
	}
	switch {
	case r.Method == http.MethodGet && q.Get("comp") == "list":
		var b strings.Builder
		b.WriteString("<EnumerationResults><Blobs>")
		var keys []string
		for k := range f.objects {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		if q.Get("include") != "metadata" {
			f.t.Error("a listing that leaves the metadata out")
		}
		for _, k := range keys {
			props, meta := f.properties(k)
			fmt.Fprintf(&b, "<Blob><Name>%s</Name><Properties><Content-Length>%d</Content-Length><Content-MD5>%s</Content-MD5><BlobType>BlockBlob</BlobType><Etag>0x8DE00000000%04X</Etag>%s</Properties>%s</Blob>", k, len(f.objects[k]), f.md5s[k], len(f.objects[k]), props, meta)
		}
		if f.other {
			b.WriteString("<Blob><Name>disk</Name><Properties><Content-Length>512</Content-Length><BlobType>PageBlob</BlobType></Properties></Blob>")
		}
		b.WriteString("</Blobs><NextMarker/></EnumerationResults>")
		return respond(r, 200, b.String(), nil), nil
	case r.Method == http.MethodGet:
		return respond(r, 200, string(f.objects[key]), nil), nil
	case r.Method == http.MethodPut && q.Get("comp") == "block":
		f.blocks[q.Get("blockid")] = body
		return respond(r, 201, "", nil), nil
	case r.Method == http.MethodPut && q.Get("comp") == "blocklist":
		var doc struct {
			Latest []string `xml:"Latest"`
		}
		_ = xml.Unmarshal(body, &doc)
		var all []byte
		for _, id := range doc.Latest {
			all = append(all, f.blocks[id]...)
		}
		f.objects[key] = all
		f.md5s[key] = r.Header.Get("x-ms-blob-content-md5")
		f.keep(key, r.Header)
		return respond(r, 201, "", nil), nil
	case r.Method == http.MethodPut && r.Header.Get("x-ms-blob-type") == "BlockBlob":
		if h := r.Header.Get("Content-MD5"); h != "" {
			s := md5.Sum(body)
			if base64.StdEncoding.EncodeToString(s[:]) != h {
				return respond(r, 400, "", map[string]string{"x-ms-error-code": "Md5Mismatch"}), nil
			}
		}
		f.objects[key] = body
		f.md5s[key] = r.Header.Get("Content-MD5")
		f.keep(key, r.Header)
		return respond(r, 201, "", nil), nil
	}
	f.t.Errorf("unexpected %s %s", r.Method, r.URL)
	return respond(r, 400, "", nil), nil
}

func TestAzureBlob(t *testing.T) {
	ctx := context.Background()
	f := &fakeAzure{t: t, objects: map[string][]byte{}, md5s: map[string]string{}, blocks: map[string][]byte{}}
	dst := &AzureBlob{Account: "acct", Container: "container", Token: "TOKEN", Base: "https://blob.example.test", Client: &http.Client{Transport: f}, SingleLimit: 8, BlockSize: 3}
	src := newMemory(map[string]string{"small": "tiny", "large": "this goes up in blocks"})
	r, err := Round(ctx, ReadOnly(src), dst, "pair", "", budget(10, 100))
	if err != nil || r.Copied != 2 {
		t.Fatalf("round %+v %v", r, err)
	}
	if string(f.objects["large"]) != "this goes up in blocks" || len(f.blocks) != 8 {
		t.Fatalf("blocks: %q %d", f.objects["large"], len(f.blocks))
	}
	v, err := Verify(ctx, ReadOnly(src), dst, "")
	if err != nil || !v.OK() {
		t.Fatalf("verify %+v %v", v, err)
	}
	// A changed source is never committed: not as one call, not as blocks.
	src.onGet = func(k string) { src.objects[k] = []byte(strings.ToUpper(string(src.objects[k]))) }
	for _, k := range []string{"small", "large"} {
		before := string(f.objects[k])
		o := Object{Key: k, Size: int64(len(before)), MD5: sum([]byte(before))}
		src.objects[k] = []byte(before)
		prog, err := Copy(ctx, ReadOnly(src), dst, []Object{o}, budget(10, 100))
		if err != nil || prog.Failed != 1 || string(f.objects[k]) != before {
			t.Fatalf("%s: %+v %v", k, prog, err)
		}
	}
	f.other = true
	if err := dst.List(ctx, "", func(Object) error { return nil }); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("page blob listed: %v", err)
	}
}

// Every backend's listing gives a tag that names one writing of an object,
// which is what lets a pair found the same go unread until it changes.
func TestListingsGiveATag(t *testing.T) {
	ctx := context.Background()
	s3 := newFakeS3(t, "r2.example.test", "/bucket/")
	s3.objects["k"] = []byte("v")
	gcs := &fakeGCS{t: t, objects: map[string][]byte{"k": []byte("v")}}
	az := &fakeAzure{t: t, objects: map[string][]byte{"k": []byte("v")}, md5s: map[string]string{}, blocks: map[string][]byte{}}
	for name, r := range map[string]Reader{
		"s3":    s3For(s3, "https://r2.example.test"),
		"gcs":   &GCS{Bucket: "bucket", Token: "TOKEN", Base: "https://gcs.example.test", Client: &http.Client{Transport: gcs}},
		"azure": &AzureBlob{Account: "acct", Container: "container", Token: "TOKEN", Base: "https://blob.example.test", Client: &http.Client{Transport: az}},
	} {
		n := 0
		err := r.List(ctx, "", func(o Object) error {
			n++
			if o.Tag == "" {
				t.Errorf("%s lists no tag", name)
			}
			return nil
		})
		if err != nil || n != 1 {
			t.Errorf("%s: %d objects, %v", name, n, err)
		}
	}
	if tag("", "") != "" || tag("a", "") == tag("", "a") {
		t.Fatal("tag")
	}
}

// page is what a web page in a bucket is stored with.
var page = Meta{
	ContentType:     "text/html; charset=utf-8",
	ContentEncoding: "gzip",
	ContentLanguage: "de",
	CacheControl:    "public, max-age=300",
	User:            map[string]string{"owner": "web", "city": "Zürich"},
}

// An object arrives with the headers and metadata it left with, in one
// call and in parts.
func TestS3KeepsWhatIsStoredWithAnObject(t *testing.T) {
	ctx := context.Background()
	from := newFakeS3(t, "src.example.test", "/bucket/")
	src := s3For(from, "https://src.example.test")
	for k, v := range map[string]string{"index.html": "short", "big.html": "0123456789abcdefghij-"} {
		if err := src.Put(ctx, k, int64(len(v)), "", page, strings.NewReader(v)); err != nil {
			t.Fatal(err)
		}
	}
	to := newFakeS3(t, "dst.example.test", "/bucket/")
	dst := s3For(to, "https://dst.example.test")
	dst.SingleLimit, dst.PartSize = 10, 8
	r, err := Round(ctx, ReadOnly(src), dst, "pair", "", budget(10, 100))
	if err != nil || r.Copied != 2 || r.Failed != 0 {
		t.Fatalf("round %+v %v", r, err)
	}
	for _, k := range []string{"index.html", "big.html"} {
		h := to.stored[k]
		if h.Get("Content-Type") != page.ContentType || h.Get("Content-Encoding") != "gzip" || h.Get("Content-Language") != "de" || h.Get("Cache-Control") != page.CacheControl || h.Get("x-amz-meta-owner") != "web" {
			t.Errorf("%s stored with %v", k, h)
		}
		body, got, err := dst.Get(ctx, k)
		if err != nil {
			t.Fatal(err)
		}
		body.Close()
		if got.User["city"] != "Zürich" || got.User["owner"] != "web" || len(got.User) != 2 || got.NoMD5 {
			t.Errorf("%s read back with %+v", k, got)
		}
	}
	// A name that can't be a header and a value that would break one are
	// left out, and the object still goes.
	odd := Meta{User: map[string]string{"bad name": "x", "split": "a\r\nb", "fine": "y"}}
	if err := dst.Put(ctx, "odd", 1, "", odd, strings.NewReader("x")); err != nil {
		t.Fatal(err)
	}
	if h := to.stored["odd"]; len(h) != 1 || h.Get("x-amz-meta-fine") != "y" {
		t.Fatalf("odd metadata stored as %v", h)
	}
}

// A bucket encrypted with a KMS key lists ETags that look like MD5s and
// aren't. Its objects copy, and compare by what they hold.
func TestS3UnderAKMSKey(t *testing.T) {
	ctx := context.Background()
	objs := map[string]string{"a": "one", "b": "two", "c": "three"}
	from := newFakeS3(t, "src.example.test", "/bucket/")
	from.kms = true
	for k, v := range objs {
		from.objects[k] = []byte(v)
	}
	src := s3For(from, "https://src.example.test")
	to := newFakeS3(t, "dst.example.test", "/bucket/")
	dst := s3For(to, "https://dst.example.test")
	pair := NewPair(ReadOnly(src), dst, "pair", "")
	p, err := pair.Plan(ctx)
	if err != nil || len(p.Copy) != 3 {
		t.Fatalf("plan %+v %v", p, err)
	}
	prog, err := pair.Copy(ctx, p.Copy, budget(10, 100))
	if err != nil || prog.Copied != 3 || prog.Failed != 0 {
		t.Fatalf("copy from a KMS bucket: %+v %v", prog, err)
	}
	// The copy read each object once, and the round after it needn't read
	// them again to know they arrived.
	v, err := pair.Verify(ctx)
	if err != nil || !v.OK() || from.gets != 3 || to.gets != 0 {
		t.Fatalf("verify %+v %v after %d source and %d target reads", v, err, from.gets, to.gets)
	}
	heads := from.heads
	if v, err = pair.Verify(ctx); err != nil || !v.OK() || from.heads != heads || from.gets != 3 {
		t.Fatalf("a pair found the same was asked about again: %+v %v", v, err)
	}
	// Another process knows nothing, and reads to compare.
	if v, err = Verify(ctx, ReadOnly(src), dst, ""); err != nil || !v.OK() || from.gets != 6 {
		t.Fatalf("verify from scratch %+v %v after %d reads", v, err, from.gets)
	}
	from.objects["b"] = []byte("TWO")
	if v, err = Verify(ctx, ReadOnly(src), dst, ""); err != nil || v.Different != 1 {
		t.Fatalf("a same-size change under a KMS key: %+v %v", v, err)
	}
	if r, err := pair.Round(ctx, budget(10, 100)); err != nil || r.Copied != 1 || string(to.objects["b"]) != "TWO" {
		t.Fatalf("round %+v %v", r, err)
	}

	// The target can be the encrypted one.
	plain := newMemory(objs)
	enc := newFakeS3(t, "kms.example.test", "/bucket/")
	enc.kms = true
	dst = s3For(enc, "https://kms.example.test")
	if r, err := Round(ctx, ReadOnly(plain), dst, "pair", "", budget(10, 100)); err != nil || r.Copied != 3 {
		t.Fatalf("round %+v %v", r, err)
	}
	if v, err = Verify(ctx, ReadOnly(plain), dst, ""); err != nil || !v.OK() {
		t.Fatalf("a KMS target never matches: %+v %v", v, err)
	}
	if r, err := Round(ctx, ReadOnly(plain), dst, "pair", "", budget(10, 100)); err != nil || r.Planned != 0 {
		t.Fatalf("a KMS target is copied again every round: %+v %v", r, err)
	}
	plain.objects["a"] = []byte("ONE")
	if v, err = Verify(ctx, ReadOnly(plain), dst, ""); err != nil || v.Different != 1 {
		t.Fatalf("a change against a KMS target: %+v %v", v, err)
	}
}

func TestGCSKeepsWhatIsStoredWithAnObject(t *testing.T) {
	ctx := context.Background()
	f := &fakeGCS{t: t, objects: map[string][]byte{}}
	g := &GCS{Bucket: "bucket", Token: "TOKEN", Base: "https://gcs.example.test", Client: &http.Client{Transport: f}}
	src := newMemory(map[string]string{"index.html": "--zoctl-\r\nnot a boundary"})
	mixed := page
	mixed.User = map[string]string{"Owner": "web", "city": "Zürich"}
	src.metas["index.html"] = mixed
	if r, err := Round(ctx, ReadOnly(src), g, "pair", "", budget(10, 100)); err != nil || r.Copied != 1 {
		t.Fatalf("round %+v %v", r, err)
	}
	if string(f.objects["index.html"]) != "--zoctl-\r\nnot a boundary" {
		t.Fatalf("stored %q", f.objects["index.html"])
	}
	got := f.stored["index.html"]
	user, _ := got["metadata"].(map[string]any)
	if got["contentType"] != page.ContentType || got["contentEncoding"] != "gzip" || got["contentLanguage"] != "de" || got["cacheControl"] != page.CacheControl || user["Owner"] != "web" || user["city"] != "Zürich" {
		t.Fatalf("stored with %v", got)
	}
	// And out again: the listing says what the object is stored with.
	out := newMemory(nil)
	if r, err := Round(ctx, ReadOnly(g), out, "pair", "", budget(10, 100)); err != nil || r.Copied != 1 {
		t.Fatalf("round %+v %v", r, err)
	}
	if m := out.metas["index.html"]; m.ContentType != page.ContentType || m.ContentEncoding != "gzip" || m.CacheControl != page.CacheControl || m.User["Owner"] != "web" || m.User["city"] != "Zürich" {
		t.Fatalf("read back with %+v", m)
	}
	// An object with no type is stored as bytes.
	if err := g.Put(ctx, "raw", 1, "", Meta{}, strings.NewReader("x")); err != nil || f.stored["raw"]["contentType"] != "application/octet-stream" {
		t.Fatalf("%v %v", err, f.stored["raw"])
	}
}

func TestAzureKeepsWhatIsStoredWithABlob(t *testing.T) {
	ctx := context.Background()
	f := &fakeAzure{t: t, objects: map[string][]byte{}, md5s: map[string]string{}, blocks: map[string][]byte{}}
	a := &AzureBlob{Account: "acct", Container: "container", Token: "TOKEN", Base: "https://blob.example.test", Client: &http.Client{Transport: f}, SingleLimit: 8, BlockSize: 3}
	src := newMemory(map[string]string{"small": "tiny", "large": "this goes up in blocks"})
	mixed := page
	mixed.User = map[string]string{"Owner": "web", "city": "Zürich", "not-a-name": "x"}
	src.metas["small"], src.metas["large"] = mixed, mixed
	if r, err := Round(ctx, ReadOnly(src), a, "pair", "", budget(10, 100)); err != nil || r.Copied != 2 {
		t.Fatalf("round %+v %v", r, err)
	}
	for _, k := range []string{"small", "large"} {
		h := f.stored[k]
		if h.Get("x-ms-blob-content-type") != page.ContentType || h.Get("x-ms-blob-content-encoding") != "gzip" || h.Get("x-ms-blob-cache-control") != page.CacheControl {
			t.Errorf("%s stored with %v", k, h)
		}
		// Azure keeps a name's case, takes only identifiers and only ASCII.
		if v := h["x-ms-meta-Owner"]; len(v) != 1 || v[0] != "web" {
			t.Errorf("%s: the name's case wasn't kept: %v", k, h)
		}
		for name := range h {
			if lower := strings.ToLower(name); lower == "x-ms-meta-city" || lower == "x-ms-meta-not-a-name" {
				t.Errorf("%s: %s was sent", k, name)
			}
		}
	}
	out := newMemory(nil)
	if r, err := Round(ctx, ReadOnly(a), out, "pair", "", budget(10, 100)); err != nil || r.Copied != 2 {
		t.Fatalf("round %+v %v", r, err)
	}
	for _, k := range []string{"small", "large"} {
		if m := out.metas[k]; m.ContentType != page.ContentType || m.ContentLanguage != "de" || m.CacheControl != page.CacheControl || m.User["Owner"] != "web" || len(m.User) != 1 {
			t.Errorf("%s read back with %+v", k, m)
		}
	}
}
