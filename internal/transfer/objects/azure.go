package objects

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const (
	azureVersion      = "2023-11-03"
	azureSingleLimit  = 256 << 20
	azureBlockSize    = 64 << 20
	azureMaxBlocks    = 50000
	azureMaxBlockSize = 4000 << 20
)

var (
	azureAccountPattern   = regexp.MustCompile(`^[a-z0-9]{3,24}$`)
	azureContainerPattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9]|-[a-z0-9]){2,62}$`)
)

// AzureBlob is an Azure Storage container, with a Microsoft Entra access
// token for https://storage.azure.com/, or with Tokens, which makes one
// as each ends. Base is for tests; empty means
// https://<account>.blob.core.windows.net.
type AzureBlob struct {
	Account   string
	Container string
	Token     string
	Tokens    *Tokens
	Base      string
	Client    *http.Client
	Now       func() time.Time
	// SingleLimit and BlockSize default to 256 MiB and 64 MiB.
	SingleLimit int64
	BlockSize   int64
}

// Name is the name Azure holds key under: Blob Storage takes each
// backslash in a blob's name as a slash (seen 20 November 2026, a key with
// backslashes copied from S3 listed with slashes).
func (a *AzureBlob) Name(key string) string { return strings.ReplaceAll(key, `\`, "/") }

func (a *AzureBlob) base() string {
	if a.Base != "" {
		return a.Base
	}
	return "https://" + a.Account + ".blob.core.windows.net"
}

func (a *AzureBlob) check() error {
	if !azureAccountPattern.MatchString(a.Account) || !azureContainerPattern.MatchString(a.Container) || a.Token == "" && a.Tokens == nil || !validEndpoint(a.base()) {
		return ErrConfig
	}
	return nil
}

func (a *AzureBlob) do(ctx context.Context, method, blob string, q url.Values, body io.Reader, size int64, hdr map[string]string) (*http.Response, error) {
	if err := a.check(); err != nil {
		return nil, err
	}
	u := a.base() + "/" + a.Container
	if blob != "" {
		u += "/" + uriEncode(blob, true)
	}
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	r, err := http.NewRequestWithContext(ctx, method, u, nil)
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
		if strings.HasPrefix(k, azureMetaPrefix) {
			// Azure keeps a metadata name's case, which Set would fold.
			r.Header[k] = []string{v}
			continue
		}
		r.Header.Set(k, v)
	}
	now := time.Now
	if a.Now != nil {
		now = a.Now
	}
	r.Header.Set("x-ms-version", azureVersion)
	r.Header.Set("x-ms-date", now().UTC().Format(http.TimeFormat))
	token, err := bearer(ctx, a.Token, a.Tokens)
	if err != nil {
		return nil, err
	}
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("Accept-Encoding", "identity")
	resp, err := client(a.Client).Do(r)
	if err != nil {
		if cerr := ctx.Err(); cerr != nil {
			return nil, cerr
		}
		return nil, ErrTransport
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		drain(resp)
		if resp.Header.Get("x-ms-error-code") == "Md5Mismatch" {
			return nil, errChanged
		}
		return nil, statusError(resp.StatusCode)
	}
	return resp, nil
}

type azureList struct {
	Blobs struct {
		Blob []struct {
			Name       string `xml:"Name"`
			Properties struct {
				Length      int64  `xml:"Content-Length"`
				MD5         string `xml:"Content-MD5"`
				Type        string `xml:"BlobType"`
				ETag        string `xml:"Etag"`
				ContentType string `xml:"Content-Type"`
				Encoding    string `xml:"Content-Encoding"`
				Language    string `xml:"Content-Language"`
				Disposition string `xml:"Content-Disposition"`
				Cache       string `xml:"Cache-Control"`
			} `xml:"Properties"`
			Metadata azureMetadata `xml:"Metadata"`
		} `xml:"Blob"`
	} `xml:"Blobs"`
	Next string `xml:"NextMarker"`
}

const azureMetaPrefix = "x-ms-meta-"

// azureMetadata is a blob's metadata as a listing gives it: one element
// for each name, in the name's own case.
type azureMetadata map[string]string

func (m *azureMetadata) UnmarshalXML(d *xml.Decoder, _ xml.StartElement) error {
	out := azureMetadata{}
	for {
		tok, err := d.Token()
		if err != nil {
			return err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			var v string
			if err := d.DecodeElement(&v, &t); err != nil {
				return err
			}
			out[t.Name.Local] = v
		case xml.EndElement:
			if len(out) > 0 {
				*m = out
			}
			return nil
		}
	}
}

// azureHeaders is meta as Azure takes it on the call that stores a blob.
// Azure takes a metadata name only when it is a C# identifier and a value
// only in ASCII, so anything else is left out.
func azureHeaders(meta Meta) map[string]string {
	hdr := map[string]string{}
	for _, f := range meta.standard() {
		hdr["x-ms-blob-"+strings.ToLower(f[0])] = f[1]
	}
	for _, name := range meta.names() {
		if v := meta.User[name]; identifierPattern.MatchString(name) && printable(v) {
			hdr[azureMetaPrefix+name] = v
		}
	}
	return hdr
}

// List lists block blobs under prefix. Page and append blobs, such as
// disks, aren't object storage a move copies this way, so a listing that
// holds one fails rather than leaving it out silently.
func (a *AzureBlob) List(ctx context.Context, prefix string, fn func(Object) error) error {
	marker := ""
	for page := 0; page < maxPages; page++ {
		q := url.Values{"restype": {"container"}, "comp": {"list"}, "include": {"metadata"}}
		if prefix != "" {
			q.Set("prefix", prefix)
		}
		if marker != "" {
			q.Set("marker", marker)
		}
		resp, err := a.do(ctx, http.MethodGet, "", q, nil, 0, nil)
		if err != nil {
			return err
		}
		var l azureList
		if err := readXML(resp, &l); err != nil {
			return err
		}
		for _, b := range l.Blobs.Blob {
			if b.Properties.Length < 0 || b.Properties.Type != "" && b.Properties.Type != "BlockBlob" {
				return ErrUnsupported
			}
			p := b.Properties
			m := &Meta{ContentType: p.ContentType, ContentEncoding: p.Encoding, ContentLanguage: p.Language, ContentDisposition: p.Disposition, CacheControl: p.Cache, User: b.Metadata}
			if err := fn(Object{Key: b.Name, Size: p.Length, MD5: b64MD5(p.MD5), Tag: tag(p.ETag), Meta: m}); err != nil {
				return err
			}
		}
		if l.Next == "" {
			return nil
		}
		if l.Next == marker {
			return ErrTransport
		}
		marker = l.Next
	}
	return ErrTooMany
}

// ErrUnsupported means the container holds something this copy can't move.
var ErrUnsupported = fmt.Errorf("%w: not a block blob", ErrConfig)

// Get reads one blob. What is stored with it comes from the listing, which
// keeps a metadata name's case where a header wouldn't.
func (a *AzureBlob) Get(ctx context.Context, key string) (io.ReadCloser, Meta, error) {
	if key == "" {
		return nil, Meta{}, ErrConfig
	}
	resp, err := a.do(ctx, http.MethodGet, key, nil, nil, 0, nil)
	if err != nil {
		return nil, Meta{}, err
	}
	return resp.Body, Meta{}, nil
}

// Put writes one block blob: one call up to SingleLimit, blocks above it.
// A known MD5 is checked by Azure on a single call and stored as the
// blob's Content-MD5 either way, so the next listing can compare it.
func (a *AzureBlob) Put(ctx context.Context, key string, size int64, md5hex string, meta Meta, body io.Reader) error {
	if key == "" || size < 0 {
		return ErrConfig
	}
	md5b64, known := md5Header(md5hex)
	limit := a.SingleLimit
	if limit <= 0 {
		limit = azureSingleLimit
	}
	if size <= limit {
		hdr := azureHeaders(meta)
		hdr["x-ms-blob-type"] = "BlockBlob"
		if known {
			hdr["Content-MD5"] = md5b64
		}
		resp, err := a.do(ctx, http.MethodPut, key, nil, body, size, hdr)
		if err != nil {
			return err
		}
		drain(resp)
		return nil
	}
	block := a.BlockSize
	if block <= 0 {
		block = azureBlockSize
	}
	if (size+block-1)/block > azureMaxBlocks {
		block = (size + azureMaxBlocks - 1) / azureMaxBlocks
	}
	if block > azureMaxBlockSize {
		return ErrConfig
	}
	var ids []string
	hb := newHashed(body)
	for n, left := 0, size; left > 0; n++ {
		length := min(block, left)
		id := base64.StdEncoding.EncodeToString(fmt.Appendf(nil, "zoctl-%08d", n))
		resp, err := a.do(ctx, http.MethodPut, key, url.Values{"comp": {"block"}, "blockid": {id}}, io.LimitReader(hb, length), length, nil)
		if err != nil {
			// Uncommitted blocks are dropped by Azure after a week.
			return err
		}
		drain(resp)
		ids = append(ids, id)
		left -= length
	}
	if n, _ := io.ReadFull(hb, make([]byte, 1)); n != 0 {
		return errOver
	}
	// The stored Content-MD5 below is only a label to Azure, so it must be
	// true before the blocks are committed.
	if !hb.matches(md5hex) {
		return errChanged
	}
	doc, err := xml.Marshal(struct {
		XMLName xml.Name `xml:"BlockList"`
		Latest  []string `xml:"Latest"`
	}{Latest: ids})
	if err != nil {
		return ErrTransport
	}
	hdr := azureHeaders(meta)
	hdr["Content-Type"] = "application/xml"
	if known {
		hdr["x-ms-blob-content-md5"] = md5b64
	}
	resp, err := a.do(ctx, http.MethodPut, key, url.Values{"comp": {"blocklist"}}, bytes.NewReader(doc), int64(len(doc)), hdr)
	if err != nil {
		return err
	}
	drain(resp)
	return nil
}
