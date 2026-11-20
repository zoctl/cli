package objects

import (
	"context"
	"mime"
	"net/http"
	"regexp"
	"sort"
	"strings"
)

// Meta is what a bucket stores beside an object's bytes and hands back with
// them: the headers a browser or a CDN acts on, and the object's own
// metadata. It is copied with the object. Access lists, tags, storage
// classes, retention and encryption settings are the target bucket's own
// and aren't.
type Meta struct {
	ContentType        string
	ContentEncoding    string
	ContentLanguage    string
	ContentDisposition string
	CacheControl       string
	// User is the object's own metadata, by the name the source gave it.
	User map[string]string
	// NoMD5 is set by a read that shows the listed MD5 isn't one: Amazon
	// gives an object encrypted with a KMS key or the customer's own an
	// ETag that only looks like it.
	NoMD5 bool
}

// standard lists the five headers by their HTTP names, leaving out any
// that are empty or couldn't go in a header.
func (m Meta) standard() [][2]string {
	var out [][2]string
	for _, f := range [][2]string{
		{"Cache-Control", m.CacheControl},
		{"Content-Disposition", m.ContentDisposition},
		{"Content-Encoding", m.ContentEncoding},
		{"Content-Language", m.ContentLanguage},
		{"Content-Type", m.ContentType},
	} {
		if f[1] != "" && printable(f[1]) {
			out = append(out, f)
		}
	}
	return out
}

// names lists User's names in order.
func (m Meta) names() []string {
	names := make([]string, 0, len(m.User))
	for k := range m.User {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}

// headerMeta reads the five headers and the metadata under prefix, such as
// x-amz-meta-, from a read's answer. Go has already folded the names'
// case, which Amazon doesn't keep either.
func headerMeta(h http.Header, prefix string) Meta {
	m := Meta{
		ContentType:        h.Get("Content-Type"),
		ContentEncoding:    h.Get("Content-Encoding"),
		ContentLanguage:    h.Get("Content-Language"),
		ContentDisposition: h.Get("Content-Disposition"),
		CacheControl:       h.Get("Cache-Control"),
	}
	var dec mime.WordDecoder
	for k, vs := range h {
		name := strings.ToLower(k)
		if !strings.HasPrefix(name, prefix) || len(name) == len(prefix) {
			continue
		}
		v := strings.Join(vs, ",")
		// Amazon hands a value that isn't ASCII back as an RFC 2047 word.
		if d, err := dec.DecodeHeader(v); err == nil {
			v = d
		}
		if m.User == nil {
			m.User = map[string]string{}
		}
		m.User[name[len(prefix):]] = v
	}
	return m
}

var (
	tokenPattern = regexp.MustCompile("^[A-Za-z0-9!#$%&'*+.^_`|~-]+$")
	// Azure takes a metadata name only when it is a C# identifier.
	identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

// printable reports whether v can go in a header as it is.
func printable(v string) bool {
	for i := 0; i < len(v); i++ {
		if v[i] < 0x20 || v[i] > 0x7e {
			return false
		}
	}
	return true
}

// prober is a bucket that can say, of one object, that the MD5 its
// listing gave isn't one.
type prober interface {
	NoMD5(ctx context.Context, key string) (bool, error)
}

// noMD5 asks r, when it can be asked.
func noMD5(ctx context.Context, r Reader, key string) (bool, error) {
	if p, ok := r.(prober); ok {
		return p.NoMD5(ctx, key)
	}
	return false, nil
}
