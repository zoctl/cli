package objects

import (
	"errors"
	"testing"
)

func TestOpen(t *testing.T) {
	env := map[string]string{
		"SOURCE_AWS_ACCESS_KEY_ID": "a", "SOURCE_AWS_SECRET_ACCESS_KEY": "b",
		"TARGET_S3_ACCESS_KEY_ID": "c", "TARGET_S3_SECRET_ACCESS_KEY": "d",
		"TARGET_GOOGLE_OAUTH_ACCESS_TOKEN": "e", "SOURCE_AZURE_STORAGE_TOKEN": "f",
	}
	get := func(k string) string { return env[k] }
	good := []struct{ spec, side string }{
		{"s3://my-bucket?region=us-east-1", "SOURCE"},
		{"s3+https://acct.r2.cloudflarestorage.com/my-bucket?region=auto", "TARGET"},
		{"gs://my-bucket", "TARGET"},
		{"azblob://acct/container", "SOURCE"},
	}
	for _, c := range good {
		if _, err := Open(c.spec, c.side, get, nil); err != nil {
			t.Errorf("%s: %v", c.spec, err)
		}
		if len(CredentialNames(c.spec, c.side)) == 0 {
			t.Errorf("%s: no names", c.spec)
		}
	}
	bad := []struct{ spec, side string }{
		{"s3://my-bucket?region=us-east-1", "TARGET"},       // the other side's key
		{"s3://my-bucket?region=us-east-1", "OTHER"},        // not a side
		{"s3://my-bucket/path?region=us-east-1", "SOURCE"},  // a path
		{"s3://u:p@my-bucket?region=us-east-1", "SOURCE"},   // user info
		{"s3+http://host/my-bucket?region=auto", "TARGET"},  // plain HTTP
		{"s3+https://host/a/b?region=auto", "TARGET"},       // nested path
		{"gs://my-bucket?x=1", "TARGET"},                    // a query
		{"azblob://acct/container", "TARGET"},               // no token for it
		{"ftp://host/x", "SOURCE"},                          // no such scheme
		{"s3://my-bucket?region=us-east-1#frag", "SOURCE"},  // a fragment
		{"s3://my-bucket?region=us-east-1&x=1", "SOURCE"},   // an extra query
		{"s3://My_Bucket?region=us-east-1", "SOURCE"},       // not a bucket name
		{"azblob://acct/Container", "SOURCE"},               // not a container name
		{"s3+https://host/my-bucket?region=auto", "SOURCE"}, // no S3_ key on the source
	}
	for _, c := range bad {
		if _, err := Open(c.spec, c.side, get, nil); !errors.Is(err, ErrConfig) {
			t.Errorf("%s %s accepted: %v", c.spec, c.side, err)
		}
	}
}
