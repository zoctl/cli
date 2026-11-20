package objects

import (
	"net/http"
	"net/url"
	"strings"
)

// Open makes a bucket from a spec and the credentials of one side. side is
// "SOURCE", "TARGET" or "STANDBY", and each credential is read as SIDE_NAME,
// so the two sides of a copy never share a key by accident. STANDBY is the
// old side's bucket opened to be written, which only a reverse sync after a
// cutover does; its keys are apart from SOURCE's so the read-only ones a
// move runs on are never the ones that write there:
//
//	s3://bucket?region=us-east-1             AWS S3: SIDE_AWS_ACCESS_KEY_ID, SIDE_AWS_SECRET_ACCESS_KEY, SIDE_AWS_SESSION_TOKEN
//	s3+https://host/bucket?region=auto       S3-compatible (R2, Hetzner, OVHcloud, MinIO): SIDE_S3_ACCESS_KEY_ID, SIDE_S3_SECRET_ACCESS_KEY
//	gs://bucket                              Google Cloud Storage: SIDE_GOOGLE_OAUTH_ACCESS_TOKEN, or SIDE_GOOGLE_SERVICE_ACCOUNT_KEY
//	azblob://account/container               Azure Blob Storage: SIDE_AZURE_STORAGE_TOKEN, or SIDE_AZURE_TENANT_ID, SIDE_AZURE_CLIENT_ID and SIDE_AZURE_CLIENT_SECRET
//
// A token from Google or Microsoft ends within the hour. The second form,
// a service account's JSON key or an application's secret, makes a new one
// as each ends, and is used when it is set; the source's asks Google for
// read-only tokens.
//
// A source is opened the same way and then wrapped in ReadOnly by its caller.
func Open(spec, side string, env func(string) string, c *http.Client) (Store, error) {
	if side != "SOURCE" && side != "TARGET" && side != "STANDBY" {
		return nil, ErrConfig
	}
	u, err := url.Parse(spec)
	if err != nil || u.User != nil || u.Fragment != "" {
		return nil, ErrConfig
	}
	get := func(name string) string { return env(side + "_" + name) }
	q := u.Query()
	switch u.Scheme {
	case "s3":
		if u.Path != "" && u.Path != "/" || len(q) != 1 {
			return nil, ErrConfig
		}
		s := &S3{Region: q.Get("region"), Bucket: u.Host, Client: c, Credentials: Credentials{
			AccessKeyID: get("AWS_ACCESS_KEY_ID"), SecretAccessKey: get("AWS_SECRET_ACCESS_KEY"), SessionToken: get("AWS_SESSION_TOKEN")}}
		return s, s.check()
	case "s3+https":
		bucket := strings.Trim(u.Path, "/")
		if strings.Contains(bucket, "/") || len(q) != 1 {
			return nil, ErrConfig
		}
		s := &S3{Endpoint: "https://" + u.Host, Region: q.Get("region"), Bucket: bucket, Client: c, Credentials: Credentials{
			AccessKeyID: get("S3_ACCESS_KEY_ID"), SecretAccessKey: get("S3_SECRET_ACCESS_KEY")}}
		return s, s.check()
	case "gs":
		if u.Path != "" && u.Path != "/" || len(q) != 0 {
			return nil, ErrConfig
		}
		g := &GCS{Bucket: u.Host, Token: get("GOOGLE_OAUTH_ACCESS_TOKEN"), Client: c}
		if key := get("GOOGLE_SERVICE_ACCOUNT_KEY"); key != "" {
			if g.Tokens, err = GoogleKey(key, map[bool]string{false: GoogleStorageRead, true: GoogleStorageWrite}[side != "SOURCE"], c); err != nil {
				return nil, err
			}
		}
		return g, g.check()
	case "azblob":
		container := strings.Trim(u.Path, "/")
		if strings.Contains(container, "/") || len(q) != 0 {
			return nil, ErrConfig
		}
		a := &AzureBlob{Account: u.Host, Container: container, Token: get("AZURE_STORAGE_TOKEN"), Client: c}
		if secret := get("AZURE_CLIENT_SECRET"); secret != "" {
			if a.Tokens, err = AzureSecret(get("AZURE_TENANT_ID"), get("AZURE_CLIENT_ID"), secret, AzureStorage, c); err != nil {
				return nil, err
			}
		}
		return a, a.check()
	}
	return nil, ErrConfig
}

// CredentialNames lists the environment names Open reads for spec's scheme
// on one side, so a caller can take them out of its environment afterwards.
func CredentialNames(spec, side string) []string {
	u, err := url.Parse(spec)
	if err != nil {
		return nil
	}
	var names []string
	switch u.Scheme {
	case "s3":
		names = []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"}
	case "s3+https":
		names = []string{"S3_ACCESS_KEY_ID", "S3_SECRET_ACCESS_KEY"}
	case "gs":
		names = []string{"GOOGLE_OAUTH_ACCESS_TOKEN", "GOOGLE_SERVICE_ACCOUNT_KEY"}
	case "azblob":
		names = []string{"AZURE_STORAGE_TOKEN", "AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET"}
	}
	for i := range names {
		names[i] = side + "_" + names[i]
	}
	return names
}
