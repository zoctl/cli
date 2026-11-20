package datasync

import (
	"errors"
	"strings"
	"testing"
)

// A refused move file says which part was refused, in fixed words. The first
// real run lost time to "command failed" over a DNS name without its dot.
func TestARefusedMoveFileSaysWhichPart(t *testing.T) {
	const good = `{"version": 1, "source": "aws", "target": "gcp",
 "datasets": [{"name": "uploads", "kind": "objects", "from": "s3://old?region=us-east-1", "to": "gs://new"}],
 "limits": {"max_objects": 10, "max_bytes": 1000, "hours": 1},
 "cutover": {"zone": "route53://Z0123456789ABC", "credentials": "DNS",
  "records": [{"name": "app.example.test.", "type": "A", "ttl": 60, "values": ["198.51.100.20"]}]}}`
	if _, err := ParseMove([]byte(good)); err != nil {
		t.Fatalf("the good file: %v", err)
	}
	for part, change := range map[string][2]string{
		"json":           {`"version": 1,`, `"version": 1, "extra": true,`},
		"version":        {`"version": 1`, `"version": 2`},
		"source, target": {`"target": "gcp"`, `"target": "aws"`},
		"limits":         {`"hours": 1`, `"hours": 0`},
		"dataset":        {`"kind": "objects"`, `"kind": "files"`},
		"cutover":        {`"credentials": "DNS"`, `"credentials": "SOURCE"`},
		"cutover zone":   {`route53://Z0123456789ABC`, `route53://`},
		"cutover record": {`app.example.test.`, `app.example.test`},
	} {
		body := strings.Replace(good, change[0], change[1], 1)
		if body == good {
			t.Fatalf("%s: the change didn't apply", part)
		}
		_, err := ParseMove([]byte(body))
		if !errors.Is(err, ErrMove) {
			t.Fatalf("%s: %v, want a refusal", part, err)
		}
		if got := MovePart(err); !strings.HasPrefix(got, part) {
			t.Fatalf("refused at %q, want %q", got, part)
		}
		// The words are fixed: nothing the file held is in them.
		if strings.Contains(MovePart(err), "example.test") || strings.Contains(err.Error(), "example") {
			t.Fatalf("%s: the refusal quotes the file", part)
		}
	}
	// A DNS name without its dot is the likeliest slip, so the part says it.
	_, err := ParseMove([]byte(strings.Replace(good, `app.example.test.`, `app.example.test`, 1)))
	if !strings.Contains(MovePart(err), "dot") {
		t.Fatalf("a name without its dot: %q", MovePart(err))
	}
	if MovePart(nil) != "" || MovePart(errors.New("other")) != "" {
		t.Fatal("a part for an error that isn't a move file's")
	}
}
