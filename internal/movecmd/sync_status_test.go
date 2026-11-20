package movecmd

import (
	"bytes"
	"strings"
	"testing"

	"github.com/zoctl/cli/internal/datasync"
)

// A check that differs says why when it knows: a status line alone left
// "the two sides differ" with no word on a table the target lacks.
func TestSyncStatusPrintsItsFindings(t *testing.T) {
	var out bytes.Buffer
	printSyncStatus(&out, datasync.Status{Stage: "running", Datasets: []datasync.DatasetStatus{
		{Kind: "postgres", Pending: 1, Findings: []string{"schema_mismatch"}},
		{Kind: "objects"},
	}})
	got := out.String()
	if !strings.Contains(got, "\n  finding: schema_mismatch\n") {
		t.Fatalf("no finding in:\n%s", got)
	}
	if !strings.Contains(got, "dataset 1, postgres: tables still copying 1,") || strings.Contains(got, "dataset 1: 0 copied") {
		t.Fatalf("a database's status counted copies it can't know:\n%s", got)
	}
	if strings.Count(got, "finding:") != 1 {
		t.Fatalf("a dataset with no finding got one:\n%s", got)
	}
}
