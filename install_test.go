// Package install tests install.sh against a fake curl and uname.
package install

import (
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

const fakeCurl = `#!/bin/sh
out=""
while [ "$#" -gt 0 ]; do
  case "$1" in -o) out=$2; shift ;; https://*) url=$1 ;; esac
  shift
done
echo "$url" >>"$FIXTURE/asked"
base=https://github.com/zoctl/cli/releases
case "$url" in
  $base/latest) if [ -f "$FIXTURE/none" ]; then printf '%s' "$base"; else printf '%s' "$base/tag/v0.1.0"; fi ;;
  $base/download/v0.1.0/zoctl-$ASSET | $base/download/v0.0.9/zoctl-$ASSET) cp "$FIXTURE/payload" "$out" ;;
  $base/download/v0.1.0/checksums.txt | $base/download/v0.0.9/checksums.txt) cp "$FIXTURE/checksums" "$out" ;;
  *) exit 22 ;;
esac
`

var payload = []byte("#!/bin/sh\necho zoctl 0.1.0\n")

func sum() string { return fmt.Sprintf("%x", sha256.Sum256(payload)) }

type run struct {
	dir, dest, out string
	err            error
}

// install runs install.sh as if on system/machine, with a release whose
// checksums.txt is checksums. none means no release exists yet.
func install(t *testing.T, system, machine, asset, checksums string, none bool, env ...string) run {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("POSIX installer")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	dest := filepath.Join(dir, "dest")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(os.Mkdir(bin, 0o700))
	must(os.WriteFile(filepath.Join(dir, "payload"), payload, 0o600))
	must(os.WriteFile(filepath.Join(dir, "checksums"), []byte(checksums), 0o600))
	if none {
		must(os.WriteFile(filepath.Join(dir, "none"), nil, 0o600))
	}
	must(os.WriteFile(filepath.Join(bin, "uname"), []byte("#!/bin/sh\ncase \"$1\" in -s) echo "+system+";; -m) echo "+machine+";; esac\n"), 0o700))
	must(os.WriteFile(filepath.Join(bin, "curl"), []byte(fakeCurl), 0o700))
	cmd := exec.Command("sh", "install.sh")
	cmd.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "FIXTURE="+dir, "ASSET="+asset, "ZOCTL_INSTALL_DIR="+dest, "ZOCTL_VERSION=")
	cmd.Env = append(cmd.Env, env...)
	b, err := cmd.CombinedOutput()
	return run{dir: dir, dest: dest, out: string(b), err: err}
}

func (r run) installed(t *testing.T) {
	t.Helper()
	if r.err != nil {
		t.Fatalf("install: %v\n%s", r.err, r.out)
	}
	got, err := os.ReadFile(filepath.Join(r.dest, "zoctl"))
	if err != nil || string(got) != string(payload) {
		t.Fatalf("installed %q, %v", got, err)
	}
	if fi, _ := os.Stat(filepath.Join(r.dest, "zoctl")); fi.Mode().Perm() != 0o755 {
		t.Fatalf("mode %v", fi.Mode().Perm())
	}
}

func (r run) refused(t *testing.T, want string) {
	t.Helper()
	if r.err == nil || !strings.Contains(r.out, want) {
		t.Fatalf("want a refusal saying %q, got %v:\n%s", want, r.err, r.out)
	}
	if _, err := os.Stat(filepath.Join(r.dest, "zoctl")); !os.IsNotExist(err) {
		t.Fatal("installed anyway")
	}
}

func TestEveryReleasedPlatform(t *testing.T) {
	for _, c := range [][3]string{
		{"Linux", "x86_64", "linux-amd64"},
		{"Linux", "aarch64", "linux-arm64"},
		{"Darwin", "arm64", "darwin-arm64"},
		{"Darwin", "x86_64", "darwin-amd64"},
	} {
		t.Run(c[2], func(t *testing.T) {
			r := install(t, c[0], c[1], c[2], sum()+"  zoctl-"+c[2]+"\n"+strings.Repeat("1", 64)+"  zoctl-other\n", false)
			r.installed(t)
			if !strings.Contains(r.out, "(v0.1.0)") {
				t.Fatalf("output:\n%s", r.out)
			}
		})
	}
}

func TestAPinnedVersion(t *testing.T) {
	r := install(t, "Linux", "x86_64", "linux-amd64", sum()+"  zoctl-linux-amd64\n", false, "ZOCTL_VERSION=v0.0.9")
	r.installed(t)
	asked, _ := os.ReadFile(filepath.Join(r.dir, "asked"))
	if strings.Contains(string(asked), "/latest") || !strings.Contains(string(asked), "/v0.0.9/") {
		t.Fatalf("asked for:\n%s", asked)
	}
	install(t, "Linux", "x86_64", "linux-amd64", "", false, "ZOCTL_VERSION=v1;rm -rf /").refused(t, "not a release tag")
	install(t, "Linux", "x86_64", "linux-amd64", "", false, "ZOCTL_VERSION=latest").refused(t, "not a release tag")
}

func TestChecksumsAreEnforced(t *testing.T) {
	for name, c := range map[string][2]string{
		"corrupt":   {strings.Repeat("0", 64) + "  zoctl-linux-arm64\n", "doesn't match its checksum"},
		"missing":   {sum() + "  zoctl-linux-amd64\n", "doesn't list zoctl-linux-arm64"},
		"duplicate": {sum() + "  zoctl-linux-arm64\n" + sum() + "  zoctl-linux-arm64\n", "twice"},
		"short":     {"abc  zoctl-linux-arm64\n", "bad checksum"},
		"capitals":  {strings.ToUpper(sum()) + "  zoctl-linux-arm64\n", "bad checksum"},
	} {
		t.Run(name, func(t *testing.T) {
			install(t, "Linux", "aarch64", "linux-arm64", c[0], false).refused(t, c[1])
		})
	}
}

func TestNoReleaseOrNoPlatform(t *testing.T) {
	install(t, "FreeBSD", "amd64", "freebsd-amd64", "", false).refused(t, "go install github.com/zoctl/cli/cmd/zoctl@latest")
	install(t, "Linux", "armv7l", "linux-armv7l", "", false).refused(t, "no release for linux/armv7l")
	// Before the first release, /releases/latest lands on /releases.
	install(t, "Linux", "x86_64", "linux-amd64", "", true).refused(t, "no release yet")
}
