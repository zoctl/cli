#!/bin/sh
# Installs zoctl from a GitHub release, checked against its checksums.txt:
#   curl -fsSL https://raw.githubusercontent.com/zoctl/cli/main/install.sh | sh
# ZOCTL_VERSION=v0.1.0 picks a release (default: the latest).
# ZOCTL_INSTALL_DIR picks the folder (default: ~/.local/bin).
set -eu
REPO=zoctl/cli
fail() { echo "zoctl install: $*" >&2; exit 1; }
command -v curl >/dev/null 2>&1 || fail "curl is needed"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' 0
trap 'exit 1' INT TERM

os=$(uname -s | tr '[:upper:]' '[:lower:]')
arch=$(uname -m)
case "$arch" in x86_64 | amd64) arch=amd64 ;; aarch64 | arm64) arch=arm64 ;; esac
case "$os/$arch" in
linux/amd64 | linux/arm64 | darwin/amd64 | darwin/arm64) ;;
*) fail "no release for $os/$arch; build from source with: go install github.com/$REPO/cmd/zoctl@latest" ;;
esac

tag=${ZOCTL_VERSION:-}
if [ -z "$tag" ]; then
  latest=$(curl -fsSL -o /dev/null -w '%{url_effective}' "https://github.com/$REPO/releases/latest") ||
    fail "couldn't find the latest release"
  prefix="https://github.com/$REPO/releases/tag/"
  case "$latest" in "$prefix"*) tag=${latest#"$prefix"} ;; *) fail "there's no release yet" ;; esac
fi
case "$tag" in v[0-9]*) ;; *) fail "not a release tag: $tag" ;; esac
case "$tag" in *[!a-zA-Z0-9.-]*) fail "not a release tag: $tag" ;; esac

asset="zoctl-$os-$arch"
base="https://github.com/$REPO/releases/download/$tag"
curl -fsSL -o "$tmp/zoctl" "$base/$asset" || fail "couldn't download $asset $tag"
curl -fsSL -o "$tmp/checksums.txt" "$base/checksums.txt" || fail "couldn't download checksums.txt"
expected=""
while read -r digest name extra; do
  if [ "$name" = "$asset" ]; then
    [ -z "$expected" ] && [ -z "$extra" ] || fail "checksums.txt lists $asset twice"
    [ "${#digest}" -eq 64 ] || fail "bad checksum for $asset"
    case "$digest" in *[!0-9a-f]*) fail "bad checksum for $asset" ;; esac
    expected=$digest
  fi
done <"$tmp/checksums.txt"
[ -n "$expected" ] || fail "checksums.txt doesn't list $asset"
if command -v sha256sum >/dev/null 2>&1; then
  actual=$(sha256sum "$tmp/zoctl")
elif command -v shasum >/dev/null 2>&1; then
  actual=$(shasum -a 256 "$tmp/zoctl")
else
  fail "sha256sum or shasum is needed to check the download"
fi
[ "${actual%% *}" = "$expected" ] || fail "$asset doesn't match its checksum; nothing installed"

dir=${ZOCTL_INSTALL_DIR:-$HOME/.local/bin}
mkdir -p "$dir"
install -m 0755 "$tmp/zoctl" "$dir/zoctl"
echo "installed $dir/zoctl ($tag)"
case ":$PATH:" in *:"$dir":*) ;; *) echo "note: $dir is not on your PATH" >&2 ;; esac
echo "next: zoctl login"
