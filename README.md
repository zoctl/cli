# zoctl

The command line for the [zoctl console](https://app.zoctl.com), where
[zoctl.com](https://zoctl.com) customers follow the moves of their
infrastructure from one cloud to another. It signs a terminal in to your
account and shows your organization's moves. Go standard library only, one
binary, no cgo.

## Install

On Linux or macOS (amd64 or arm64):

```sh
curl -fsSL https://raw.githubusercontent.com/zoctl/cli/main/install.sh | sh
```

It downloads the latest release's `zoctl-<os>-<arch>`, refuses it unless it
matches `checksums.txt`, and puts it in `~/.local/bin`. `ZOCTL_VERSION=v0.1.0`
picks a release and `ZOCTL_INSTALL_DIR` another folder, set on the `sh`:
`curl ... | ZOCTL_INSTALL_DIR=/usr/local/bin sh`.

By hand, or on Windows (`zoctl-windows-amd64.exe`): download `zoctl-<os>-<arch>` and `checksums.txt` from
the [releases page](https://github.com/zoctl/cli/releases), check one against
the other (`sha256sum -c --ignore-missing checksums.txt`, `shasum -a 256 -c`
on macOS) and put it on your `PATH`.

From source, with Go installed:

```sh
go install github.com/zoctl/cli/cmd/zoctl@latest
```

or `make install` in a clone, which puts it in `~/.local/bin`
(`PREFIX` or `BINDIR` move it).

## Signing in

```sh
zoctl login
```

prints a code like `BCDF-GHJK` and opens `https://app.zoctl.com/cli` in your
browser. Sign in to the console there as you always do, type the code, and the
terminal is signed in a few seconds later. `-no-browser` only prints the
address, for a machine with no browser: open it on any device you're signed in
on.

The terminal never sees your passkey or your provider's password. It gets a
session of its own that reads what you can read in the console and nothing
that takes your passkey: it can't change the team, read a cloud account, or
plan or approve a run. The session lasts 30 days, or until it goes a week
unused, `zoctl logout` ends it, or your role changes. Your Account page in the
console counts the terminals signed in as you and signs them all out.

The session is kept in `session.json` in your configuration folder
(`~/.config/zoctl` on Linux, `~/Library/Application Support/zoctl` on macOS),
readable only by you. zoctl never prints it.

## Commands

| Command | What it does |
| --- | --- |
| `zoctl login [-no-browser]` | Sign this terminal in with a code typed in the browser. |
| `zoctl whoami [-json]` | The organization, member and role this terminal is signed in as. |
| `zoctl moves [-json]` | Every move your runners report: stage, progress, alerts, last report. Those needing attention come first. |
| `zoctl move [-json] ID` | One move in full, with its findings. The first few characters of its ID will do. |
| `zoctl open [ID]` | Open the console, or a move's page in it, in the browser. |
| `zoctl logout` | End this terminal's session on the console and forget it here. |
| `zoctl version` | Print the version. |

`-json` prints what the console sent, for scripts:

```sh
zoctl moves -json | jq -r '.[] | select(.attention) | .id'
```

A move's report holds codes, counts and times only, never resource names,
account IDs or domains: your runner keeps those, and the plan, on your side.

Exit status is 0 on success, 1 when something failed, 2 for a command line
that couldn't be read and 130 when Ctrl-C stopped it.

## Environment

| Variable | Meaning |
| --- | --- |
| `ZOCTL_CONSOLE` | The console to use, `https://` and a host. Defaults to `https://app.zoctl.com`. A session is only ever sent to the console it came from. |
| `ZOCTL_CONFIG_DIR` | Where `session.json` is kept. |

## Development

```sh
make vet test     # go vet, go test -race
make build        # ./zoctl
```

Tests run against a fake console in memory, never the network. Releases are
built by `.github/workflows/release.yml` when a `v*` tag is pushed: Linux,
macOS and Windows binaries plus `checksums.txt`.

## License

MIT, see [LICENSE](LICENSE).
