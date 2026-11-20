# zoctl

The command line for the [zoctl console](https://app.zoctl.com), where
[zoctl.com](https://zoctl.com) customers follow the moves of their
infrastructure from one cloud to another. It signs a terminal in to your
account and shows your organization's moves, and it copies a move's data and
switches its DNS where you run it, with your own keys. One binary, no cgo.

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
| `zoctl sync -move FILE -workspace DIR` | Copy a move's databases and buckets to the target and keep them caught up, once you approve the plan it prints. `-check` only compares the two sides. |
| `zoctl cutover -move FILE -workspace DIR` | Point the move's DNS at the target once the data has caught up, or back again with `-back`. |
| `zoctl runner pair -console URL -contact URL -workspace DIR` | Let a workspace report its counts to the console (`-report` on `sync` and `cutover`). |

`-json` prints what the console sent, for scripts:

```sh
zoctl moves -json | jq -r '.[] | select(.attention) | .id'
```

A move's report holds codes, counts and times only, never resource names,
account IDs or domains: your runner keeps those, and the plan, on your side.

Exit status is 0 on success, 1 when something failed, 2 for a command line
that couldn't be read and 130 when Ctrl-C stopped it.

## Moving data and DNS

`sync` and `cutover` don't use the console session. They run on your machine,
with both sides' keys in the environment, and nothing they copy goes through
zoctl. A move file names what to copy and the limits one approval covers,
never a key:

```json
{"version": 1, "source": "aws", "target": "gcp",
 "datasets": [
  {"name": "uploads", "kind": "objects", "from": "s3://old-bucket?region=us-east-1", "to": "gs://new-bucket"},
  {"name": "shop", "kind": "postgres", "from_env": "SOURCE_SHOP_URL", "to_env": "TARGET_SHOP_URL",
   "publication": "zoctl", "slot": "zoctl", "copy_schema": true}],
 "limits": {"max_objects": 1000000, "max_bytes": 500000000000, "hours": 72},
 "cutover": {"zone": "clouddns://new-project/shop", "credentials": "TARGET",
  "health": "https://new.shop.example.com/healthz",
  "records": [{"name": "www.shop.example.com.", "type": "A", "ttl": 60, "values": ["198.51.100.20"]}]}}
```

Each run plans first: it prints counts and a digest, and changes nothing until
you type the digest back. One approval runs once, and the workspace (a folder
only you can read) remembers it across restarts. Output and the workspace hold
counts, never a bucket, key, table or account.

Keys come from `SOURCE_` and `TARGET_` variables and are taken out of the
environment once read, so nothing the command starts inherits them. Buckets
are S3 (`s3://bucket?region=r` with `SOURCE_AWS_ACCESS_KEY_ID` and
`SOURCE_AWS_SECRET_ACCESS_KEY`, or `s3+https://host/bucket` for R2, Hetzner,
OVHcloud and MinIO with `SOURCE_S3_ACCESS_KEY_ID` and
`SOURCE_S3_SECRET_ACCESS_KEY`), Cloud Storage (`gs://bucket` with
`SOURCE_GOOGLE_SERVICE_ACCOUNT_KEY`) and Azure Blob
(`azblob://account/container` with `SOURCE_AZURE_TENANT_ID`,
`SOURCE_AZURE_CLIENT_ID` and `SOURCE_AZURE_CLIENT_SECRET`), and the same
names with `TARGET_` for the other side. The source's keys only ever read.

A Postgres database moves by Postgres's own logical replication. You set up
the source, since zoctl never changes it: `wal_level = logical`, a
publication of the tables to move and a `pgoutput` slot, named in the
dataset. Both URLs have to say `sslmode=require`, `verify-ca` or
`verify-full`; a URL that could fall back to plaintext is refused. zoctl makes
the subscription on the target and, with `copy_schema`, the missing tables.

A MySQL (5.7.20 or later) or MariaDB (11.1 or later) database (`"kind":
"mysql"`, `from_env` and `to_env` as above) has no subscription a managed
target lets anyone make, so zoctl is the replica: it copies every table
from one consistent read of the source, then reads the source's binary log
from just before that read and writes each transaction to the target in one
of its own, a row at a time by its primary key. The source needs `binlog_format = ROW`, `binlog_row_image = FULL` and a
role with `SELECT`, `REPLICATION CLIENT` and `REPLICATION SLAVE`, and every
table needs a primary key. URLs look like
`mysql://user:password@host:3306/shop?tls=true`, and `tls` has to be `true`
or `skip-verify`. With `copy_schema` zoctl makes the missing tables from the
source's `CREATE TABLE`; views, triggers, routines and users are yours. Where
the log got to is kept in the workspace, so the next approval carries on from
there while the source still has that log file. A table made or changed on
the source while it syncs stops the sync, to be planned again.

A Redis database (`"kind": "redis"`) can't take a replica from outside on a
managed service either, so every round reads every key: a key the target is
missing or holds otherwise is copied with `DUMP` and `RESTORE` with what's left
of its time to live, and a key the source no longer has is removed from the
target. The role needs `SCAN`, `DUMP` and `PTTL` on the source. URLs are
`rediss://user:password@host:6379/0` (TLS; `?skip_verify=true` doesn't check
the certificate). A cluster isn't supported, and the target can't run an older
Redis than the source. Neither MySQL nor Redis has a way back yet, so a move
with one has no `sync -back`, and `cutover -back` only checks the target took
nothing since.

`cutover` switches the records in `cutover` once every dataset has caught up
and the health address answers. Stop the source's writers before you type its
digest: it waits, checks nothing moved, sets the target's sequences, and only
then changes DNS. For 30 days, or the move's `return_days`, `zoctl cutover
-back` puts the records back, and `zoctl sync -back` copies what the target
took to the old side first, with keys from `STANDBY_` variables that a sync
and a cutover never read.

None of this has run against a real cloud's database or DNS zone yet: it's
tested against fakes, and the Postgres sync against two local Postgres 17
servers. The MySQL and Redis syncs haven't met a real server at all.

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
