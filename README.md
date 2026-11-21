# zoctl

The command line for the [zoctl console](https://app.zoctl.com). It signs a
terminal in to your account, shows your organization's moves, and runs a
move's data sync and DNS cutover on your own machine with your own keys.

## Install

Linux or macOS, amd64 or arm64:

```sh
curl -fsSL https://raw.githubusercontent.com/zoctl/cli/main/install.sh | sh
```

It checks the binary against `checksums.txt` and puts it in `~/.local/bin`.
Set `ZOCTL_VERSION` or `ZOCTL_INSTALL_DIR` on the `sh` to change either.
The Windows build is on the [releases page](https://github.com/zoctl/cli/releases).

From source:

```sh
go install github.com/zoctl/cli/cmd/zoctl@latest
```

## Use

```sh
zoctl login        # type the printed code at app.zoctl.com/cli
zoctl moves        # every move, the ones needing attention first
zoctl move ID      # one move in full; the first few characters of ID do
zoctl help         # every command and flag
```

The terminal's session only reads. It can't change the team, read a cloud
account, or plan or approve a run: those take your passkey in the console. It
lasts 30 days, or until a week unused, `zoctl logout` or a role change.

## Sync and cutover

`zoctl sync` copies a move's databases and buckets to the target and keeps
them caught up. `zoctl cutover` points DNS at the target once they have. Both
read keys from `SOURCE_*` and `TARGET_*` variables, print a plan and a digest,
and change nothing until you type the digest back. Nothing they copy goes
through zoctl.

- Postgres: logical replication. The source needs `wal_level = logical`, a
  publication and a `pgoutput` slot. Make sure the target doesn't log
  statements: `CREATE SUBSCRIPTION` holds the source's password.
- MySQL 5.7.20 or later, MariaDB 11.1 or later: `binlog_format = ROW`,
  `binlog_row_image = FULL`, and a primary key on every table.
- Redis: every key, with `DUMP` and `RESTORE`. No cluster.
- Buckets: S3 and S3-compatible, Cloud Storage, Azure Blob.

Database URLs have to use TLS: `sslmode=require` or stricter, `tls=true`,
`rediss://`.

For 30 days after the cutover, or the move's `return_days`, `cutover -back` switches DNS back and
`sync -back` copies what the target took to the old side first, with
`STANDBY_*` keys. MySQL and Redis have no `sync -back` yet.

It's tested against fakes and two local Postgres 17 servers only. MySQL and
Redis haven't met a real server.

## Development

```sh
make check
```

## License

MIT, see [LICENSE](LICENSE).
