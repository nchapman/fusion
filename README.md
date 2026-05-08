# fusion

Read-only NFSv3 server that unions multiple media roots into Infuse-friendly
shares. Built for serving disk-spanning libraries to clients like Infuse over
LAN.

## Configure

The simplest config is one line per share:

```yaml
shares:
  Movies: /mnt/movies
  TV: /mnt/tv
```

A share value can be a single path, a list of paths (unioned), or the full
form with `merge:` (unioned roots) and `subdirs:` (named subdirectories):

```yaml
shares:
  Movies:
    merge:
      - /mnt/disk1/Movies
      - /mnt/disk2/Movies
    subdirs:
      Archive: /mnt/archive/Movies
```

Copy `config.example.yaml` (minimal) or `config.advanced.yaml` (every option
documented) and edit.

## Run

```bash
cargo run --release -- --config config.yaml
# or
docker compose up --build
```

The default bind is `0.0.0.0:11111` (non-privileged). To expose port 2049 on
Linux, give the binary `CAP_NET_BIND_SERVICE` or set
`sysctl net.ipv4.ip_unprivileged_port_start=2049`, then set
`server.bind: 0.0.0.0:2049` in the config.

Mount from a Linux client:

```bash
mount -t nfs -o vers=3,port=11111,mountport=11111 fusion-host:/ /mnt/fusion
```

Mount from macOS Finder: **Cmd-K → `nfs://fusion-host:11111/Movies`** (or use
`mount_nfs -o vers=3,port=11111,mountport=11111 fusion-host:/Movies /mnt/m`).

## Develop

```bash
make test           # cargo test
make lint           # cargo clippy --all-targets -- -D warnings
make bench-quick    # smoke-run all benchmarks
make ci             # fmt-check + lint + test
```

See `CLAUDE.md` for architecture notes and load-bearing invariants.
