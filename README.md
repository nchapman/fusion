# fusion

Read-only NFSv3 server that unions multiple media roots into Infuse-friendly
shares. Built for serving disk-spanning libraries to clients like Infuse over
LAN.

## Configure

Copy `config.example.yaml` to `config.yaml` and edit. Each share has:

- `merge:` roots that union into the share root (first-root-wins on file
  conflicts).
- `mount:` roots that appear as named subdirectories (mount names shadow
  same-named entries from merge roots).

```yaml
shares:
  Movies:
    merge:
      - /mnt/disk1/Movies
      - /mnt/disk2/Movies
    mount:
      Archive: /mnt/archive/Movies
```

## Run

```bash
cargo run --release -- --config config.yaml
# or
docker compose up --build
```

Port 2049 is privileged on Linux. In dev, set `server.bind: 0.0.0.0:11111` in
the config; in prod, give the binary `CAP_NET_BIND_SERVICE` or set
`sysctl net.ipv4.ip_unprivileged_port_start=2049`.

Mount from a client:

```bash
mount -t nfs -o vers=3 fusion-host:/ /mnt/fusion
```

## Develop

```bash
make test           # cargo test
make lint           # cargo clippy --all-targets -- -D warnings
make bench-quick    # smoke-run all benchmarks
make ci             # fmt-check + lint + test
```

See `CLAUDE.md` for architecture notes and load-bearing invariants.
