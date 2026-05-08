.PHONY: help build release check test lint fmt fmt-check bench bench-quick run docker clean ci

help:
	@echo "Targets:"
	@echo "  build        cargo build"
	@echo "  release      cargo build --release"
	@echo "  check        cargo check --all-targets"
	@echo "  test         cargo test"
	@echo "  lint         cargo clippy --all-targets -- -D warnings"
	@echo "  fmt          cargo fmt"
	@echo "  fmt-check    cargo fmt -- --check"
	@echo "  bench        cargo bench"
	@echo "  bench-quick  cargo bench -- --quick (smoke run, no statistics)"
	@echo "  run          cargo run -- --config config.yaml"
	@echo "  docker       docker compose up --build"
	@echo "  clean        cargo clean"
	@echo "  ci           fmt-check + lint + test"

build:
	cargo build

release:
	cargo build --release

check:
	cargo check --all-targets

test:
	cargo test

lint:
	cargo clippy --all-targets -- -D warnings

fmt:
	cargo fmt

fmt-check:
	cargo fmt -- --check

bench:
	cargo bench

bench-quick:
	cargo bench -- --quick

run:
	cargo run -- --config config.yaml

docker:
	docker compose up --build

clean:
	cargo clean

ci: fmt-check lint test
