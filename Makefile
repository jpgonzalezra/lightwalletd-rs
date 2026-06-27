.PHONY: build test lint fmt fmt-fix run verify

build:
	cargo build

test:
	cargo test

lint:
	cargo clippy --all-targets -- -D warnings

fmt:
	cargo fmt --check

fmt-fix:
	cargo fmt

run:
	cargo run --

verify: fmt lint build test
