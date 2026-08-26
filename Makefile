.PHONY: build test lint fmt fmt-fix prose run verify

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

prose:
	python3 scripts/prose-lint.py --self-test
	python3 scripts/prose-lint.py

run:
	cargo run --

verify: fmt prose lint build test
