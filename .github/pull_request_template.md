## What changed

<!-- Before -> after. One concern per PR. -->

Before:

After:

## How verified

<!-- Commands run, tests added, output checked. -->

## Performance impact

<!-- Measured numbers (throughput, CPU, memory, latency) with the command used, or "none". -->

## Linked issue or RFC

<!-- e.g. Fixes #123, RFC 0001 -->

## Checklist

Run in `asap-precompute-rs/`:

- [ ] `cargo fmt --check`
- [ ] `cargo clippy --all-targets --features otap-engine,stream-benchmark -- -D warnings`
- [ ] `cargo test --features otap-engine,stream-benchmark`
- [ ] This is one of at most two open PRs I have.
