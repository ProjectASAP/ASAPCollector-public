# Contributing

This guide is for people and for AI agents. Follow it as written. If a step
does not fit your change, say so in the PR description.

The project follows OpenTelemetry practice for roles, review and design
documents.

## Roles

| Role | Who | What they do |
| --- | --- | --- |
| Contributor | Anyone | Opens issues and PRs, reviews any PR. |
| Approver | Listed in [MAINTAINERS.md](MAINTAINERS.md) | Reviews PRs. An approver's approval counts toward merge. |
| Maintainer | Listed in [MAINTAINERS.md](MAINTAINERS.md) | Merges PRs, cuts releases, accepts RFCs, sets project direction. |

To become an approver:

- Contributed for at least one month.
- Authored several merged PRs that are not trivial.
- Reviewed several PRs with useful comments.
- Sponsored by one maintainer, with no objection from the others.

To become a maintainer:

- Has been an approver for at least two months.
- Has reviewed a substantial share of PRs in that time.
- Shows good judgement on design, performance and accuracy.
- Sponsored by one maintainer and approved by a majority of maintainers.

Changes to roles are made by a PR to `MAINTAINERS.md`. People who stop
contributing move to the emeritus list.

## Before you start

- Bugs, small features and new capabilities: open an issue with the matching
  form in `.github/ISSUE_TEMPLATE/`.
- Long or cross-cutting designs: write an RFC in [docs/rfcs](docs/rfcs/README.md).
- Discuss larger changes in an issue or in the weekly sync before writing code.

## Pull requests

- At most two open PRs per contributor. Finish or close one before opening a
  third.
- Keep PRs small and reviewable. One concern per PR. Split refactors from
  behavior changes.
- Fill in the PR template:
  - What changed, written as before -> after.
  - How you verified it: commands run, tests added, output checked.
  - Performance impact: measured numbers when the change can affect
    performance, otherwise "none".
  - The linked issue or RFC.
- Keep the PR up to date with `main` and answer review comments.
- An approver or maintainer approves, and a maintainer merges.

## Design principles

1. **Performance is a feature.** Every change keeps the end-to-end benchmark
   green. A change that can affect throughput, CPU, memory or latency reports
   measured numbers.
2. **Accuracy is stated as a bound next to every answer.** An estimate without
   its error bound is not a finished feature.
3. **Sketches use asap_sketchlib.** Use asap_sketchlib's implementations and the
   ASAPv1 wire format only. Fix gaps in asap_sketchlib, not with a local copy.
4. **Upstream-compatible with OTAP, no forks.** Nodes register through the
   official OTAP Dataflow factories and depend on pinned upstream crates.

## Local checks

Run these in `asap-precompute-rs/` before you push. They must all pass.

```sh
cd asap-precompute-rs
cargo fmt --check
cargo clippy --all-targets --features otap-engine,stream-benchmark -- -D warnings
cargo test --features otap-engine,stream-benchmark
```

## Benchmarks

The benchmark is the evidence that a function is cheap enough to leave on.
Expect three tiers:

| Tier | When | Scope | Gate | State |
| --- | --- | --- | --- | --- |
| Smoke | Every merge to `main` | Short run of each scenario with correctness checks | Fails on wrong answers | Planned |
| Nightly sweep | Every day | Full rate sweep per scenario | Alerts on a throughput or CPU regression beyond a set margin | The sweep runs today in `.github/workflows/asap-sketch-benchmark.yml` (nightly, on PRs that touch the code, and on manual dispatch); the regression alert is planned |
| PR comparison | On demand, by a comment on a PR | Same sweep, this PR vs `main` | Posts a comparison table | Planned |

Until the PR comparison exists, run the relevant benchmark yourself for a
performance-related change and paste the numbers into the PR. See
`benchmarks/` and the docs in `docs/`.

## Weekly sync

<!-- TODO(maintainers): add the meeting time, time zone and link. -->
**Placeholder: time and link to be added by maintainers.**

Anyone can join. Bring RFCs for review and PRs that are stuck.

## Notes for AI agents

- Follow every rule above, including the two-open-PR limit.
- Run the local checks and paste their result into "How verified".
- Do not invent benchmark numbers. Report only numbers you measured, with the
  command that produced them.
- Do not change accepted RFCs except as described in [docs/rfcs](docs/rfcs/README.md).
