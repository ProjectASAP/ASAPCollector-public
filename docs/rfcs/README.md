# RFCs

An RFC (request for comments) is a design document kept in this repository.
It records what we decided and why, so later contributors can find it.

## RFC or issue?

Write an RFC when the design is long or cross-cutting. Examples:

- It changes more than one component, such as a processor, the sketch wire
  format and the benchmark harness together.
- It changes a public interface: a node URN, a config shape, or the sketch
  carrier.
- It sets a plan or principle that many later PRs will follow.

Use a GitHub issue for everything else: a bug, a small feature, or one new
capability that fits the capability issue form. If an issue discussion grows
into a design, move it to an RFC and link the two.

## Numbering and files

- Name the file `NNNN-short-title.md`, with the next free four-digit number.
  The first RFC is `0001`.
- Put images in `images/`, named with the RFC number as a prefix, for example
  `images/0001-architecture.png`.
- Start the RFC with a header table: Status, Date, Owner.
- Numbers are never reused, even if an RFC is withdrawn.

## Statuses

| Status | Meaning |
| --- | --- |
| Draft | Open for review. The PR that adds it is not merged yet, or it is merged for discussion only. |
| Accepted | Maintainers approved it. Code should follow it. |
| Superseded | A later RFC replaces it. Add "Superseded by RFC NNNN" to the header and keep the file. |

After an RFC is accepted, do not rewrite it. If a fact changes, add a short
line marked "Update after approval" next to the original text, or write a new
RFC that supersedes it.

## Review

1. Open a PR that adds the RFC with Status: Draft.
2. Announce it in the weekly sync or in an issue.
3. Approvers and maintainers review it in the PR.
4. A maintainer sets Status: Accepted and merges it, or closes the PR with a
   reason.

## Index

| RFC | Title | Status |
| --- | --- | --- |
| [0001](0001-sketch-telemetry-functions.md) | Sketch-based telemetry functions in the OTAP collector | Accepted |
