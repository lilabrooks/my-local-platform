# Application validation review resolution

Date: 2026-09-20
Status: Documentation revisions complete; original review preserved.

The [Claude review](application-validation-claude-second-review.md) assessed
the first candidate and returned **REVISE**. Its file hashes and findings remain
historical evidence. The changes below address all 11 findings in
[the guide](../application-validation.md), README, and AGENTS.md; they do not
change the AWS tooling or relay's #97 completion contract.

| Finding | Resolution |
|---|---|
| F1: inventory scope | Describe regional project/name matching, final ECR emptiness, and dev-state destruction. Require scope conflicts to be resolved before coexistence; preserve tagging and budget coverage. |
| F2: shared budget | Document the shared $5 monthly project allowance and how another app can block hourly plan/apply. Changing amount or scope requires an owner decision. |
| F3: ADR authority | Identify per-app rehearsal as new guidance; attribute only the existing live-verification requirement to ADR 0001. |
| F4: cheap versus hourly tier | Give cheap-tier checks their own path, with applicable artifacts, authority, costs, and explicit cleanup or retention responsibility. |
| F5: unnamed limits | Require maximum spend, elapsed-time destroy deadline, cleanup reserve and completion target, cleanup owner, and abort procedure for hourly runs. |
| F6: failed demonstration | Preserve failure after clean teardown and settled billing; require repair and a newly authorized retry or an explicit owner deferral. Relay's existing contract still governs #97. |
| F7: duplicate rehearsal | Allow an existing automated end-to-end check to satisfy rehearsal when it covers the agreed path and has dated results. |
| F8: mandatory telemetry | Condition observability checks on the application's contract. Scope read-back assertions to paths that write data. |
| F9: broad AWS trigger | Distinguish an intended AWS target and local emulated API use from claims requiring live evidence. |
| F10: source identity | Require subsequent M4 gates to use a clean checkout pinned to the qualified source; documentation merges do not move receipt identity. |
| F11: navigation | Label the README link for both applications and shared-stack changes. |

## Following the guidance in future work

The owner requested a commit and PR and asked that the guidance be followed.
AGENTS.md now requires relevant issues to cite the guide in Governing anchors
and name the environment, checks, and observable acceptance results. The new
PR template asks for the governing scope, dated evidence, and AWS decision for
new-app or shared-stack changes; other changes can omit that section.

A fresh, read-only agent task with no inherited conversation followed
CLAUDE.md to AGENTS.md and the guide, and found the README and PR-template
routes. It correctly scoped a Compose/Postgres app, an S3 IAM check, and another
app sharing relay's AWS account against the linked contracts. It identified an
older README bullet calling the budget account-wide; that bullet was corrected
to project-tag-filtered scope with tax excluded, and the reviewer verified the
correction.

This verifies discovery and interpretation in one fresh task. The PR fields
are editable and policy compliance remains a contributor/reviewer obligation;
no CI check proves live AWS validation. This check does not replace Claude's
full review or claim that Claude re-reviewed the revised candidate.

## Verification

Before publication, run the repository Markdown lint and ADR-index checks,
GFM parsing and relative-link/anchor checks for the changed documents,
`git diff --check`, and a working-tree secret scan. Record the actual results
in the PR. No workload, cloud mutation, or qualification run is needed for
these documentation and template changes.
