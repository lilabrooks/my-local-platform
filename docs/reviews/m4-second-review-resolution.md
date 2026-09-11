# M4 second review resolution

Status: Implemented in the uncommitted working tree on 2026-09-11; awaiting
Claude rereview and clean-candidate rehearsal. No AWS execution authorized.

Base HEAD: `88e9a103d123feddef19720af1730253c8630327` on `main`. The source
includes untracked files; `git diff HEAD` alone is insufficient for review.

The owner approved shared-account cost totals with an explicit attribution
limit, a separate append-only recovery packet, and standalone #96 staging
publication. [ADR 0010](../adr/0010-live-aws-relay-contract.md#evidence-and-redaction)
records these choices. The original Claude review remains the user-supplied
attachment; this table records the implementation response.

| Finding | Change and regression evidence |
|---|---|
| F1 | AWS setup waits for Deployments, Tempo, and Services before rollout/forwarding; baseline retries missing or stale initial samples. A fake full setup returns missing objects first, then objects, and an empty scrape before the baseline. |
| F2 | Provisional/final publication and verification require matching clean AWS capture and controller receipts, all export hashes, successful evidence statuses, and non-overdue cleanup. Missing/failed/local captures, altered exports, unfinished controllers, and evidence errors are rejected. |
| F3 | The executor scope cancels queued requests and requests stop before waiting for active workers. Both sampling failure and interruption tests hold 8 workers while cancelling the remaining 592 requests. |
| F4 | `--before-session` rechecks original observation ages with a 15-minute reserve. The 23h55m-old identity fixture passes ordinary validation but fails before-session validation. |
| F5 | Deployment ends by destroy deadline minus 20 minutes; proof gets a separate bounded window. Installer bounds cover their child waits. Fake-clock tests check the reserve and expired session. |
| F6 | Failed publication snapshots nullable statuses, including explicit identity refusal and lost controller. Recovery is a later linked packet; fresh scoped identity/backend/state/inventory checks determine cleanup independently. Original failed history survives later controller changes. |
| F7 | Final-cost collection/validation checks session dates, intended account, all pages, daily service amounts, collection at least 48 hours after cleanup, and non-estimated results. Placeholder, early, partial, and estimated packets fail; a UTC month-boundary fixture passes. |
| F8 | Secret scanning decodes Kafka base64 key/value/raw-value fields before bounded nested-JSON scanning. An embedded JSON canary is rejected. |
| F9 | The Go fake dispatches the Python action correctly. A failing pre-session check leaves no session, controller, lock, apply, or destroy. Python tests reject changed IP, non-/32 CIDR, and unavailable address lookup. |
| F10 | Stop requests isolate their process group and defer further signals; acceptance is reported explicitly. Read-only verification and duplicate claims cannot request stop. |
| F11 | Fresh broker group membership must rise above 1, then return to 1 with zero lag and one desired replica. All queried series have scrape-age checks; stale member samples fail. |
| F12 | Runbook distinguishes renderer from deploying capture; plan has explicit hourly flags and actual queries. Secret consumers, ELBv2 scope, full-SHA build/load commands, and credential recovery are documented. |

## Verification and limits

`make test` passed all 7 race-enabled Go modules and 169 Python tests.
`make terraform-check` validated 3 stacks and passed 8 mocked contracts.
`make k8s-validate` reported 104 valid resources, 59 intentional skips, and
zero invalid resources or errors. `make aws-preflight-check` passed.
Strict `make lint` passed all 12 checks. The final account/profile snapshot
binding passed the 28 focused publication tests; changing mutable raw identity
files cannot retarget recovery. Repository audit: zero errors and the expected
dirty-worktree warning.

A targeted Codex subagent source review found no remaining material defect in
the final proof-result checks, recovery backend/scope/timestamp checks, or
standalone staging publication. The reviewer did not rerun tests or review the
whole implementation; this is not a Claude approval.

The earlier local run `20260911T005220Z` predates these fixes and used a dirty
working tree. No new load sample was taken, and no current AWS identity,
inventory, prices, authorization, billing, or teardown evidence was collected.

Next: Claude rereview, then merge and rebuild the selected candidate, collect
all clean-candidate local receipts, and obtain the separate #96 staging
approval. #97 still needs its own paid-run approval after staging.

## Follow-up findings

Claude's follow-up independently reproduced 11 fixed findings and left F11
partial because its freshness check introduced N1. The owner authorized fixes
for N1/N2 and documentation for N3. The earlier test counts above describe the
preceding revision.

- N1: relay metrics retain a 30-second age limit; the replica series permits
  60 seconds. Cached chart 88.5.4 values document a 30-second default, and an
  offline AWS render confirmed no kube-state-metrics or Prometheus interval
  override. Monitoring configuration is unchanged. Pending observations retry
  within the original load/drain deadlines and cannot release the sink delay
  or satisfy a terminal condition. Hard errors still cancel the load.
- N2: historical GO receipt validation checks the preserved input hashes and
  recorded plan/summary hash binding, independently of the shared binary.
  Tests publish with that binary present, missing, or replaced, while the
  execution validator continues to reject missing/replaced plans. Changed
  inputs and inconsistent or malformed recorded plan hashes are rejected.
- N3: the recovery runbook explicitly states that failed/recovery publication
  dirties the executing checkout. Preserve the evidence and use another clean
  candidate checkout for a later attempt; no new publication flag was added.

Focused regression tests exercise the real proof loop with a deterministic
600-event producer: one pending mid-load sample followed by fresh samples
completes without cancellation; persistent staleness stops at 480 seconds;
a hard error still stops immediately. Drain tests retry one pending sample
and fail persistent staleness at 120 seconds. The earlier real-thread
cancellation regression remains in place. No new cluster rehearsal ran.

Verification on 2026-09-11: `make test` passed all 7 Go race modules and 174
Python tests before the final wait-boundary correction. After that correction,
Python discovery passed 176 tests and the focused capture/staging/publication
suites passed 50. The new tests reject probes finishing exactly at or after
the wait deadline and cancellation arriving during a successful probe.
`make aws-preflight-check` passed; the repository audit found no errors and the
expected dirty-tree warning. A targeted Codex source review found no remaining
actionable N1/N2 issue after the deadline correction; it did not rerun tests.
Strict `make lint` passed all 12 checks; Ruff was rerun on the final Python
edits. Clean-candidate rehearsal and the separate AWS approvals remain required.
