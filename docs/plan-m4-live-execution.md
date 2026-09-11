# M4 staging and live validation plan

Status: Review fixes implemented on 2026-09-11. Final review, merge, and fresh
clean-candidate rehearsals still precede staging. The earlier dirty-tree local
run remains development evidence. This document does not authorize AWS mutations.

Reviewed source: `88e9a103d123feddef19720af1730253c8630327`, containing
[PR #137](https://github.com/lilabrooks/my-local-platform/pull/137).
The next executable steps belong to preparation; this SHA is not yet the
candidate to stage.

The governing contract is [ADR 0010](adr/0010-live-aws-relay-contract.md).
The [AWS runbook](runbook-aws-relay.md) and [cost guide](costs.md) carry the
operating instructions. [#96](https://github.com/lilabrooks/my-local-platform/issues/96)
owns cheap staging; [#97](https://github.com/lilabrooks/my-local-platform/issues/97)
owns the separately approved paid proof and teardown.

## Initial review disposition (2026-09-10)

The current source confirms the missing live capture script, the publisher's
requirement for plaintext secret values, incomplete staging instructions, and
the gap between receipt validation at GO generation and at apply. These make
the earlier execution plan premature.

The review's account observations, old local receipt inventory, and estimated
live durations have not been independently rechecked here. This plan requires
new evidence at execution time. It makes no claim about current AWS resources,
prices, quota capacity, credential lifetime, or final billing attribution.

Several proposed remedies need adjustment:

- `command || make aws-live-stop` can return success after requesting cleanup
  and allow later commands to run. The paid workflow must request stop and exit
  with the original failure, with every subsequent step skipped.
- Secret fingerprints are a design candidate. Their representation coverage,
  partial-failure behavior, and publisher validation need review before adoption.
- A session-date cost query grouped by service measures shared-account spend
  over that interval. It cannot establish exact M4 attribution by itself.
- Refreshing SSO is useful, but does not prove credentials last through cleanup.
- Receipt freshness must be checked at apply against the original observations;
  generating a new GO packet must not reset their ages.

## 1. Complete preparation before freezing

Implement and review the following bounded changes locally. Keep the approved
runtime topology, spend limits, and demonstration load unchanged.

1. **Make every live capture executable.** Replace the missing
   `scripts/m4-live-capture.py` references with an implemented path. Give it
   explicit cluster context, endpoint inputs, deadlines, and evidence paths.
   Own port-forward startup and cleanup, event and trace ID discovery, the fixed
   600-event/16-tenant load, attempts, trace export, metrics, KEDA transitions,
   DLQ reads, and replay correlation. Check whether the approved identities
   permit the observation operations; resolve permission gaps before staging.
   Connect each capture-plan command to a real producer and validator.

2. **Make publication survive secret destruction.** Design the bootstrap to
   publisher handoff while preserving memory-only credential handling. Review
   the proposed private digest-and-byte-length receipt, bound to run and commit.
   Specify raw, URL-encoded, JSON-escaped, and base64 representations actually
   produced by the workflow. Define safe creation before sensitive operations,
   interruption and partial-bootstrap behavior, and rejection of missing or
   conflicting receipts. Test publication after simulated secret deletion,
   including leaked canaries and placeholder input. Retain human screenshot
   review; text fingerprints do not inspect pixels. Record the chosen mechanism
   and its limits in ADR 0010 before implementation is accepted.

3. **Connect failures to cleanup.** Give deployment and capture one bounded
   execution path that stops at the first failure, requests controller cleanup,
   and returns failure. Verify it cannot continue installing or collecting once
   cleanup starts. Use the same resolved kubeconfig/context throughout render,
   bootstrap, KEDA, monitoring, ArgoCD, and capture. Test wrong context, stale
   controller, command failure, and interruption without AWS.

4. **Close the approval-input gaps.** Validate rehearsal receipts against their
   actual schemas, exact source commit, clean worktree, and passing outcomes.
   Record test-command evidence for checks that do not produce receipts.
   At the final apply boundary, verify the staged input hashes and original
   observation ages as well as the plan and summary. Compare the operator's
   current IPv4 with the planned API allowlist before starting the controller.
   Refusal here must happen before creating the spent-session receipt.

5. **Write the complete staging sequence.** Document backend inspection before
   guardrails; cheap plan/review/apply; preserved cheap-plan evidence; inventory
   containing both ECR repositories; image staging; expensive plan without
   apply; then GO. Pass hourly flags explicitly for the expensive plan and keep
   default variable files disabled. Name the producer of every runtime input:
   image references from GO and brokers from Terraform output under the chosen
   profile. Document how each re-plan invalidates the previous plan and GO.

6. **Finish the evidence lifecycle.** Provide a reviewed path for publishing a
   failed attempt's sanitized cleanup evidence without claiming a full demo
   pass. Confirm after-destroy checks reject remaining dev ECR repositories,
   while before-run checks allow the two staged repositories. Match inventory
   claims to actual service coverage. Define the final cost query using session
   UTC dates, collection time, service breakdown, and AWS estimated status.
   Check whether attribution is possible and label shared-account totals
   honestly. If exact attribution is unavailable, resolve the closure wording
   before staging. Preserve raw evidence through final publication.

Trace a representative event from the request through Kafka, subscriber,
attempt history, trace, exported file, sanitizer, and verified publication.
Keep each test cohort identifiable: the idempotent repeat must not count as a
new durable event; load, failing-subscriber, poison, and replay records affect
different delivery, retry, lag, and DLQ observations. Use bounded windows and
cohort IDs so cumulative counters cannot pass on old activity.

Verify these changes with targeted regression tests and the repository checks
they touch. Rehearse the capture path on minikube, including failure and cleanup.
AWS IAM, private networking, actual teardown timing, and Spot capacity remain
live-only outcomes. A local pass must not claim them.

## 2. Freeze and establish local evidence

After preparation is reviewed and merged, select a clean checkout at one full
commit SHA. Keep it unchanged until the paid attempt and cleanup finish.
Publish review or evidence changes from another checkout if needed meanwhile.

Rebuild the minikube images at that SHA. Run `m4-k8s-sigterm`, the integrated
local demonstration including the new capture path, `aws-live-rehearse`, and
`m4-local-abort`. Check the recorded outcomes, source SHA, and worktree state.
Perform the required visual rehearsal. Reuse valid results from this exact
candidate; do not add repeated runs to seek a passing sample.

Then choose a fresh UTC run ID and run `aws-preflight` before any other command
creates that live run directory. A failure requires local repair and fresh
candidate evidence. Any later source change invalidates the candidate.

## 3. Execute #96 after its separate approval

1. Confirm repository and AWS identity privately, refresh authentication, and
   inspect the account-scoped backend. Distinguish absence from access failure.
   Create the backend only if absent and covered by the staging approval.
2. Review and apply the persistent budget plan, then capture account, backend,
   budget, quota, availability, EKS support, and current official price evidence.
3. Create and inspect the cheap dev plan with all hourly flags false. Require
   zero hourly resources and review deletions or migrations before applying.
   Apply that exact plan and retain its summary privately.
4. Capture inventory after cheap apply. Require no hourly runtime and both ECR
   repositories. Stage and inspect the commit-tagged linux/amd64 images;
   record their digests and revision labels.
5. Plan the fixed hourly topology with all three enable flags true and the
   current operator IPv4 /32. Inspect resource actions and counts, private
   exposure, identities, EKS standard support, and the current price gate.
   The GO packet binds this plan to the image digests; Terraform itself does
   not deploy the application images.
6. Generate GO only after all inputs pass. Any rewritten input or re-plan
   requires GO regeneration and another read-back of changes before approval.

Exit: an inspected packet at the frozen commit, staged images, working budget,
and no hourly runtime. Review findings that affect GitHub labels or issue text
are separate proposed backlog updates until publication is authorized.

## 4. Obtain #97 approval and run once

Read back the redacted identity, commit, plan hash, image digests, resource
shape, EKS support evidence, current modeled price, $1.25/hour shape limit,
$5 session maximum, cleanup owner, and abort procedure. Preserve the 150-minute
destroy deadline and 180-minute hard deadline. Budget alerts are delayed;
these limits cannot guarantee an exact final bill or bound provider teardown.

State that even a refused controller apply can consume the run ID and trigger
full dev destroy, including #96's staged ECR images. A later attempt needs
fresh evidence and explicit authority for whatever staging and paid work it
requires. Previous approvals do not silently authorize another attempt.

Immediately before the controller starts, refresh SSO, check credential
availability and recovery, confirm the planned IPv4 and cluster configuration,
and validate unchanged, fresh inputs. If a check fails, refresh the affected
inputs and repeat the review before starting a session.

Start `aws-live-run` in the foreground on the powered, connected Mac. After
successful apply, execute the prepared failure-stopping workflow:

1. Resolve Terraform outputs and GO image references; select and verify EKS.
2. Render the AWS bundle and run `aws-runtime-bootstrap`.
3. Install KEDA and AWS monitoring, then the generated ArgoCD root Application.
4. Require the expected healthy, private workload and identity state.
5. Run the fixed capture sequence and required screenshots. Capture live data
   before destroy on success; skip unfinished capture immediately on failure.
6. Request `aws-live-stop` as soon as the evidence is complete or a step fails.

Do not rerun failed bootstrap or expand the load within this attempt. Technical
idempotence remains useful for tests; the experiment's failure ends the run.
The controller starts destroy at its fixed deadline even with missing evidence.

## 5. Prove cleanup and publish

Wait for the controller's terminal result. Require empty dev Terraform state
and the reviewed service inventory, including dev ECR, while retaining the
bootstrap backend and persistent budget. Record command failures separately
from empty-inventory evidence. Follow the runbook's recovery procedure if the
controller fails; verify identity and active Terraform processes before any
lock recovery. Keep cleanup running after an overdue deadline.

After cleanup, publish and verify the appropriate sanitized result: a full
provisional proof or an explicitly failed attempt with cleanup evidence. Keep
the private run directory and scanning metadata until final publication.

Publish #96 completion evidence from a separate checkout as needed; it can
close once its staging criteria are evidenced, independently of whether the
demo passes. Keep #97 open on a failed demonstration. After a passing run,
collect the dated cost result no earlier than 48 hours after destroy and wait
if AWS still marks it estimated. Verify the final packet before proposing
closure of #97. Update status lines with measured results and remaining limits.

## Verification of this plan revision

On 2026-09-10, read the supplied Claude review and inspected the current capture
commands, publication requirements, bootstrap secret path, GO validation,
inventory classification, and runbook using local reads and CodeGraph.
`git rev-parse HEAD` returned the source SHA above; the worktree was clean
before these planning documents were written. No tests, local cluster rehearsal,
or AWS operation ran as part of this planning revision.

## Implementation evidence

On 2026-09-10, the owner approved a dedicated operator-only capture Pod
Identity. ADR 0010 records its two-topic and separate-group boundary. The
working tree now includes the capture binary and runner, private secret
fingerprint handoff, failed-attempt publication, rehearsal receipt gates,
apply-boundary receipt and IPv4 checks, and after-destroy ECR rejection.

The local command `python3 scripts/m4-live-capture.py --environment local
--context mlp --run-id 20260911T005220Z --commit
88e9a103d123feddef19720af1730253c8630327 --image relay:dev --tempo-url
http://127.0.0.1:3200` passed from 00:52:33 to 00:55:52 UTC on 2026-09-11.
It recorded 600 unique load events, peak lag 593, peak replicas 12, and final
lag zero with one replica, plus idempotency, six persisted attempts joined to
the trace, DLQ source coordinates, and replay delivery. Receipts remain private
under `.evidence/m4-local/20260911T005220Z/`. Earlier failed development runs
are retained separately. No sample is frozen-candidate or AWS evidence.

The local telemetry ConfigMap now points at the Docker-host collector. During
the rehearsal only, root and relay ArgoCD automated sync were paused to test
that uncommitted configuration; both original policies were restored afterward.
Later cleanup read-back and diagnostic-suppression fixes have unit coverage
but have not had another full cluster run.

Verification: `make test` passed all Go race tests and 146 Python tests; a
subsequent Python discovery run passed 148 tests, followed by the ten capture
regressions after the initialization-failure cleanup fix. `make terraform-check`
passed validation and all eight mocked contracts. `make k8s-validate` passed
with 104 schema-valid resources and 59 intentional skips. The repository audit
reported no errors and the expected dirty-worktree warning. `make lint` passed
11 checks and failed Trivy: three refresh attempts did not produce the complete
pinned checks bundle. No security-gate bypass was used. The targeted independent
review found no remaining secret-carrier or cleanup defects after those fixes;
it was not a review of the whole implementation.

The following 2026-09-11 revision supersedes those remaining-work notes. The
owner approved shared-account daily service totals with an explicit attribution
limit, separate append-only recovery packets, and standalone #96 publication.
ADR 0010 and the runbook now record the decisions and their commands.

The second review fixes wait for generated resources and fresh initial metrics,
separate deployment time from the proof reserve, and cancel queued load before
requesting cleanup. Read-only verification and duplicate capture claims cannot
request destroy. GO freshness is checked before spending a run ID with a
15-minute reserve; the original observation ages are checked again at apply.

Publication now binds passing AWS capture exports to the controller's complete
successful result. Failed snapshots preserve unknown or not-run outcomes.
Recovery requires later identity, dev backend/default workspace, state, and
inventory observations. Final cost validates account, dates, pagination,
service totals, and settled status. Secret scanning decodes Kafka's base64
JSON carriers, and scale evidence requires fresh broker membership.

Strict `make lint` subsequently passed all 12 checks. The previous Trivy cache
contained metadata but no policy files; the same pinned bundle downloaded into
a fresh cache and restored the check. The incomplete cache was preserved.
No security gate changed. The final verification results are recorded below.

Remaining before staging: accept the final review, merge, rebuild, and produce
all clean-candidate rehearsal receipts. Live IAM authorization, private
networking, actual teardown, and billing remain unverified. No AWS apply,
staging push, or live resource mutation has run.

Final local verification on 2026-09-11: `make test` passed all 7 Go race modules
and 169 Python tests. `make terraform-check` validated 3 stacks and passed all
8 mocked contracts. `make k8s-validate` reported 104 valid resources, 59
intentional skips, and no invalid resource or error. `make aws-preflight-check`
passed. The repository audit found no errors and one expected dirty-tree
warning. A targeted Codex source rereview found no material defect in the
revised publication, recovery, or standalone staging paths; this does not
replace Claude's full review or a clean-candidate rehearsal.

Strict `make lint` passed all 12 checks after the documentation correction.
The final recovery binding also snapshots the original account/profile inputs;
its regression changes both mutable raw files and still checks the original
target. The focused publication suites passed 28 tests afterward.

Claude's next review confirmed 11 fixes and found N1–N3. The approved follow-up
adds per-series freshness margins and bounded retries, separates historical
staging validation from the executable binary plan, and documents recovery's
dirty-checkout consequence. The [resolution record](reviews/m4-second-review-resolution.md#follow-up-findings)
records these changes. They still require clean-candidate rehearsal; the old
dirty-tree sample does not cover them.

Follow-up verification: `make test` passed all 7 Go race modules and 174 Python
tests, followed by 176 Python tests after the last wait-boundary correction.
All 50 focused capture/staging/publication tests passed. Repository preflight
passed and the audit found only the expected dirty-tree warning. Targeted
source review found no remaining actionable N1/N2 issue. No AWS calls or
cluster mutations ran for this follow-up.
Strict lint passed all 12 checks, with Ruff rechecked after the final Python
edit. Changes remain uncommitted and no clean-candidate receipt was produced.
