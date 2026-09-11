# Claude final second review: M4 preparation fixes

Review the current working tree in `/Users/lilabrooks/code/my-local-platform`.
This is the final follow-up to your second review, which reported F1–F12 and
blocked freezing the candidate. Determine whether those findings are resolved,
whether the fixes introduce material regressions, and whether this implementation
can now be frozen for clean-candidate local rehearsal.

Read this entire prompt before starting. It supersedes the earlier reviewer
prompt's statements that final-cost collection and the Trivy failure remain
unresolved. It does not supersede the original findings or their evidence.

## Authority and method

This is a read-only review. Do not edit repository files, stage, commit, push,
change GitHub issues or PRs, run AWS CLI commands, start or stop containers,
change cluster state, execute bootstrap, or repeat the demonstration load.
Leave the running local stack untouched. Do not install tools or repair caches.

Read `AGENTS.md` and `CLAUDE.md`; follow their navigation rules. Record actual
HEAD, branch, and worktree status at the start and end. The last checked HEAD
was `88e9a103d123feddef19720af1730253c8630327` on `main`, with uncommitted and
untracked implementation files. Inspect both `git diff HEAD` and untracked
source. HEAD alone does not identify the reviewed implementation.

Focused offline tests with disposable fixtures are allowed after you inspect
their effects. Do not run broad Make targets or container-backed checks under
this review's authority. If a necessary check cannot run safely, report the
limit. Use current official documentation for uncertain AWS/Terraform semantics
without querying the account. GitHub history, if needed, is read-only through
the connector.

Use synthetic secrets and account values for adversarial tests. Never reproduce
real account identifiers, endpoints, credentials, or secret carriers in the
report. Treat implementation notes, test counts, and the prior Codex helper
review as claims to verify. Separate source inspection, tests you actually ran,
historical local evidence, and live-only unknowns.

## Governing inputs

Read these files completely:

- `docs/reviews/m4-second-review-resolution.md`
- `docs/plan-m4-live-execution.md`
- `docs/adr/0010-live-aws-relay-contract.md`
- `docs/runbook-aws-relay.md`
- `docs/costs.md`

The original second review, including F1–F12, is at:
`/Users/lilabrooks/.codex/attachments/a874afc0-9b15-4933-b02a-0f6609df585a/pasted-text.txt`.

The first review is at:
`/Users/lilabrooks/.codex/attachments/9b901984-1931-441c-81ef-c1f1e4cd5155/pasted-text.txt`.

Use the original finding IDs. If an attachment is unavailable, say so; do not
invent its wording. PR #137 and issues #95, #96, #97, and #136 are historical
context, not execution authority.

The owner explicitly approved these choices:

1. A dedicated operator-only capture Pod Identity, limited to the two relay
   topics and the operations required for proof, with a separate capture group.
2. Settled account-wide daily service totals over the session's UTC dates for
   #97, explicitly labeled as unable to establish exact M4 attribution.
3. A separate append-only manual-recovery packet linked to the original failed
   attempt, preserving controller history and requiring fresh cleanup evidence.
4. Independent sanitized #96 staging publication under
   `docs/evidence/m4-staging/<run-id>/`, whether or not #97 ever runs.

Do not list these decisions as pending owner approval. Check that implementation
and documentation stay within them. Flag any newly discovered conflict or
broader authority separately. AWS staging and paid execution still require
their own approvals.

## Reported verification and its limits

The implementer reports:

- `make test`: 7 race-enabled Go modules and 169 Python tests passed. Python
  discovery passed again after the final production-code change; the 28 focused
  publication tests passed after additional profile-routing assertions.
- `make lint`: all 12 checks passed. Trivy's old cache contained metadata
  without policy files. The same pinned bundle was downloaded into a fresh
  cache, the incomplete cache preserved, and the gate rerun without a bypass.
  Later affected Ruff and Markdown checks were rerun separately.
- `make terraform-check`: 3 stacks validated and 8 mocked contracts passed.
- `make k8s-validate`: 104 valid resources, 59 intentional skips, zero invalid
  resources or errors. `make aws-preflight-check` passed.
- Repository audit: zero errors and an expected dirty-worktree warning.
- A targeted Codex subagent source review found no remaining material defect in
  the revised publication/recovery/staging paths. It did not rerun tests or
  review the entire implementation; it is not a Claude approval.

The only full capture sample cited is local run `20260911T005220Z`: 600 unique
load events, peak lag 593, peak desired replicas 12, final lag zero and one
replica. Its private receipts are under `.evidence/m4-local/20260911T005220Z/`.
It used a dirty tree, predates the fixes, and did not exercise AWS deployment.
No new load sample was taken. No clean candidate has been frozen, and no AWS
staging or paid proof was executed during this preparation work.

Do not treat that old sample as coverage of today's code. Equally, distinguish
an expected post-freeze rehearsal gate from a code defect that prevents freeze.

## Review scope

Start with the changed code and tests in:

- `scripts/m4-live-capture.py`, `scripts/m4-stage.py`, and `scripts/m4-preflight.py`
- `scripts/m4-evidence.py` and the new `scripts/m4-publication-extra.py`
- `scripts/m4-aws-inventory.py` and `scripts/aws-terraform-guard.sh`
- `scripts/tests/test_m4_live_capture.py`, `test_m4_evidence.py`,
  `test_m4_publication_extra.py`, `test_m4_stage.py`, and `test_m4_preflight.py`
- `tools/m4-live-run/`, `tools/m4-bootstrap/`, and their tests
- `services/relay/cmd/relay-capture/`, shared Kafka transport, and relay Dockerfile
- Terraform capture identity/contracts, AWS service accounts, the local relay
  ConfigMap, `k8s/validate/aws_test.go`, and affected Make targets/installers

Follow producer/consumer boundaries as needed. Prioritize these checks:

### 1. Reproduce the F1–F12 failure conditions against the fixes

| Finding | Required check |
|---|---|
| F1 | Delayed ArgoCD-created Deployments, Tempo, and Services are awaited before rollout/forwarding; initially absent scrapes can reach a fresh baseline within the bounded window. Check what the fake deployment test actually exercises. |
| F2 | Both publication and later verification require matching clean passing AWS capture receipts, unchanged exports, and the controller's full successful result. Cleanup success alone must not permit a pass after overdue cleanup, failed destroy, log cleanup, transcript, or immediate-cost checks. |
| F3 | Sampling failure or interruption cancels queued load and requests stop before the executor drains. At most the already-active workers may continue within their bounds; preserve the original failure. |
| F4 | Before-session validation leaves 15 minutes of receipt freshness and does not reset original observation ages. Apply independently rechecks them. |
| F5 | Deployment uses the destroy deadline minus the proof reserve; proof gets its own bounded allowance. Installer parent timeouts cover child waits without extending the session. |
| F6 | Identity failures, missing exits, and a lost controller can publish an honest failed snapshot. Unknown/not-run outcomes cannot become cleanup success; manual recovery preserves failure history. |
| F7 | Placeholder text, monthly totals, early collection, estimated days, missing dates/pages, or wrong account/session bindings cannot publish as settled final cost. |
| F8 | Kafka base64 key/value/raw-value carriers are decoded before bounded nested-JSON secret scanning. Check malformed, oversized, and depth-limit behavior and error-output leakage. |
| F9 | A failing Python pre-session check is reachable through the Go fake and leaves no spent session, controller state, lock, apply, or destroy. Check changed-IP, non-/32, and unreachable-address tests. |
| F10 | A second signal cannot falsely report stop acceptance or strand an interrupted request. Read-only output verification and duplicate capture invocation cannot request destroy. |
| F11 | Fresh broker membership must exceed one and finish at one with zero lag and one desired replica. Pending pods or evaluation-time timestamps cannot substitute for fresh consumer observations. |
| F12 | Runbook commands, capture-plan flags/queries, Secret consumers, ELBv2 scope, exact-SHA image build/load, and credential recovery match executable behavior. |

### 2. Trace the new publication boundaries end to end

For a passing run, trace request → Kafka → delivery/attempts → trace → capture
exports → capture receipt → controller outcome → sanitizer → publication →
later verification. Ensure cohort joins and hashes reject mixed-run or stale
outputs. Preserve the fixed 600-event/16-tenant cohort and sink delay through
drain; do not propose extending the experiment to get a positive result.

For recovery, trace failed controller → exclusive private snapshot → published
failure → later credential refresh → recovery observation → published recovery
→ verification. Check specifically:

- The failed publication binds original identity and GO files. Changing mutable
  raw account/profile inputs later cannot retarget STS, Terraform, or inventory.
  Inspect the actual AWS runner and Terraform environment, not just mocks.
- State evidence addresses the intended S3 bucket/key/region and default
  workspace; backend credential/endpoint overrides or metadata changes cannot
  make an unrelated empty state pass. Check compatibility with the backend
  metadata that the documented `aws-init` produces using offline evidence.
- Inventory requires the correct schema, project, region, full service set,
  and empty ECR as well as runtime services. Identity, state, and inventory
  observations must belong to the later recovery collection and be fresh.
- Active/unknown controller or Terraform process state blocks collection.
  Collection is read-only; login, initialization, destroy, and lock repair stay
  outside its authority. Walk the manual recovery commands literally, including
  profile/workspace propagation and preservation of the original files.
- Missing/failed evidence yields `cleanup_unverified`; a recovered cleanup
  never converts the demonstration to passed. Repeated observations are
  separate packets. Old packets without authority bindings fail closed and
  cannot be backfilled from mutable inputs.

For staging, trace the eight GO inputs → dated validation → sanitization →
separate-checkout publication → read-back. Check source and output hashes,
destination safety, retention, failure behavior, and historical semantics:
publication must not imply fresh approval or spend authority, or dirty the
candidate that will execute #97.

### 3. Verify the accepted cost and secret contracts

Inspect final-cost collection and validation across the actual API response
shape: intended linked account, daily UnblendedCost grouped by SERVICE,
inclusive session UTC dates with an exclusive next-day end, month boundaries,
all pages/dates, service amounts, `Estimated: false`, and collection at least
48 hours after cleanup. Check receipt bindings and provisional-to-final
publication. Shared-account totals cannot prove exact M4 attribution or the
$5 per-run maximum; ensure closure and spend-control language says so.

Recheck the Go-produced/Python-consumed fingerprint receipt, partial-bootstrap
failure, capture-time and post-destroy scanning, and screenshot-review boundary.
Distinguish supported representations from arbitrary encodings, fragments,
pixels, and an actor able to rewrite evidence and hashes. Recheck capture IAM
scope and replay identity for regressions without claiming live authorization.

## Suggested safe tests

After inspecting their effects, these focused commands are candidates:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest \
  scripts.tests.test_m4_live_capture \
  scripts.tests.test_m4_evidence \
  scripts.tests.test_m4_publication_extra \
  scripts.tests.test_m4_stage \
  scripts.tests.test_m4_preflight
GOPROXY=off GOSUMDB=off go -C tools/m4-live-run test -race ./...
GOPROXY=off GOSUMDB=off go -C tools/m4-bootstrap test -race ./...
```

Missing cached dependencies are a review limit; do not install them. Add only
small disposable reproductions needed to establish a concrete failure. Do not
alter source tests or broaden this into a new infrastructure experiment.

## Required response

Lead with actionable findings, severity ordered. Each needs an exact current
file/line, triggering input or sequence, concrete consequence, evidence or
reproduction, and the smallest correction/test. Identify remaining F-findings
versus new regressions. Avoid style-only findings and speculative redesigns.
If no actionable findings remain within scope, say so explicitly.

Then provide:

1. F1–F12 disposition: fixed / partial / open / not verifiable, with evidence.
2. Commands actually run, outcomes, inspected scope, and evidence limits.
3. Any minimum work before freeze, separately from post-freeze clean-candidate
   rehearsal requirements, staging approval, and live-only validation.

End with explicit verdicts:

- Implementation: ACCEPT / REVISE.
- Freeze: READY FOR FROZEN-CANDIDATE REHEARSAL / BLOCKED BEFORE FREEZE.
- Staging: prerequisites still missing, or READY FOR OWNER STAGING APPROVAL
  only if exact clean-candidate receipts and other staging prerequisites have
  actually been verified. Review approval does not authorize AWS execution.

Do not execute the plan, freeze a commit, authorize spending, or request AWS
approval. Return the review to the owner.
