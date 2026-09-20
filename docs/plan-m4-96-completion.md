# Bounded plan to complete M4 issue #96

Status: #96 staging completed on 2026-09-20 UTC for frozen source
`474dca7e8e121c08f2951b02871cfe4c4b87e2ee`; the issue closed when
[PR #148](https://github.com/lilabrooks/my-local-platform/pull/148) merged.
Local run `20260920T153931Z` passed all four machine rehearsals and
all four visual views. Preflight `20260920T155738Z` passed all 16 checks.
The cheap dev tier was applied, both immutable images were staged, and the
hourly plan was reviewed without applying it. GO passed at 16:13:51Z; the
[sanitized staging packet](evidence/m4-staging/20260920T155738Z/publication.json)
passed `verify-stage`. Final inventory at 16:13:47Z found two ECR repositories
and no hourly runtime. #97 still requires separate paid-run approval.
See the [staging record](reviews/m4-96-staging-20260920.md) for commands,
results, preserved hashes and the closure boundary.

The owner explicitly selected `474dca7` for one new qualification after PR #147
merged. The merge hold ended at 15:57:38Z. Both old and new receipts remain
bound to their original SHAs. The dated plan below retains its original
`d63d028` prediction; the execution record above supplies the approved
candidate replacement and final result. The evidence publication and later
documentation-only merges do not require another local qualification.

**Approved sequence: prepare the evidence handoff, then run the merged machine
capture once before the demo.** Freeze the candidate and use the result to decide what
happens next. Another speculative code change is not justified by the evidence
currently available.

This plan supplements the [execution plan](plan-m4-live-execution.md) and
[AWS runbook](runbook-aws-relay.md). GitHub [#96](https://github.com/lilabrooks/my-local-platform/issues/96)
remains the backlog authority. Its staging approval is already recorded;
[#97](https://github.com/lilabrooks/my-local-platform/issues/97) still needs
separate paid-run authority.

## Budget amendment disposition, 2026-09-20

Status: PR #147 merged, the owner resolved the candidate handoff, and staging
completed as recorded above. The following paragraphs preserve the disposition
written before that handoff.

The owner authorized the project budget and propagation changes after review.
The AWS mutation is complete; the code amendment is prepared in a separate
checkout. It changes Terraform tags, the budget, account/plan/GO checks, their
regression tests, and documentation. Application code, Kubernetes manifests,
load, cadence, timeouts, image build inputs and local rehearsal code are
unchanged. The Makefile change is the guardrail apply message only.

Review this bounded amendment before selecting the next staging source.
The current gates still require receipts and images at the exact approved SHA;
this change does not add an evidence-reuse exception or rewrite any receipt.
Resolve the candidate/qualification handoff explicitly before invoking
preflight. Preserve the passing local evidence, and do not rerun load merely
because billing data updates or an AWS observation expires. That handoff is
separate from the completed budget activation/apply/coverage work.

The inspection-only Terraform plan is not a staged hourly plan. It used a
documentation address to inspect tags and must never be applied or promoted to
GO. #96's remaining real staging commands, source bindings and publication
requirements below still apply. The original diagnosed-blocker record stays
historical; the amended budget's passing readback supersedes its live status.

## What is established, and what is still unknown

- PR #146 merged as `d63d028985f12e234bdaed14a9b5c7f68c46de0c`.
  Its tree matches head `2dedf7169d3a762602c0b79eb7d31bb603d6cc0b`, whose final
  commit post-dates the second code review of `93c697c`. That review did not
  cover the diagnostics amendment; the later plan review inspected its changes
  and identified the exception-message race recorded below.
  [CI run 206](https://github.com/lilabrooks/my-local-platform/actions/runs/35483939189)
  passed on that head. The local `main` ref points to the squash commit.
  During final review, the working checkout advanced separately to `a278013`
  on `docs/local-stack-lifecycle`. That work remains untouched; its `HEAD`
  is not the qualification candidate.
- Both earlier candidates passed their demo and failed their separate machine
  capture. The first failure was at log export; the second was at the load
  assertion. Those receipts remain failed. See the
  [rehearsal record](reviews/m4-replay-capture-rehearsal.md).
- The ordering defect has offline regression coverage. The cause of the second
  historical failure remains unknown, and merging does not resolve the F1
  disagreement or establish that capture now works in minikube.
- The terminal handoff also failed: the command wasn't visible to the owner.
  Pasted transcript text didn't supply a screenshot. This can be corrected
  operationally before another load cohort starts.

The merged diagnostics amendment retains per-instance poller values, iteration
boundaries and pending reasons. Once load evidence has been initialized,
finalization attempts to save it after handled failures. This is a concrete
improvement over the missing sample array from the old run. It supports one
bounded qualification on the frozen candidate.

Export can still fail, and optional diagnostics can be unavailable. Snapshots
do not recover peaks missed between scrapes. A future failure may remain
unexplained; the plan does not promise to distinguish a frozen poller from
scrape aliasing in every case.

The next run tests the existing collector at its configured cadence. Passing
unit tests cannot establish that it observes the required peaks. A fresh quiet
sample, a missing sample, and a product failure must stay distinguishable.

## Work backward from closure

| Gate | Evidence and consumer | Exit condition |
|---|---|---|
| Candidate identity | Clean checkout, OCI revision labels, running pod provenance; remote `main` and ArgoCD revisions during local qualification | Candidate SHA stays fixed through staging; the remote merge hold ends at the recorded local handoff |
| Local qualification | Four receipts plus eight capture exports, checked by `check_local_rehearsals` in `scripts/m4-preflight.py` | All four receipts pass for that SHA; cleanup and artifact hashes pass |
| Local visual practice | Legible ArgoCD, terminal, Grafana and Tempo images with run/environment notes | Actual files inspected; an unresolved practice gap requires Lila's explicit disposition |
| Preflight | `make aws-preflight`, then `capture-plan.json` | Existing checks pass without weakening their requirements |
| Cheap AWS staging | Identity/backend/budget, prices, exact plans, inventory and immutable image receipts | GO for the frozen candidate; no hourly runtime |
| #96 publication | `m4-publication-extra.py stage`, then `verify-stage`, in another checkout; tracked retry history if used | Sanitized staging packet verifies, failed-attempt paths/hashes are recorded, and the completion PR ends with `Closes #96` |

The staging publisher consumes eight GO inputs and GO itself. It does **not**
require a paid capture, paid screenshots, destroy receipt, or settled cost.
ADR 0010 explicitly permits #96 to close before #97. The required local
qualification still applies. Local visual practice is a runbook process gate:
the runbook says to complete it before preflight, although the staging
publisher does not validate screenshots. Lila owns any decision to move an
unrecoverable practice gap to #97 readiness. Until such a decision is recorded
in the runbook and rehearsal record, that gap stops progression; absence of a
machine check is not a waiver. Paid screenshots remain #97's responsibility.

Representative handoff checked in source: machine capture writes and hashes
its eight exports; `check_local_rehearsals` checks the passing receipt, exact
SHA, cleanup, provenance and hashes; preflight binds those receipts; GO hashes
preflight with its other seven inputs; the staging publisher sanitizes those
inputs and reads their hashes back. This is the path to use for closure.

## 1. Freeze the candidate and prepare without load

Codex owns setup, commands, receipt inspection and cleanup. Lila handles the
iTerm2 screenshot and any browser sign-in the available tools cannot do, and
owns the temporary hold on merges to the remote `main` branch.

1. Use a dedicated clean checkout at `d63d028985f12e234bdaed14a9b5c7f68c46de0c`.
   Proposed path: `/private/tmp/mlp-m4-96-d63d028`. Keep planning documents,
   review notes and eventual published evidence in another checkout. Preserve
   the old worktrees and failed evidence. Don't delete or relabel old receipts.
2. ArgoCD follows GitHub's `main` with automated prune and self-heal. Another
   checkout protects candidate files, but cannot prevent remote reconciliation.
   Before the first ArgoCD revision check, Lila confirms a hold on all merges
   to remote `main`, including lifecycle docs, these plan documents and
   Dependabot PRs. Codex records `git ls-remote origin refs/heads/main` and
   requires the selected SHA, then rechecks before capture, before demo and
   immediately before preflight. Also check ArgoCD's revision before capture
   and demo. Release the hold at the recorded handoff in section 4, before
   staging publication; the evidence PR can then merge normally. This plan
   does not activate a hold or change branch protection by itself.
3. Allocate a local UTC run ID and record the exact checkout, candidate SHA,
   remote SHA, run ID, artifact directory and commands in chat. Keep the same
   information and attempt counts in a plain ignored qualification note under
   `.evidence/m4-local/`, outside individual run directories. A replacement
   run ID or resumed task never resets the allowances below. The candidate's
   `.evidence/m4-local/<run-id>/` is the artifact destination. Do not create
   `.evidence/m4/<aws-run-id>/` before `aws-preflight` claims it.
4. Build/load the candidate images using the runbook and check ready pod image
   provenance. Keep Compose app consumers stopped while minikube consumers
   use the shared Kafka group. Confirm neutral sink controls, no KEDA pause,
   available telemetry, and ArgoCD synced/healthy at the chosen revision.
5. Prepare the four views before any cohort: ArgoCD signed in, Grafana's relay
   dashboard reachable, Tempo's query path reachable, and iTerm2 legible.
   Use task-owned UI port-forwards that do not conflict with the capture runner.
   Save and open a harmless practice terminal screenshot. Label it practice;
   it cannot satisfy the real visual gate. Provide commands directly in chat,
   even when a document or side panel is also available.

Exit: the owner can read the commands and save a screenshot to a known path,
and Codex can open that saved image. If this handoff fails, fix it before load.
No new helper, dashboard, metric, timeout or test suite is needed for this step.

If remote `main` is already ahead, or advances during the hold, stop before
starting the next producer and retain the observed SHAs. A docs-only advance
does not by itself corrupt machine receipts, but it breaks the exact ArgoCD
revision gate; a workload change can also cause rollout churn. Do not reset
the remote, silently select a new candidate, or count a local feature-branch
commit as remote drift. Lila must resolve the candidate/revision decision
before execution resumes. The existing evidence and attempt counts remain.

## 2. Qualify the machine path first

Use this order, subject only to the bounded setup recovery below:

1. `make m4-local-abort M4_LOCAL_RUN_ID="$local_run_id"`.
   Inspect its receipt. This uses the existing local simulation.
2. `make m4-k8s-sigterm M4_LOCAL_RUN_ID="$local_run_id"`.
   Inspect its exact-SHA receipt and restored controls before proceeding.
3. Before claiming capture, allow up to 120 seconds of read-only readiness
   checks: exactly two ready `relay-ingest` pods, one ready `sink`, at least
   one ready `relay-deliver`, and no terminating pods for those workloads.
   Verify image provenance and that `deployment/relay-ingest` uses the image
   passed as capture's `--image`. Do not alter scaling or pause KEDA to meet
   this gate. A timeout is a setup failure; do not launch capture yet.
4. `make m4-local-capture M4_LOCAL_RUN_ID="$local_run_id"`.
   Save stdout/stderr from process start, its exit status and UTC start/end,
   as well as capture's files. `capture-result.json` has no error field;
   command output is needed to retain the reported failure, and can itself
   contain only a generic exception type. Do not promise a diagnosis from it.

The capture's existing baseline waits up to 180 seconds for fresh zero lag,
one member and one desired replica. Its 600-event/16-tenant cohort keeps the
480-second window and the proof keeps its 1,200-second allowance. Let its
existing terminal conditions decide the result. Do not add a grace period,
increase load, change scrape intervals, or lower thresholds during the attempt.

The saved history showed four members and five desired replicas at
`01:35:40Z`, 28 seconds after capture started, then one by `01:35:43–44Z`.
The demo/replay had run immediately beforehand. Putting capture first removes
that preceding demo as a source of residual activity in the next capture.
The existing baseline check still handles any older activity.

Those values support the scheduling change without establishing the old
failure's cause or which capture phase was active at those times. The export
contains evaluation times, not the source scrape and broker-refresh stamps
needed to establish sample eligibility. `check_local_rehearsals` imposes no
demo-before-capture order. Record the new order; it changes the old sequence.

Stop immediately if capture fails. Finish cleanup and use the failure procedure
below; the sole retry exception requires positive evidence of an environment
failure before the first proof-event POST. Don't run the demo as a substitute.
If capture passes, verify all eight exports and the receipt now, including
replay and application logs. Partial
success is insufficient. Preserve this successful capture and continue without
repeating it for screenshots.

## 3. Run the demo once and finish the visual handoff

After capture's replay has drained and cleanup has passed, run
`make m4-local-demo M4_LOCAL_RUN_ID="$local_run_id"` once. Its receipt and
`demo-transcript.txt` provide the terminal evidence. Also retain command output
from process start in case the transcript cannot be written. Codex must show
the exact command, checkout and run ID in chat before execution; only one
operator starts it. Running both this target and `make relay-demo` manually
would duplicate load.
The demo's replay selects the last 10 minutes, so it may also redeliver earlier
capture events. Its receipt proves replayed deliveries appeared, not that all
those replayed records drained. Keep that activity out of the already-finished
machine capture's measurements and label the Grafana time range accordingly.

If the demo fails after a passing capture, preserve that capture. Use the
conditional demo-only recovery below; do not restart the full qualification.

After the machine checks pass, capture ArgoCD, terminal, Grafana and Tempo in
the runbook's order. Codex opens and inspects each saved file before marking
that visual complete. Use explicit UTC ranges in Grafana so the historical
cohort remains visible after it drains. Keep the selected trace ID and backend
with the Tempo image.

For iTerm2, Codex must paste an expanded version of this command with the
actual absolute transcript path, rather than relying on a side panel:

```bash
clear
less -R '/absolute/candidate/.evidence/m4-local/ACTUAL_RUN_ID/demo-transcript.txt'
```

This is a template, not a command to run now. Space advances, `b` goes back,
`g` goes to the start, `G` to the end, and `q` exits. Show the lag/consumer table
and completion output. Press Shift-Command-4, then Space, then click iTerm2.
That gesture saves to macOS's configured screenshot location with a generated
name. Codex gives the intended destination in chat; Lila moves the saved file
there, or supplies its actual path for Codex to open and confirm. Do not assume
the gesture chose the requested path. Keep additional images if one window
cannot show the useful output legibly.

Follow the runbook's separate Compose Tempo practice only after stopping
minikube; never overlap their app consumers. Label that trace with its own
environment and event. It cannot substitute for capture's `13-trace.json`.
The human practice record stays separate from the automated demo receipt,
whose `visual_capture_status` value is `pending-human-capture`. Do not manually
rewrite that automated receipt.

A missed terminal shot can use this same saved transcript. A failed file open,
browser login, screenshot save or read-only port-forward can be repaired
without replaying load. If a view truly cannot be recovered, record exactly
which gate is missing and stop for Lila's disposition under the local-practice
gate above. A repair gets one recheck, not unlimited attempts. Never turn a
skip into a pass or rerun load because a screenshot is missing.

### Keep the cohorts separate

| Activity | Observations it can affect | Treatment |
|---|---|---|
| Shutdown rehearsal | Kafka offsets, sink deliveries, delivery counters, pod membership | Require restored controls and the readiness gate before capture starts |
| Capture's proof event and idempotent repeat | One durable event, attempts, trace, healthy delivery and flaky-subscriber DLQ | Correlate event/trace IDs; the repeated request is not another durable event |
| Capture's fixed 600-event load | Group lag, membership, desired replicas, delivery totals | Use capture's boundaries and accepted samples |
| Capture's poison and replay | DLQ offsets; redeliveries, lag and membership | Use their recorded offsets/IDs and verify final drain |
| Demo and its replay | Another fixed load, deliveries, DLQ and group offsets | Run after capture; don't combine the two runs' peaks |
| Compose Tempo practice | A separate event, consumer activity and trace | Isolate consumers and label it as visual practice |

## 4. Consume the receipts once

Check the four receipts and artifact hashes against the frozen candidate;
old passes from `274e601` or `1e7c30e` don't count for `d63d028`. Check the
visual files and their environment notes, or Lila's recorded practice-gap
disposition. A Compose practice trace remains separately labelled.

Immediately before preflight, recheck and record the remote `main` SHA. With
the local receipts complete and the visual gate resolved, including ArgoCD's
image or its explicit practice-gap disposition, record the end of Lila's
merge hold. Later plan/evidence merges do
not retroactively invalidate the recorded local qualification. Keep using the
clean candidate checkout for preflight and staging, not a newly advanced HEAD.

Allocate a fresh AWS run ID and run the existing `make aws-preflight` with the
local run ID. This already runs lint, tests, `aws-live-rehearse`, image builds,
Terraform checks and Kubernetes validation. Don't precede it with another
manual copy of all those checks. A separate standalone controller-test run is
unnecessary here because preflight invokes it.

If preflight fails on a diagnosed tool, credentials or environment issue
without a source change, preserve the failure and repair that issue once.
Reuse still-valid local receipts for the same SHA; use a fresh AWS run ID as
the command requires. If that rerun fails, stop with its named blocker. A new
AWS run ID alone is not a reason to repeat the local load. A source change
requires a deliberate new candidate and qualification; never relabel receipts.

## 5. Stage and close #96

Follow the runbook's existing staging commands under the recorded cheap-tier
approval. Check that authority still covers each concrete plan; any changed
scope is a separate decision. Keep the budget destination in private config.

1. Confirm identity and backend state. Resolve access errors before deciding
   a backend is absent. Bootstrap only if needed, then create/verify the
   persistent $5 monthly budget and all notification subscribers.
2. Collect account/quota/availability/support evidence and check the official
   price inputs. These are current observations, not copies of September 19's
   diagnostics. Plan the session so account, price, plan and inventory evidence
   reaches GO within the existing 24-hour limits.
3. Review and apply the saved cheap dev plan with all three hourly flags false.
   Preserve its summary before the next plan replaces it. Inspect inventory
   for the two expected ECR repositories and absence of hourly runtime.
4. Stage both SHA-tagged `linux/amd64` images and inspect their immutable
   digests/revision labels. Generate and review the expensive topology plan
   without applying it. Generate GO from the unchanged, passing inputs.
5. Preserve GO and all eight input files. Publish before any later refresh or
   re-plan can overwrite the only raw copy bound to this decision. Publish through
   `m4-publication-extra.py stage` into a separate evidence checkout, then run
   `verify-stage` and inspect the sanitized output. Keep the candidate clean.
6. Match each #96 checkbox to a concrete artifact. If recovery was used,
   include the tracked failure record described below, with archived relative
   filenames and SHA-256 values, and cite it in the PR body. Open the evidence
   PR with `Closes #96`, update the relevant status lines, and leave #97 waiting
   for its separate approval. Don't wait for a paid demo or settled billing to
   publish #96's completed staging evidence. Closure requires the PR to merge;
   an open PR is a publication handoff, not a closed issue.

The historical staging packet remains valid evidence of what was staged at
that time. Its publication does not claim inputs are still fresh for a later
paid apply. Expired AWS observations require refreshing their own inputs and
GO; they don't automatically invalidate unchanged local qualification.

## Failure procedure: preserve success and bound recovery

Across this qualification, allow at most one capture that reaches its first
proof-event POST, and at most two invocations of `make relay-demo`. The second
demo invocation requires a corrected, diagnosed environment or handoff fault;
it is not automatic. One verified setup-only capture failure may use a new
local run ID under the rule below. These allowances do not reset with a new
run ID, task, branch or documentation revision.

A demo invocation counts once execution reaches `make relay-demo`, even if
it subsequently fails before generating load. Positively identified earlier
refusals are setup failures. A missing `demo-transcript.txt` cannot establish
this: the writer saves it after execution, and interruption can leave no file.
The receipt's `commands` list also describes both commands unconditionally.
Use retained execution output and the diagnosed code path; if the stage is
unknown, conservatively count the invocation and do not authorize a retry.

At each named setup or handoff step, allow one diagnosed correction and one
recheck. A repeated or unexplained failure stops that step with a recorded
blocker. This applies to visual recovery and staging/publication repairs too;
these are not open-ended loops hidden outside the load budget.

| Failure | Bounded response | Condition for further work |
|---|---|---|
| Setup or visual handoff, before load | Correct the diagnosed path/login/tool problem once and recheck that step | Gate passes; otherwise stop with the named blocker |
| Capture claimed, but positively established environment failure before the first proof POST | Preserve the entire run; correct the fault and verify cleanup/readiness; allow one new local run ID | Re-make all four receipts under the new ID; no previous receipt is copied or relabelled |
| Producer, ordering or stale/missing proof observations | Preserve command output, receipt and phase artifacts; retain `12-metrics.txt` with boundaries, pending reasons and poller values if present; record absence otherwise | Identify the failed condition and distinguish missing observation from a quiet cohort |
| Fresh quiet cohort without observed scale-out | Preserve it as a failed qualification; compare only evidence from its original window | A justified measurement or product diagnosis, not a wait chosen to obtain a peak |
| Replay, logs or cleanup in capture | Preserve the phase-specific artifacts; complete cleanup; don't rerun successful load phases | A concrete failing operation and a bounded corrective action |
| Demo fails after capture passed | Preserve and hash-check the passing capture and failed demo; use the conditional demo-only recovery below | At most one demo retry after a diagnosed environment or handoff fault is corrected, with unchanged SHA and qualification configuration |
| Remote `main` or ArgoCD revision drifts during the hold | Preserve observed revisions and stop before the next producer | Lila resolves candidate/revision ownership; no automatic candidate change or allowance reset |
| Staging input or publication | Preserve receipts and hashes; one diagnosed correction and recheck of the affected step | Existing passing local evidence remains usable if its SHA and validity are unchanged; otherwise stop with the named blocker |

### Capture setup recovery

`capture-start.json` claims the run ID before setup. Permit one replacement
only when retained output and the exact failing code path positively establish
an environment failure before the first `/v1/events` proof POST, and the fault
has been corrected without source or qualification-configuration changes.
For example, a reported pod-provenance readiness failure occurs in setup.
A baseline timeout alone is not an environment diagnosis. Unknown execution
stage, an in-flight POST, a proof failure or any later failure ends capture
qualification even if the 600-event cohort has not begun.

Absence of `10-event.json` is not a safe discriminator: capture writes it only
after the event and repeat, attempts, healthy delivery, database and Kafka
checks. Absence of `12-metrics.txt` also proves no phase boundary; its writer
runs only after load evidence is initialized, and can fail. Keep whatever was
written without manufacturing missing files or modifying the failed receipt.

Preserve the old run directory intact, record its files/hashes and correction,
allocate one new UTC run ID, and re-make all four receipts there in the planned
order. Recheck readiness after the new shutdown rehearsal. The one capture
allowance remains unspent only under the positive pre-POST rule above; a
second setup failure after claiming capture ends this qualification.

### Conditional demo-only recovery

A retry is justified only by a specific corrected environment or handoff
failure. A quiet cohort, unexplained missed peak, product defect or desired
positive result does not qualify. Restore and verify sink/KEDA controls and
consumer isolation first. If the correction changes source or qualification
configuration, stop and choose a new candidate deliberately.

Before a permitted retry, verify that preflight/GO has not bound the failed
demo receipt. Record the failure and correction, and hash the passing capture
receipt and its eight exports. Keep their bytes, names and run ID unchanged.
The retry remains part of that same qualification run and uses its local run
ID; each demo attempt retains its own real start/end timestamps.

The existing demo writer refuses an existing output or sibling transcript.
Preserve the failed `demo-rehearsal.json`, `demo-transcript.txt` and command
output intact in a new `failed-demo-attempt-1/` directory under the local run.
Record original paths and hashes, verify the archived bytes, and move only
those failed demo files out of the canonical output names. If failure occurred
before a file was created, record its absence instead of fabricating it.
Do not overwrite an archive, alter a receipt, or move any capture artifact.

Then run `make m4-local-demo M4_LOCAL_RUN_ID="$local_run_id"` once, within the
remaining invocation allowance. It creates a new receipt and transcript at
the canonical paths consumed by preflight. If a positively identified
pre-demo setup failure wrote files, preserve them the same way in a uniquely
named setup-failure directory before its one permitted recheck; that does not
spend a `make relay-demo` invocation or erase the setup failure.
Recheck the preserved capture hashes afterward. If the demo passes, keep the
failed archive and document all attempts in the qualification handoff before
preflight. If the second counted demo invocation fails, stop: no third demo,
no repeat capture, no AWS staging. A pre-demo refusal follows the separate
bounded setup rule. A failed visual save after a passing demo uses the saved
output and does not consume or justify another demo attempt.

This follows the existing producer/refusal and consumer paths:
`m4-local-demo.py` refuses existing files, then records the exact candidate;
`check_local_rehearsals` reads the four canonical receipts and hash-checks the
capture's eight exports. Neither it nor the staging publisher consumes the
archived failed demo. Before the closure PR, record every recovery's candidate,
run ID, attempt times, diagnosed failure/correction, archived relative filenames
and SHA-256 values in `docs/reviews/m4-replay-capture-rehearsal.md` in the
publication checkout, and cite that entry from the PR. Record missing files as
absent. Keep raw transcripts private and hash them; do not publish private
paths or turn an archived failed receipt into a passing one. The tracked
manifest makes the failure discoverable but does not publish its raw contents.

For a failed capture, allow one bounded read-only postmortem while the local
state is available: existing logs, pod/Argo state and Prometheus history for the
original run interval. Label later snapshots as postmortem. Don't append them
to the qualifying sample or treat later peaks as a pass.

Before another code/test PR, write the observed failure, the causal evidence,
the smallest proposed correction, how it can be checked without load, and why
another qualification would answer a different question. Fix environment and
handoff faults operationally. Add a regression only for an established code
defect; a generic failure message alone is insufficient.

If that single analysis cannot explain the failure, stop automatic execution
and record an explicit blocked outcome. The next decision is a bounded review
of the observation protocol or an explicit M4 deferral. Any proposed change to
the acceptance contract must be presented as such. #96 stays open unless its
actual criteria pass; repeated speculative repairs are not the fallback.

There are three terminal outcomes: verified staging evidence and a merged
closure PR; a diagnosed blocker with a concrete next action; or insufficient
evidence after the one analysis. The latter two leave #96 open and end this
execution plan. They do not automatically schedule another review, code PR or
rehearsal. Lila owns any subsequent change of candidate or acceptance contract.

## Verification of this plan

Read-only inspection covered merged PR/CI state, #96 and #97, the Make targets,
receipt producers, `check_local_rehearsals`, preflight, GO freshness checks,
the standalone staging publisher, ADR 0010 and the visual runbook. The source
trace establishes the proposed handoffs, not their runtime success.

No runtime code, tests, thresholds or evidence schemas were changed during plan
drafting and review. That work ran no local load, cluster probe or AWS operation.
Execution began afterward; the status line and rehearsal record report its
results. Planning and status edits remain separate from the frozen candidate.
The pinned Markdown linter and `git diff --check` passed. Runtime tests were
not repeated for these prose changes. The original link check covered the full
text of these four files, not just changed lines: 3 local links in this plan,
6 in the execution plan, 4 in the runbook and 1 in the rehearsal record. The
check resolves target files; it does not validate section anchors or web URLs.
Run it from the repository root:

```bash
python3 - <<'PYLINKS'
from pathlib import Path
import re

names = (
    "docs/plan-m4-96-completion.md",
    "docs/plan-m4-live-execution.md",
    "docs/runbook-aws-relay.md",
    "docs/reviews/m4-replay-capture-rehearsal.md",
)
for name in names:
    path = Path(name)
    targets = re.findall(r"\]\(([^)]+)\)", path.read_text())
    for target in targets:
        if re.match(r"^[a-z]+://", target) or target.startswith("#"):
            continue
        assert (path.parent / target.split("#", 1)[0]).is_file(), (name, target)
print("All relative document link targets resolve.")
PYLINKS
```

## Plan-review dispositions

- **Finding 1 corrected:** the final head is identified as a post-review
  amendment, with CI and tree equality distinguished from review coverage.
- **Finding 2 recorded, code change deferred:** a producer can fail between
  the explicit exception check and the per-iteration `f.result()` read. The
  resulting exception may report its type instead of `load producer failed`.
  `post()`, `load_pool()` and `run()` still request stop/cancel on failure;
  the stop request is idempotent. This is a known diagnostic limitation of
  the frozen candidate. An interrupted observation without a sample or pending
  reason and a generic exception is compatible with this race, but is not a
  unique signature. Retain the output; call it a producer failure only if the
  evidence establishes that cause. Do not infer a scaling defect from the less
  specific message. This message path alone does not justify a new candidate.
- **Finding 3 addressed conditionally:** a passing capture survives a later
  demo failure. One diagnosed-fault demo retry has an explicit archive and
  receipt-discovery path; a second failure or unexplained quiet run stops.
- **Finding 4 clarified:** the full-file link count reproduced as 3 + 6 + 4 + 1.
  The command and scope are now written down. All checked targets resolve.
- **Minor correction:** `visual_capture_status` is the field;
  `pending-human-capture` is its value.

The plan review accepted the sequencing. Its stronger claim that every next
outcome will be decidable is not adopted. Nor does the unchanged quiet-cohort
completion rule establish that the historical ten-second window passed all
ordering checks. The missing timestamps and possible missing diagnostics
remain evidence limits. One bounded run still answers more than another
speculative change, with an explicit stop if the result remains unresolved.

## Final-review dispositions

- **F1 addressed with a narrower retry rule:** readiness is checked before
  claiming capture; one positively diagnosed pre-POST environment failure may
  replace the run ID. The review's inference from a missing `10-event.json`
  was rejected because that file is written after multiple proof assertions.
- **F2 addressed:** Lila owns a remote merge hold with explicit start/end,
  revision checks and a stop on drift. Separate checkouts alone are inadequate.
  Her local lifecycle-docs branch remains separate from the candidate.
- **F3 addressed:** every recovery requires a tracked file/hash manifest cited
  by the closure PR; the staging publisher still publishes its existing packet.
- **F4 addressed with a corrected discriminator:** count actual demo-command
  invocations. Transcript absence and the static `commands` field cannot prove
  that no demo ran. Unknown stages do not earn another attempt.
- **F5 addressed:** preserve available metrics and command output, record
  missing artifacts, and acknowledge that diagnostics may remain insufficient.
- **F6 clarified:** the runbook explicitly requires local visual practice
  before preflight. Lila owns disposition of an unrecoverable practice gap;
  the absence of screenshot checks in #96's publisher is not an automatic waiver.
- **F7 addressed:** distinguish the screenshot's configured save location
  from the intended destination and confirm the actual saved file.

These are documentation corrections, not a new runtime candidate. Passing the
next qualification is still an empirical question; no review has certified it.
