# M4 replay capture rehearsal

Status: Two local failures preserved; PR #146 diagnostics revised; second-review F1 remains disputed.
No AWS resources were created. Issue #96 remains open.

## Observed on 2026-09-20 UTC

Clean candidate: `274e6010c88426216e697700abd7274c22941e54` (PR #144).
Private local run: `20260920T002945Z`.

The candidate images were built, loaded into minikube profile `mlp`, and rolled
out. All ArgoCD applications reported that source revision, synced and healthy.
Compose supplied Kafka, Postgres, and telemetry; its app consumers were stopped.

- `make m4-k8s-sigterm M4_LOCAL_RUN_ID=20260920T002945Z` passed. Both roles
  drained, and all four cleanup checks passed.
- `make m4-local-demo M4_LOCAL_RUN_ID=20260920T002945Z` passed in 196.221 seconds.
  Peak lag was 598, peak consumers 12, and the final sample was zero lag with
  one consumer. Healthy-delivery and dead-letter counters each increased by
  one; replay and cleanup passed.
- `make aws-live-rehearse` passed the controller race tests and 28 Python tests.
  `make m4-local-abort M4_LOCAL_RUN_ID=20260920T002945Z` passed its six ordered
  events and cleanup checks.
- `make m4-local-capture M4_LOCAL_RUN_ID=20260920T002945Z` failed at the final
  application-log export. Its receipt is `failed` with `cleanup_verified=true`.
  Event, attempt, trace, metrics, KEDA, DLQ, and replay exports were written;
  `application-logs.txt` was absent. This is not a passing capture rehearsal.

## Diagnosis and limit

The last replay sample at `00:38:59Z` said zero lag, one member, and one replica.
Kubernetes events show replay's old delivery pod stopping at `00:38:53Z` and its
replacement starting at `00:38:58Z`. The kubelet observed that replacement
running at `00:38:59.874Z`. A later read-only repetition of the same log-export
command succeeded. These observations are consistent with an export racing pod
startup, but the original command suppressed stderr, so its precise failure
cannot be recovered from the receipt.

Source inspection found a separate, demonstrable gap: the final drain check
could accept samples preceding replay resume if they were still within the
normal 30/60-second age limits. It did not read replacement-pod readiness.

The correction requires one non-terminating, running, ready delivery pod and
metric scrape timestamps after a Prometheus-clock resume boundary. It compares
the broker's whole-second refresh stamp against that boundary too; this part
depends on agreement between the ingest and Prometheus clocks. The existing
120-second drain window and proof/session deadlines remain in force.

## Second-review corrections

Claude reviewed `4ea9f11a7b4931a6512d1e1f1ec97b9337f7f0fc` and returned REVISE.
The revision addresses F1–F7:

- F1: recheck every live ingest, delivery, and sink pod and its containers
  immediately before log export. Retry only recognized container-startup and
  pod-disappearance errors for at most 60 seconds, bounded by the proof/session
  deadline. Other errors still fail. Raw stderr stays private and is discarded.
- F2: obtain the resume boundary through a 30-second bounded wait. Missing
  samples and transport interruptions are pending; HTTP errors, invalid values,
  cancellation, and deadline failures remain fatal.
- F3: exclude terminating pods before counting the live delivery replacement.
- F4: exercise real scalar/vector response parsing and the replay step's ordering
  from offset reset through unpause, boundary query, drain, and receipt.
- F5–F7: document the broker clock assumption, treat negative broker age as
  pending, and retain the accepted broker and per-metric scrape timestamps.

No load or full capture was repeated for this revision. The original export
error remains unknown; the new retry classifier does not establish its cause.

## Verification

`PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts/tests`
passed 185 tests. After using `vector(time())` for the Prometheus clock query,
all 26 capture tests passed again. `make lint` passed all 12 checks with no skips;
`make aws-preflight-check`, Ruff, and `git diff --check` also passed.

A read-only probe called the corrected readiness and sampling path against the
running local cluster. It accepted a ready replacement and a zero-lag,
one-member, one-replica sample at `2026-09-20T00:46:44Z`, after the new
Prometheus boundary. It produced no load and is not a full capture rehearsal.

For the second-review revision, the full Python suite passed 193 tests,
including all 34 capture tests. `make lint` passed all 12 checks with no skips;
`make aws-preflight-check` and `git diff --check` passed. New tests cover the
Prometheus wire shapes and replay ordering, all log-source roles and container
readiness, bounded retries, hard failures, and retained timestamps. The earlier
cluster probe exercised the first revision only.

Fresh rehearsals and visual review of the next merged candidate remain required
before AWS staging. The original failed run must not be reused as a pass.

## Merged candidate on 2026-09-20 UTC

PR #145 merged as `1e7c30ee562e813e1e28bdd9a1a7c4184c958e6e`; CI run 204
passed on its reviewed head, with the same tracked tree. Local `main` was
fast-forwarded to the squash commit and the completed repair branch removed.
The new clean checkout and rebuilt workload images used that exact revision.
All five ArgoCD applications reported it as synced and healthy.

Private local run: `20260920T012921Z`.

- Shutdown, controller, and abort rehearsals passed.
- The integrated demo passed in 217.727 seconds: peak lag 597, peak consumers
  12, final zero lag and one consumer, fresh healthy-delivery and dead-letter
  counter increments, replay complete, and cleanup verified.
- Capture started at `01:35:12Z` and failed at `01:36:20Z` with
  `load cohort or backlog observation is incomplete`. Cleanup was verified.
  Only event, attempt, and trace exports were written. The revised replay and
  application-log paths were not reached, so this run does not validate them.

The original load sample array was not exported before its assertion failed.
The failure condition combines event-ID uniqueness and three peak observations;
the receipt cannot identify which individual condition failed. Read-only
Prometheus history shows one member until `01:36:23Z`, then five; the replica
series still reported one until `01:36:44Z`, then five. Kubernetes recorded
scale-out at `01:36:15Z`. These are consistent with the capture accepting an
older terminal sample, but are not the original query responses.

Source inspection and offline regression exercise a concrete gap: producer
futures can finish during sampling, and a recent pre-submission zero-lag sample
can then release the sink delay and finish the loop before scale-out is observed.
The repair takes a Prometheus boundary before submission and another after all
600 producers complete, and uses the existing broker/scrape ordering checks.
The initial repair retained terminal metrics before validating peak observations.
The fixed load, three-second sleep, and 480-second window stay unchanged; a
genuinely quiet cohort still fails without extra load or observation time.

No full rehearsal was repeated after this failure. ArgoCD's visual view was
captured, while the remaining visual rehearsal is incomplete. No live preflight
packet or AWS resources were created. The account still matched the selected
profile, the backend and persistent budget were absent, and EKS 1.35 reported
standard support in a fresh read-only check.

### Load-observation repair verification

`PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts/tests`
passed 193 tests. The capture suite includes real Prometheus response fixtures
for pre-submission samples, producers completing during a query, transient and
persistent missing completion clocks, hard errors, and a quiet cohort that
fails immediately. Both ordering scenarios fail against `1e7c30e` and pass
with the repair. `make aws-preflight-check` and `git diff --check` passed.

After explicit bindings corrected the new test fixture's Ruff findings, all
34 capture tests passed again. `make lint` passed all 12 checks with no skips.

### PR #146 second-review response

The review of `93c697c732dcc24a098fa38af3abd0c8f88977a9` confirmed the two
ordering regressions and requested revisions. It did not approve merge.

**F1 remains disputed.** No wait for positive peaks was added. A fresh,
post-boundary quiet cohort remains negative evidence and fails immediately;
extending its observation solely until a positive peak appears conflicts with
the repository instruction to preserve quiet samples. The proposed 60-second
grace derives from a staleness allowance, not an established settling period;
the configured KSM scrape interval is 30 seconds. The saved range query records
evaluation timestamps and only three metric values, not the actual source
scrape timestamps, broker refresh timestamp, or submission boundary. It cannot
establish that a sample in the review's ten-second window would have passed
all five metrics' ordering checks. Matching the failure message alone does not
establish that the ordering defect survives. This does not prove the historical
failure is fixed, nor rule out a missed scale-out. Review reconciliation remains
required before treating this candidate as ready to merge.

**F2's diagnostic gap is addressed; its causal inference remains unproven.**
Load evidence now records every started sampling iteration, pending reason,
accepted sample, and available per-instance poller diagnostics. Finalization
exports partial evidence after stop/cleanup, including on handled timeout,
producer failure, and interruption. Storage failure keeps the receipt failed.
The diagnostics are separate from proof gates so missing diagnostic series do
not suppress otherwise valid proof observations. Iterations interrupted during
a query remain marked interrupted; acknowledgements are those observed by the
loop, not a claim that every submitted producer completed on failure.

Five observed desired replicas cannot be inverted into a contemporaneous
41–50 lag: the HPA updates asynchronously and has a 30-second scale-down
stabilization window. The saved history begins at `01:35:40Z`, after capture
started at `01:35:12Z`; it is not the entire capture window. A lag peak of one
in that export does not establish the cause or size of an under-report. The
poller's rebalance path returns before either diagnostic counter/gauge update;
zero errors and missing partitions therefore do not exclude a frozen snapshot.
Per-instance broker refresh values are retained alongside them. No claim is
made that these optional observations recover unsampled proof values.

**F3 and F4 are documented.** A full boundary-checked sample uses 17 queries;
the new optional diagnostic read adds one. The sleep remains three seconds,
but total iteration time includes requests. Both boundaries can leave up to
one normal 30-second KSM interval without an accepted proof sample. Longer
observation gaps and missed short peaks remain possible.

**F5's scope is retained.** Only the two ordering scenarios are claimed to
reproduce the original defect against the base. The other modes cover the
revised implementation's failures and deadlines; they are not all regressions
against that base.

No load, cluster probe, or AWS mutation was performed for this revision. The
owner pasted the saved demo transcript, which is textual evidence, not a
terminal screenshot. The visual gate remains incomplete. Both historical
failed receipts are unchanged, and neither replay nor log-export completion
has been established by a full capture on this repair.

Verification for this response: the full Python suite passed 197 tests, and all
38 capture tests passed after the final failure-handling changes. Those checks
cover partial export after stop/cleanup, write failure preserving the original
error, per-instance poller values, pending reasons, and diagnostic errors that
cannot hide deadline expiry. `make aws-preflight-check` passed. The only initial
lint failure was an extra blank line in this record, which was removed.
The other 11 `make lint` checks passed with no skips; the corrected Markdown
passed a separate run of the pinned `davidanson/markdownlint-cli2:v0.23.2`
container. `git diff --check` passed.
