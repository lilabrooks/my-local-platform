# M4 replay capture rehearsal

Status: Local failure preserved; correction awaiting review and merge.
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
