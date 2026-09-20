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

The correction requires one current, running, ready delivery pod and broker
and metric observations after a Prometheus-clock resume boundary. It retains
the existing 120-second drain window and proof/session deadlines. Hard errors
still fail the capture; no log error is ignored and no load is repeated.

## Verification

`PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts/tests`
passed 185 tests. After using `vector(time())` for the Prometheus clock query,
all 26 capture tests passed again. `make lint` passed all 12 checks with no skips;
`make aws-preflight-check`, Ruff, and `git diff --check` also passed.

A read-only probe called the corrected readiness and sampling path against the
running local cluster. It accepted a ready replacement and a zero-lag,
one-member, one-replica sample at `2026-09-20T00:46:44Z`, after the new
Prometheus boundary. It produced no load and is not a full capture rehearsal.

Fresh rehearsals and visual review of the next merged candidate remain required
before AWS staging. The original failed run must not be reused as a pass.
