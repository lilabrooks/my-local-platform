# M4 replay capture rehearsal

Status: the latest qualified and staged source is `474dca7`, with local run
`20260920T153931Z` passing all four machine rehearsals and all four visual
views. #96 closed when
[PR #148](https://github.com/lilabrooks/my-local-platform/pull/148) merged on
2026-09-20 UTC. #97 still awaits separate paid-run authority. See the
[completed qualification](#completed-bounded-qualification-on-2026-09-20-utc)
and [staging record](m4-96-staging-20260920.md). Earlier receipts and the
historical failure/F1 disagreement remain unchanged.

Historical status before the budget amendment merged: Run `20260920T042557Z`
passed all four local machine rehearsals on
`d63d028`; all four visual views passed inspection. Both historical failed capture
receipts remain unchanged, and their cause/F1 disagreement is not resolved by
this pass. The [bounded completion plan](../plan-m4-96-completion.md) governs
execution. Preflight `20260920T050446Z` passed all 16 checks. The approved AWS
backend and budget were created; staging initially stopped on the account-wide
budget's three `ALARM` states. The owner authorized the project-budget amendment.
Its AWS update and coverage checks now pass; code review and the explicit
candidate/qualification handoff precede resumed staging. #96 remains open.

## Bounded qualification on 2026-09-20 UTC

Candidate: `d63d028985f12e234bdaed14a9b5c7f68c46de0c` (PR #146).
Local run: `20260920T042557Z`. The clean candidate checkout is separate from
the documentation checkout. Its images were built and loaded into minikube
profile `mlp`; every ready relay and sink pod's OCI revision matched. Compose
supplied Kafka, Postgres and telemetry with its app consumers stopped. Remote
`main` and all five healthy, synced ArgoCD applications matched the candidate
before shutdown/capture and again before the demo. Remote `main` still matched
at the final local handoff; Lila's temporary merge hold ended at `05:04:46Z`.

Commands used, in order:

```bash
make m4-local-abort M4_LOCAL_RUN_ID=20260920T042557Z
make m4-k8s-sigterm M4_LOCAL_RUN_ID=20260920T042557Z
# Read-only readiness and image-provenance gate passed before capture.
make m4-local-capture M4_LOCAL_RUN_ID=20260920T042557Z
make m4-local-demo M4_LOCAL_RUN_ID=20260920T042557Z
```

- Abort simulation passed its ordered stop/cleanup checks on the clean SHA.
- Shutdown ran `04:40:21Z–04:40:49Z`. Ingest drained in 3.418 seconds and
  delivery in 21.008 seconds; both exited zero after readiness fell. All four
  cleanup checks passed. The next read-only gate found two ready ingest pods,
  one ready sink, one ready delivery pod, no terminating workload pods,
  matching image provenance and no KEDA pause.
- Capture ran `04:42:28Z–04:46:03Z`, before the demo. All 600 load-event IDs
  were retained. Its 39 accepted samples observed peak lag 584, peak group
  members 12 and peak desired replicas 12. Load and replay both ended at zero
  lag, one member, one replica and no unassigned partitions or idle members.
  All eight exports, including replay and application logs, were present with
  matching SHA-256 values; the exact-candidate receipt reports cleanup verified.
- The first demo ran `04:48:15Z–04:51:14Z` in 178.852 seconds. Its 23 scale
  samples observed peak lag 598, peak consumers 12 and final zero lag/one
  consumer. Fresh healthy-delivery and dead-letter counters each increased by
  one. Replay and all cleanup checks passed. The capture receipt and all eight
  exports remained byte-identical afterward.

There was one capture attempt, no replacement run ID, one demo invocation and
no runtime retry. Load, scrape cadence, deadlines, thresholds and candidate
source were unchanged. This run establishes that the frozen collector can
complete this local workload; it does not establish either historical failure's
cause or prove that every future observation will be decisive.

Private files below are relative to `.evidence/m4-local/20260920T042557Z/`.
This manifest records their identities; it does not publish their raw contents.

| File | SHA-256 |
|---|---|
| `abort-rehearsal.json` | `7eebe6265a3cc5c0716895165afc0d6bb5295a3fb311de5c160761b0bbad5c49` |
| `k8s-sigterm.json` | `7a94ef18fd1b1fa6a24513b882ecf4ef77de155c368d9d1196ce1dc6108ed98c` |
| `capture-result.json` | `2c0bbdd27802d7f418e08655b07bd2424025098f4eef57e68ff82858e9bcf9e4` |
| `demo-rehearsal.json` | `663f3de3d6d24bac0239b97149f9ddac076ae74a7d6c032d490bc6a2aaa19394` |

Two setup handoffs needed correction before load. The first image build was
refused because the sandbox could not write Docker buildx activity metadata;
the identical build passed outside the sandbox. `images-build.log` preserves
that refusal with SHA-256
`a2b14b015130c877541ff8513ba7ce255ff2c8eafcce34a17c2580a842ce9d8d`;
`images-build-recheck.log` records success with SHA-256
`012a13e1346eb5000b42206e828715baa22fe53d09435f218332e6237621ed8a`.
The initial terminal practice destination contained no image; a direct-save
interactive command resolved the handoff. The saved image was opened and
inspected: `visuals/terminal-practice.png`, SHA-256
`e395ebf6c7ce01e3230a03dff3f8f395ad8278ee56b708033d577a890a2e62b8`.
It is explicitly practice and is not the final terminal evidence.

All four final views were saved and opened for inspection in the required
order: ArgoCD, iTerm2, Grafana, then Tempo. ArgoCD shows all five applications
healthy and synced. The owner's two iTerm2 images show the saved demo's scaling
table, replay offsets and completion. Grafana uses the explicit demo range
`04:48:15Z–04:51:14Z`; its plotted maximum lag is 595 and maximum consumers 12.
That range includes demo replay, and its last plotted lag of one is not a
claim that replay fully drained. The demo receipt's scale samples remain the
source for the separate load result above.

After `make k8s-down` and a stopped-profile readback, `make up-apps`,
`make up-obs` and `make smoke-traces` passed. The separate Compose practice
event is `evt_d4e381e433467cc5f62648283a42c239`; Tempo trace
`9e9f5e501584777108422277f3946493` joins ingest, Kafka, consume and all four
persisted delivery attempts. The two Tempo images show its identity and span
tree through Compose Grafana at `localhost:3000`, backed by Tempo at
`localhost:3200`. Its displayed `01:01:46.441` America/Toronto timestamp is
`05:01:46.441Z`. It does not replace capture's `13-trace.json`.

The private `visual-review.json` records the ordered views, environment notes
and image hashes. The automated demo receipt still correctly says
`visual_capture_status: pending-human-capture`; it has not been rewritten.
All four machine receipts and the saved demo transcript remained unchanged.
Preflight run `20260920T050446Z` passed all 16 checks after the final remote
revision check and merge-hold release. Its local-rehearsal hash manifest names
the four unchanged passing receipts. `visual-review.json` has SHA-256
`69341f209a77c43110b5c693be8ddc0423a59d50aa231238067f86d8668acf3e`.

### Staging stop on 2026-09-20 UTC

The caller matched the selected SSO account. The account-scoped backend was
absent by both `HeadBucket` (404) and `ListBuckets`. The first bootstrap plan
failed before mutation because Terraform could not refresh a stale SSO token.
After the owner refreshed SSO, the identical plan passed its single recheck.
The reviewed saved plan then created only the state bucket, versioning,
AES256 encryption and all four public-access blocks. Its local bootstrap state
was backed up privately in the original workspace and compared byte for byte.

The reviewed guardrails plan created `mlp-live-aws-monthly`: a persistent
$5 account-wide monthly cost budget with actual 80%, actual 100% and forecast
100% notifications. All three subscribers match the private destination, and
the owner confirmed receiving alarm emails. The checked pricing worksheet
passed the existing gate. No paid apply was attempted.

The first `make aws-account-check` stopped because all three notifications
were `ALARM`. AWS reported pre-existing account spending above the threshold;
the private Cost Explorer response identifies its services. This is an active
alarm, not an uninitialized notification. `scripts/m4-aws-account.py` requires
every notification to be `OK`. No account receipt was fabricated, no gate was
weakened, and no account recheck was spent without a correction.

The service-native inventory at `05:20:31Z` passed with `runtime_empty=true`:
EKS, MSK, RDS, EC2, EBS, EIP, ELB, NAT, runtime log groups and ECR were empty
for this project. The intended backend and budget remain. Quota/availability
capture, cheap dev apply, images, hourly-plan review, GO and staging publication
remain incomplete. #97 still requires separate paid authority.

This execution reached the plan's **diagnosed-blocker** terminal condition.
The next decision concerns budget scope and policy. Preserve the qualified
candidate and local receipts. A future source change needs a deliberate
candidate/qualification decision; a stale AWS observation alone does not
justify repeating local load. No new runtime code or test revision was made.

Private staging files below are relative to `.evidence/m4/20260920T050446Z/`.
The complete raw evidence and qualification ledger are backed up in the
original workspace's ignored `.evidence/` directory.

| File | SHA-256 |
|---|---|
| `00-preflight.json` | `3d34ec8a1a0520a7eb298b76fe3ed1b9760e0b3b3e64691cbe00a22a00949c9d` |
| `bootstrap-plan.log` | `d6523f21f2f8a5b3aab80f10a47ecd194a1d6057c696047dac96342a73df0529` |
| `bootstrap-plan-recheck.log` | `3284ca8a23168636c92ebaeaf9b4d176d3351b87cc242be96722f148cb0dcbd3` |
| `account-check-command.log` | `1a0b2d935f96447b95681879301e2945e2b70df3d144e9cb8b0e1376da74df8f` |
| `budget-notification-diagnosis.json` | `c57103e5148775900916e4f97ba992647282e50d6fad43ca233f174790d8a191` |
| `04-inventory-before.json` | `54b58c3fcbbe774fe443d45fcab18884a6285536fc086a32df948aeffb93cd4f` |

### Project-budget amendment on 2026-09-20 UTC

The owner authorized tag activation, propagation, coverage verification and a
joint budget/gate update. Work uses the separate `codex/m4-project-budget`
checkout; the qualified checkout and raw local receipts are unchanged.

`Project` read back `Active` at `05:42:07Z`. The saved guardrail plan changed
only the budget's cost filter and tax setting in place. Apply and AWS readback
confirmed `TagKeyValue=user:Project$my-local-platform`, `IncludeTax=false`, the
original $5 limit, all three subscribers, and three `OK` states. The shared
live budget check passed. The state bucket's existing project tag was verified.
The new zero tagged subtotal is not proof of complete historical attribution.

All 204 Python tests passed, including 80 focused tests. The guardrail Terraform test and seven
dev mocked tests passed. Mocked provider `tags_all` values could not establish
coverage; that failed inspection is retained as inconclusive. A real-provider
inspection-only plan then verified all 14 reviewed resource tags and EKS
launch-template tags for instances, volumes and network interfaces. It used
`198.51.100.7/32`, did not create resources and cannot serve as a staging plan.

The scoped budget removes the diagnosed account-wide alarm blocker. It does
not complete #96: source review, explicit candidate/qualification disposition,
account/quota capture, cheap apply, images, reviewed staging plan, GO and
publication remain. No new local load ran, and no hourly apply was attempted.
The amended contract and tag-attribution limits are in ADR 0010 and the cost
guide. Private evidence is under
`.evidence/budget-amendment/20260920T054128Z/`.

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

## Completed bounded qualification on 2026-09-20 UTC

Status: local qualification passed on the owner-selected merged candidate
`474dca7e8e121c08f2951b02871cfe4c4b87e2ee` (PR #147). The owner explicitly
approved one qualification and held merges during its local handoff. The old
`d63d028` evidence remains unchanged; no receipt was relabelled.

Local run `20260920T153931Z` ran abort, controlled SIGTERM, machine capture,
then demo. Each passed on its first runtime attempt. Capture ran from
15:43:52Z to 15:48:12Z, preserved all eight exports and verified cleanup.
Its 39 accepted samples observed peak lag 586, 12 members and 12 desired
replicas, then lag zero, one member and one replica. This establishes this
run's result; it does not diagnose the two historical failed captures.

The demo ran from 15:48:52Z to 15:52:07Z. Its separate cohort reached lag 596
and 12 consumers, drained to zero and returned to one consumer. Healthy
and dead-letter counter deltas were each one; replay completion and cleanup
passed. The full demo dashboard range includes replay of capture and demo
records and ends with lag 1206. That screenshot does not prove replay drain.

ArgoCD, the owner's two iTerm2 images, Grafana, and Tempo were opened and
inspected in order. The scaling dashboard range was fixed to
15:48:52Z–15:51:40Z. Minikube was stopped before the separate Compose Tempo
practice; its trace `13354af13c10ec084b2bc248fe79466a` joined ingest, Kafka,
consume and four persisted attempts at 15:55:21.997Z. This is local visual
practice, not AWS evidence. The machine demo receipt's original
`pending-human-capture` field remains unchanged; `visual-review.json` records
the later human review separately.

The final remote `main` check matched the candidate. The merge hold ended
at 15:57:38Z. Preflight `20260920T155738Z` consumed the four exact-SHA receipts
and passed all 16 checks: local receipt and artifact validation, lint, seven
Go modules with race detection, 204 Python tests, controller rehearsal, image
builds, eight Terraform test runs, and Kubernetes validation. The preflight's
normal golangci CI delegation was its one lint skip; the other 11 lint checks
passed. No source, load, cadence, threshold, or timeout changed during this
qualification.

The [qualification manifest](../evidence/m4-local/20260920T153931Z/qualification.json)
records the measured results, inspected visual files and raw SHA-256 values.
Private raw files and images were backed up and byte-compared outside git.

### Diagnosed setup refusals and bounded corrections

Before load, an explicit Compose stop using `.env` refused because that
private file was absent. The three known Compose application containers were
then stopped directly and image readiness/provenance passed. Builds used the
repository's default `.env.example`. No proof event or demo had started.
A dedicated failed-command log and exact refusal timestamp were not retained;
these are absent, not reconstructed. The qualification note preserves the
observed error and correction.

While the one preflight was still running, `aws-price-template` and the
subsequent `aws-prices` command each refused with `preflight receipt is
missing`, before writing their outputs. After preflight passed, the normal
pricing sequence passed its one recheck at 16:01:14Z. No preflight, capture,
or demo was repeated. The refusal record retains both invocations.

Raw archive roots are `.evidence/m4-local/` and its `20260920T153931Z/`
subdirectory. Hashes below identify preserved records; they are not substitute
passing receipts.

| Relative file | SHA-256 |
|---|---|
| `qualification-474dca7.json` | `c319f852a45eaff9ce8fdac802ddbf1d24f0812befe4f94473c749e98193dc17` |
| `20260920T153931Z/price-preparation-refusal.json` | `586f70189015b75c6948b2bc8c7a114e2784b32cc9e9faab403c643d22ccf5ce` |
