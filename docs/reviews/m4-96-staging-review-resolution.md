# Issue #96 staging review resolution

Status: Corrections implemented and locally verified on 2026-09-19. Follow-up
review, merge, and fresh candidate rehearsals remain pending.

The initial review examined the working-tree diff based on
`8809338a796ce8b59d8226b4dd1e9a7f2f8948ab`; that base commit alone does not
contain the reviewed changes.

This follows Claude's review of the initial 5-file staging correction.
The original prompt is
[m4-96-staging-second-review-prompt.md](m4-96-staging-second-review-prompt.md).
The diff now includes the shell guard, the RDS plan-to-account comparison,
their tests, and operational documentation. The unrelated untracked
`docs/validators-and-dependencies.md` remains outside the change.

## Finding disposition

| Finding | Local correction | Verification boundary |
|---|---|---|
| F1: shell EKS request combines incompatible filters | `check_eks_support` requests the exact version as JSON and evaluates the returned version and support status with `jq`. It still runs immediately before guarded plan and apply. | The AWS stub rejects the original parameter combination and supplies response data. Tests execute the real shell filter and prove nonstandard, missing, conflicting, malformed, or failed responses prevent both Terraform operations. |
| F2: operational examples filter the deprecated field | Updated `AGENTS.md`, the costs guide, roadmap, and Terraform comment to use `versionStatus`. | A literal sweep found no remaining operational `clusterVersions[?status...` query. The dated ADR retains the original failing command as evidence. |
| F3: deprecated status can override the current status | Python and shell use `versionStatus` whenever the key is present. The old `status` field is accepted only when the new key is absent. An empty or null new value blocks the gate. | Both paths reject extended support paired with stale standard support, plus empty/null current values. They accept a current standard-support response even when the old field disagrees, and preserve legacy-only compatibility. |
| F4: RDS availability can diverge from the planned engine | Terraform has one local version pin. For enabled RDS, `runtime_shape.rds.engine_version` comes directly from the database resource. Python uses one constant for the request, matcher, and receipt. GO requires the planned version to equal the passed RDS availability observation. | A mocked Terraform assertion checks the pin and resource/output binding. GO tests reject drift in either direction, missing shape or observation, empty versions, and failed observations. |
| F5: current plan status contradicts its original reviewed SHA | The header identifies `88e9a10` as the source the plan was written against. It states that the staging candidate will be the merge commit containing these reviewed corrections and does not exist yet. | The dated historical plan and verification text remain intact. |

The optional `--cluster-type` suggestion is unchanged. Both paths continue to
require exactly one supported match; no observed duplicate response justified
expanding this correction.

## RDS producer-to-consumer trace

`runtime_contract.tf` owns `local.rds_engine_version`; `expensive.tf` assigns
it to `aws_db_instance.main`. The enabled resource's `engine_version` enters
`outputs.tf` as `runtime_shape.rds.engine_version`. `check-aws-plan.py` copies
that output into the saved plan summary.

Separately, `m4-aws-account.py` queries the pinned version, requires the exact
RDS offering in both fixed zones, and writes
`availability.rds_postgres.engine_version` with its result. `m4-stage.py`
requires a passed observation and compares it with the plan's RDS shape before
emitting GO. The existing GO hashes bind both files, and apply rechecks their
hashes and observation ages. Missing version data in an older staging input
cannot produce a new GO packet.

The mocked Terraform contract, shell tests, and staging tests exercise these
boundaries locally. They do not prove live RDS creation, IAM access, or paid
runtime behavior.

## Scope correction to the review

The review's final sequencing assigns the expensive plan and GO to #97 and
calls F1 and F4 nonblocking for #96. The
[#96 completion criteria](https://github.com/lilabrooks/my-local-platform/issues/96)
and the [staging plan](../plan-m4-live-execution.md#3-execute-96-after-its-separate-approval)
require that plan and packet during #96, before any paid apply.

F1 therefore blocks completion of #96's planned work even though the cheap
apply can skip the EKS check. F4 belongs in that same staging evidence path.
The owner's existing #96 authority covers planning and GO preparation. #97's
paid apply still requires separate approval. These changes do not close #96,
and this document does not grant GitHub publication or merge authority.

## Verification

The focused account, guarded-plan, and staging suites passed 66 tests:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest \
  scripts.tests.test_m4_aws_account \
  scripts.tests.test_check_aws_plan \
  scripts.tests.test_m4_stage
make terraform-check
bash -n scripts/aws-terraform-guard.sh
git diff --check
```

Terraform validated all 3 stacks and passed 8 mocked contracts. The first test
attempt exposed a disposable-fixture reuse error in the new apply subtests;
the corrected test prepares one valid plan/GO fixture and reuses it for the
rejected apply attempts. Post-rebase testing also exposed an expired synthetic
controller heartbeat between apply cases. Each case now refreshes that fixture
so it reaches the EKS check; the production five-second freshness limit is
unchanged. No production check was bypassed.

The broader checks then passed: `make lint` reported 12 passes with no skips;
`PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts/tests`
passed 182 tests; and `make aws-preflight-check` passed the repository and
capture-protocol checks. Markdown validation passed after the document edits.

The review corrections used no AWS account calls, real Terraform plan/apply,
cluster mutation, or new demonstration load. The earlier account diagnostics
remain historical observations. A clean candidate and fresh rehearsals are
still required after review and merge.
