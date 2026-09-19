# Claude second review: issue #96 staging corrections

Review the uncommitted staging corrections in
`/Users/lilabrooks/code/my-local-platform`. Decide whether the 2 defects found
during #96 account diagnostics are fixed, whether the changes introduce a
material regression, and what must happen before staging can resume.

This is a bounded review of the current 5-file diff. The earlier M4 preparation
review prompts describe historical work; do not reuse their unresolved-finding
lists or reopen the entire M4 implementation without a concrete connection to
this change.

## Authority and source identity

Read `AGENTS.md` and `CLAUDE.md` and follow their navigation rules. Record HEAD,
branch, and worktree status at the beginning and end. When this prompt was
written, HEAD was `8809338a796ce8b59d8226b4dd1e9a7f2f8948ab` on `main`.
The implementation is the working-tree diff, not that commit alone.

This is a read-only review. Do not edit files, stage, commit, push, mutate
GitHub, run AWS CLI commands, run Terraform plan/apply, change the cluster,
start or stop containers, or repeat the demonstration load. The local Compose
stack and `mlp` minikube profile are running; leave them untouched. Do not
install tools or repair caches. Focused offline tests with disposable fixtures
are allowed after inspecting their effects. Public official documentation and
read-only GitHub connector inspection are allowed when needed.

The owner has already authorized #96 cheap staging and supplied a budget alert
destination. Do not report staging authority or the email choice as missing.
That authority does not authorize #97's paid apply or GitHub publication and
merge. This review authorizes none of those actions.

The untracked `docs/validators-and-dependencies.md` predates this task and is
outside the review. This review prompt is also untracked. The clean checkout
at `/private/tmp/mlp-m4-96-8809338` contains the old candidate, without these
fixes; do not review it in place of the working tree. Do not read or reproduce
private tfvars, AWS account IDs, email addresses, credentials, or state.

## Changed files and governing context

Inspect the complete diff and the surrounding behavior in:

- `scripts/m4-aws-account.py`
- `scripts/tests/test_m4_aws_account.py`
- `infra/terraform/envs/dev/expensive.tf`
- `docs/adr/0010-live-aws-relay-contract.md`
- `docs/plan-m4-live-execution.md`

Use `docs/runbook-aws-relay.md`, `docs/costs.md`, the relevant Make targets,
Terraform contract tests, and staging consumers as needed to verify a boundary.
Issue #96 remains incomplete; this change must not close it.

## Implementer's observations and evidence limits

The following were observed on 2026-09-19 using read-only AWS calls in
`us-east-1`. Treat them as reported observations, not your own verification:

1. The original EKS query combined `--cluster-versions 1.35` with
   `--version-status STANDARD_SUPPORT`. AWS returned `InvalidParameterException`
   saying only one of the version-selection parameters was accepted. Querying
   `--cluster-versions 1.35` alone returned `STANDARD_SUPPORT`.
2. RDS rejected PostgreSQL `17.4` with `InvalidParameterCombination` and
   `Cannot find version 17.4 for postgres`. Engine discovery listed `17.11` as
   available. Its orderable-options query confirmed `db.t4g.micro`, encrypted
   gp3, and both `us-east-1a` and `us-east-1b`.
3. All 5 quota checks passed. The MSK limit used the existing documented
   fallback, not an account-specific Serverless quota. The corrected
   availability function passed all its checks. Regional presence and quotas
   do not prove successful provisioning, IAM access, or Spot capacity.
4. The intended SSO account matched its configured profile. The backend bucket
   returned 404 and was absent from the owned-bucket list; the M4 budget was
   absent. No AWS resources were created and no ECR images were pushed.

The diagnostics called `quotas(Runner())` and `availability(Runner(),
"us-east-1")` directly. They did not run the full account receipt producer or
produce a passing staging packet. The original command output is in the Codex
task history; the repository has an ADR summary, not preserved raw diagnostic
receipts. Identify any observation you cannot independently establish under
this review's authority. Do not query the account to fill that gap.

Reported checks for this diff:

- `make lint`: 12 passed, no failures or skips.
- `make terraform-check`: 3 stacks validated, 8 mocked contracts passed.
- `python3 -m unittest discover -s scripts/tests -p test_m4_aws_account.py`:
  14 tests passed.
- `python3 -m unittest discover -s scripts/tests -p 'test_m4_*.py'`:
  117 tests passed.
- `make aws-preflight-check` and `git diff --check` passed. Markdown validation
  passed again after the final documentation edits.

No fresh clean-candidate rehearsal or full `aws-preflight` passed during this
task. Local images were built and loaded at the old HEAD, and ArgoCD reported
that revision synced and healthy. Those observations do not establish a
rehearsal pass for the changed source. Current prices, budget activation,
backend creation, staging inventory, ECR digests, and GO remain unfinished.

## Review questions

1. **EKS request and support gate.** Does removing the server-side status
   filter make the request valid while preserving exact-version and standard
   support checks locally? Inspect current and deprecated response fields,
   absent or unexpected data, and conflicting statuses. Does the regression
   fixture reject the original request, and do the tests establish failure
   for extended support, unsupported, missing status, and the wrong version?
   Distinguish pre-existing edge cases from regressions introduced here.

2. **RDS pin through its consumers.** Trace Terraform's version through the
   availability query, response matching, account receipt, and staging/GO
   validation. Check whether `17.11` is consistent everywhere operationally
   required, whether fixtures would catch drift, and whether any consumer
   still assumes `17.4`. Historical evidence may retain the old version.
   Determine whether the patch update changes any accepted topology or cost
   assumption; do not infer a current price or successful migration from the
   orderable-options response. Identify any additional check necessary before
   accepting the patch without broadening the infrastructure experiment.

3. **Evidence and execution order.** Are the ADR and plan status accurate about
   what ran, what was only reported, and what is still required? Check for
   contradictory candidate/review status near the changed text. Trace the
   proposed next steps from review and merge to a clean source SHA, rebuilt
   images, exact-candidate rehearsal receipts, preflight, backend and budget,
   cheap apply, image staging, expensive plan without apply, and GO. Separate
   actual enforced gates from procedural requirements. Do not classify a
   deliberately pending post-merge rehearsal as a defect in these fixes.

Keep recommendations proportionate. Recommend additional testing only where
it resolves a concrete uncovered failure. Do not extend or repeat a load
sample just to obtain a passing result.

## Suggested offline check

After inspecting the test effects:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover \
  -s scripts/tests -p test_m4_aws_account.py
```

Use small disposable reproductions if needed. Do not run broad Make targets,
container-backed checks, or live account checks under this review's authority.

## Required response

Lead with actionable findings ordered by severity. Each finding needs a current
file and line, triggering input or sequence, concrete consequence, supporting
evidence, and the smallest correction or test. Label new regressions,
pre-existing defects relevant to staging, and evidence limits separately.
Avoid style-only findings. If no actionable findings remain, say so explicitly.

Then report commands actually run, results, inspected scope, and limits. Give
separate verdicts for:

- Corrections: ACCEPT / REVISE.
- Candidate preparation: READY FOR REVIEWED COMMIT AND FRESH REHEARSALS /
  BLOCKED BY CODE OR DOCUMENTATION.
- AWS staging: list remaining prerequisites; review approval is not proof that
  they passed and does not add execution authority.

Return the review to the owner. Do not execute the staging plan.
