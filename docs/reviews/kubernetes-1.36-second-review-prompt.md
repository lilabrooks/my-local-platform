# Claude second review: Kubernetes 1.36 update

Independently review the uncommitted Kubernetes 1.36 update in
`/Users/lilabrooks/code/my-local-platform`. Determine whether it is correct
and ready to commit as a repository update, and whether the evidence supports
the claims made. Do not implement fixes or launch a paid rehearsal.

The owner asked to implement the move from EKS 1.35 to 1.36 before the next
authorized AWS run. The local pin, validation target, staging checks and
documentation were updated as part of that change. Passing tests and the
implementation summary below are claims to assess, not proof of correctness.

## Exact scope

- Checkout: `/Users/lilabrooks/code/my-local-platform`, branch `main`.
- Base commit: `c842e51b974ae97a1d346a06764e923e02d1afd8`.
- Review the working-tree contents, including uncommitted edits. A new
  worktree created from HEAD alone would omit the change.
- The scoped binary diff's SHA-256 is `deb12eb5d03a3eee298c6771249b4f1b185a2118494362c6fab1c11fc6bbc4ae`.
- Record the base, diff hash and files you actually reviewed. If they have
  changed, identify the drift before reaching a verdict.

The 15 implementation files are:

```text
Makefile
infra/terraform/envs/dev/variables.tf
infra/terraform/envs/dev/expensive.tf
infra/terraform/envs/dev/tests/runtime.tftest.hcl
scripts/m4-aws-account.py
scripts/m4-stage.py
scripts/validate-k8s-schema.sh
scripts/tests/test_check_aws_plan.py
scripts/tests/test_m4_aws_account.py
scripts/tests/test_m4_stage.py
docs/adr/0010-live-aws-relay-contract.md
docs/costs.md
docs/repository-file-checks.md
docs/runbook-aws-relay.md
docs/runbook-local.md
```

The hash was calculated from `git diff --binary BASE -- FILES`, with the base
and file order above. Untracked review documents, including this prompt, are
outside the implementation diff. Preserve other people's work. Read the
current `AGENTS.md`, `docs/application-validation.md`, ADR 0010 and the AWS
runbook's infrastructure prerequisites before judging the change.

## Changes to examine

- Terraform's EKS default, the account collector's version and GO's fixed
  runtime shape move to 1.36.
- kube-proxy moves to `v1.36.0-eksbuild.25`; CoreDNS moves to
  `v1.14.3-eksbuild.23`. VPC CNI stays `v1.22.4-eksbuild.3` and the Pod
  Identity agent stays `v1.3.10-eksbuild.3`. EKS module 21.25.3, creation
  ordering, IAM permissions, worker count and spend limits are unchanged.
- New local profiles use upstream Kubernetes v1.36.5. Kubeconform uses
  1.36.0 schemas, and the validation script passes the same version to Helm.
- Tests exercise the new version, reject a 1.35 GO plan and require the
  kube-proxy pin to match the EKS minor in the mocked Terraform plan.
- ADR 0010 records the decision and dated evidence. Historical 1.35 receipts
  remain unchanged; new qualification, staging and GO receipts are required.

## Review priorities

1. Trace the effective version from Terraform input through runtime outputs,
   account availability evidence, saved-plan review, staging/GO and the
   deployment consumer. Look for stale constants, overrides, missing version
   bindings, and ways an old receipt could falsely qualify the new candidate.
   Inspect relevant unchanged consumers as well as the edited lines.
2. Check the exact EKS add-on builds and worker-image assumptions against
   current primary sources or read-only AWS APIs. Assess the significance of
   local v1.36.5, observed EKS patch 1.36.4 and schemas for 1.36.0. Distinguish
   a compatibility listing from demonstrated readiness. Review the relevant
   1.36 changes, including container runtime requirements, volume behavior
   and IP/CIDR validation, against the actual manifests and dependencies.
3. Check that VPC CNI placement, CNI IAM policy, CoreDNS, kube-proxy and Pod
   Identity remain declared and ordered correctly. The preceding AWS attempt
   failed because networking prerequisites were absent. Three-worker pod
   capacity and the existing abort/cleanup contract must remain intact.
4. Trace the local pin through `make k8s-up`, image loading, deployment and
   schema/Helm validation. Check what happens when an operator invokes the
   target against an existing stopped 1.35 profile. Do the instructions
   accurately describe migration, fresh profiles and context handling?
5. Assess whether the tests would catch a missed version update or an
   incompatible dependency, rather than merely restating fixture values.
   Identify concrete gaps, with a failing scenario or minimal reproduction.
6. Judge the evidence at its stated scope. An echo rehearsal and mocked
   Terraform tests do not establish the full relay/KEDA/ArgoCD/telemetry
   demonstration or EKS behavior. Decide whether the recorded deferrals are
   sufficient for committing this update and what must still precede staging
   or paid execution. Check historical records and status claims for drift.
7. Review the lint limitation below. Establish whether it is unrelated to
   these edits and whether the alternate scan preserves the checks needed
   for this diff. Do not silently waive a required gate or expand the change
   into a lint-framework repair.

## Evidence available for review

The detailed record is ADR 0010's **Kubernetes version amendment, accepted
2026-09-23** and **Kubernetes 1.36 update, 2026-09-23** sections. Codex reports:

- AWS read-only queries on 2026-09-23 confirmed EKS 1.36 standard support
  until 2027-08-02 00:00 UTC, compatibility of all four add-on pins and AL2023
  x86_64 worker release `1.36.4-20260917` in `us-east-1`.
- `make test` passed Go race tests and 224 Python tests.
- `make terraform-check` passed validation and eight mocked tests. The dev
  tests passed after adding the kube-proxy assertion. Restoring the old
  kube-proxy pin in a disposable copy failed that assertion.
- `make k8s-validate` passed. The schema result was 163 resources: 104 valid,
  zero invalid/errors and 59 skipped custom resources. The Helm version
  change was checked by rerunning `scripts/validate-k8s-schema.sh`.
- A temporary Docker/minikube cluster, four CPUs and 6 GiB, ran v1.36.5 with
  containerd 2.3.4. Two echo replicas became ready; a ClusterIP request
  returned the exact requested path. A pod handled SIGTERM, logged shutdown
  and was replaced; the request passed again. This did not test workload DNS
  resolution, the complete relay stack or AWS. The temporary cluster was
  deleted; the original `mlp` and `minikube` profiles remained stopped on
  v1.35.1.
- Full-workspace `make lint` remains failing. An incomplete pinned Trivy
  cache was restored. Its full scan reported 11 findings: ten KSV-0125
  findings under two ignored `.claude/worktrees` directories and one
  AWS-0132 finding reported as `main.tf`, whose precise origin was not
  resolved. The same scanner/options on a temporary copy of current tracked
  file contents returned zero findings across 50 result targets. All other
  lint checks passed after fixing and separately rechecking Markdown.

Local logs may still be available at `/private/tmp/mlp-k136-tests.log`,
`/private/tmp/mlp-k136-lint.log` and
`/private/tmp/mlp-k136-lint-final.log`. The last lint log retains the Markdown
failure that was subsequently fixed; a separate pinned Markdown run passed.
The temporary cluster no longer exists. Do not claim to have reproduced its
results from these summaries alone.

## Boundaries and response

This is a review request. Do not edit implementation files, commit, push,
merge, mutate the backlog, create AWS resources or change account settings.
Use focused offline tests, disposable copies and read-only queries as needed.
Do not start or upgrade existing local clusters. If an additional live
rehearsal is essential, describe the unresolved question and proposed scope
instead of running it. Existing paid-run and staging approvals do not carry
over to this changed candidate.

Write the review to
`docs/reviews/kubernetes-1.36-claude-second-review.md`, and summarize it in
your final response. Lead with actionable findings ordered by severity.
For each, name the file and line, trigger, consequence, checked evidence and
smallest correction. Separate confirmed defects from unanswered questions
and known deferred live checks. Include commands actually run and their
results, the lint disposition, and a verdict on readiness to commit versus
readiness to stage or execute on AWS. If no actionable findings remain, say
so explicitly; do not invent issues to fill the review.
