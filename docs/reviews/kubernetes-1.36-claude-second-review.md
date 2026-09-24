# Claude second review: Kubernetes 1.36 update

Reviewer: Claude Code (Opus 5.5), 2026-09-24. Requested by
[kubernetes-1.36-second-review-prompt.md](kubernetes-1.36-second-review-prompt.md).

## Scope reviewed

- Checkout `/Users/lilabrooks/code/my-local-platform`, branch `main`.
- `HEAD` and base: `c842e51b974ae97a1d346a06764e923e02d1afd8`.
- Scoped diff SHA-256, computed at the start and again at the end of the
  review: `deb12eb5d03a3eee298c6771249b4f1b185a2118494362c6fab1c11fc6bbc4ae`.
  It matches the prompt, so nothing drifted.
- Files reviewed: all 15 implementation files in full diff form (580 lines).
  Unchanged consumers read: `scripts/check-aws-plan.py` (EKS dependency review
  and shape gate), `scripts/aws-terraform-guard.sh` (support check and
  plan/apply paths), `scripts/m4-stage.py` `build_go_no_go`,
  `scripts/m4-aws-account.py` `availability`, `infra/terraform/envs/dev/outputs.tf`
  (`runtime_shape`), `scripts/validate-k8s-schema.sh`, `scripts/lint.sh` Trivy
  block, `.trivyignore.yaml`, the `k8s-up` and `keda-install` Makefile
  targets, `AGENTS.md`, `docs/application-validation.md`, ADR 0010's new
  sections, and the AWS runbook's infrastructure prerequisites.

A first hash attempt printed `e3b0c442…`, the SHA-256 of empty input. zsh does
not word-split an unquoted `$FILES`, so the diff was empty. Rerunning under
`bash -c` gave the expected hash. Anyone reproducing the hash from zsh will hit
the same thing.

## Findings

Ordered by severity. Findings 1 and 2 are confirmed defects in what the change
documents. Finding 3 is a confirmed missing binding. Findings 4 and 5 are
lower-risk gaps.

### 1. Medium: `make k8s-up` upgrades the existing `mlp` profile in place, contrary to the new runbook text

- **Where:** `Makefile:307` (`--kubernetes-version=v1.36.5`);
  `docs/runbook-local.md:119-124` (new paragraph); still-current resume
  instructions at `docs/runbook-local.md:569` ("back, with the pinned CPU,
  memory and version") and `README.md:86` ("Resume the cluster | `make k8s-up`").
- **Trigger:** the documented pause/resume path. The owner's `mlp` profile is
  stopped on v1.35.1 (`minikube profile list -o json`, checked this session).
  The next `make k8s-up` runs `minikube start -p mlp … --kubernetes-version=v1.36.5`.
- **Consequence:** passing a newer `--kubernetes-version` to an existing
  profile is minikube's upgrade mechanism. The installed minikube v1.39.0
  binary contains the message "Kubernetes {{.new}} is now available. If you
  would like to upgrade, specify: --kubernetes-version=…". The reverse is
  refused: "Unable to safely downgrade existing Kubernetes v{{.old}} cluster
  to v{{.new}}" (`K8S_DOWNGRADE_UNSUPPORTED`). So a routine resume performs an
  in-place control-plane upgrade of the cluster holding ArgoCD, KEDA and
  kube-prometheus-stack state. The rehearsal never exercised that path; it
  used a fresh profile. The only way back to 1.35 is `make k8s-delete`. The new
  paragraph says "The version update does not migrate existing local
  clusters." A reader will take that to mean resume leaves them on 1.35.
- **Evidence checked:** Makefile target, both runbook passages, README table,
  current profile versions, strings in the local minikube binary, minikube's
  downgrade behaviour as quoted in kubernetes/minikube#1201. **Not executed.**
  The prompt forbids starting existing clusters, so the upgrade itself was not
  run.
- **Smallest correction (doc-only):** replace the sentence with a statement
  that `make k8s-up` against an existing 1.35 profile upgrades it in place and
  cannot be reversed. Say that a clean 1.36 profile needs `make k8s-delete`
  then `make k8s-up`, and bootstrap again afterwards. Adjust the line-569 comment
  to match. Optional: expose `MINIKUBE_K8S_VERSION ?= v1.36.5` so an operator
  can resume on v1.35.1 deliberately.

### 2. Medium: KEDA 2.20.2 is outside KEDA's tested Kubernetes window, and the dependency review does not record it

- **Where:** ADR 0010 amendment (`docs/adr/0010-live-aws-relay-contract.md`,
  "Kubernetes version amendment, accepted 2026-09-23"); `Makefile:345`
  (`KEDA_VERSION ?= 2.20.2`).
- **Trigger:** the 1.36 move changes the Kubernetes minor under every
  in-cluster controller. The amendment reviews the four EKS add-ons and the
  worker AMI. It does not review the controllers the demonstration depends on.
- **Consequence:** KEDA's own matrix
  ([keda.sh/docs/2.20/operate/cluster](https://keda.sh/docs/2.20/operate/cluster/))
  lists v2.20 as tested on v1.33–v1.35. No released KEDA version lists 1.36 as
  tested; the docs mark 2.21 unreleased. KEDA drives the `relay-deliver` 1→12
  scaling at the centre of M4. For this combination there is now no
  compatibility listing and no demonstration, because the local rehearsal did
  not install KEDA. That is not evidence of incompatibility, since KEDA uses
  stable HPA and external-metrics APIs. It is a gap in the record.
  `AGENTS.md` ("record … their resolved versions") and
  `docs/application-validation.md:93` (scaling controllers: "versions and
  capacity" before apply) ask for that record.
  For comparison, the other controllers do have listings. Argo CD 3.5 lists
  v1.36 as tested
  ([release-3.5 installation](https://argo-cd.readthedocs.io/en/release-3.5/operator-manual/installation/)).
  kube-prometheus-stack 88.5.4 rendered with `--kube-version 1.36.0` without a
  `kubeVersion` rejection in `make k8s-validate`.
- **Smallest correction:** add a line to the amendment naming KEDA 2.20.2's
  tested window as an open dependency. Make KEDA install, ScaledObject
  reconciliation and HPA creation on a 1.36 cluster an explicit part of the
  fresh local qualification before staging. Alternatively, adopt a KEDA release
  that lists 1.36 once one exists.

### 3. Low: GO does not bind the account receipt's EKS version to the planned version

- **Where:** `scripts/m4-stage.py:956` checks only
  `identity.eks.standard_support is True`. `scripts/m4-stage.py:1007-1013`
  checks the plan shape against the literal `"1.36"`. Nothing compares
  `identity.eks.version` with `shape.eks.kubernetes_version`. RDS gets exactly
  that comparison at `:1014-1023`.
- **Trigger:** a future version change updates `variables.tf`, the tftest
  literal and `m4-stage.py`, but misses `EKS_VERSION` in
  `scripts/m4-aws-account.py:26` and that module's test fixtures. That is one of
  the three hand-edited constants in this diff.
- **Consequence:** the collector attests standard support for the old minor.
  GO still passes and records it beside a plan for the new minor. Offline tests
  stay green, because each test module hard-codes its own matching literal and
  no test ties the constants together. The live guard
  (`aws-terraform-guard.sh:233-236`, `check_eks_support`) independently checks
  the actual variable before plan and apply, so the wrong version would still
  be stopped before spend. The GO record the owner approves would just be
  wrong. An old 1.35 receipt cannot qualify this candidate: `require_bound`
  pins every receipt to the run ID and commit.
- **Reproduction:** a disposable test that subclassed `ImageStageTest`, wrote
  `eks = {"version": "1.35", "status": "STANDARD_SUPPORT", "standard_support": true}`
  into `01-identity.txt`, and called `build_go_no_go` with the 1.36 plan
  fixture. GO was built without error. The script lived in scratch space and
  is not part of the repository.
- **Smallest correction:** in `build_go_no_go`, require
  `eks.get("version") == shape["eks"]["kubernetes_version"]`, mirroring RDS.
  Add `"version": "1.36"` to the identity fixture and a mismatch test.

### 4. Low: the local qualification receipt does not record which Kubernetes version it ran on

- **Where:** `.evidence/m4-local/qualification-<commit>.json` (for example
  `qualification-474dca7.json`, inspected by key only). No field records a
  server or kubelet version. No script under `scripts/` reads or writes a
  cluster version.
- **Trigger:** the runbook requires a "newly qualified candidate" before
  staging (`docs/runbook-aws-relay.md:7`, `:76`). Given Finding 1, that run
  might happen on a fresh 1.36 profile, on an in-place-upgraded one, or on a
  1.35 profile started without the Makefile.
- **Consequence:** the receipt that is supposed to qualify the 1.36 candidate
  locally does not show that it did.
- **Smallest correction:** record `kubectl version -o json` `serverVersion`
  (and node `kubeletVersion`) in the qualification notes. The runbook step
  could also state the expected minor.

### 5. Low, pre-existing: the schema check passes silently when the schema version does not exist

- **Where:** `scripts/validate-k8s-schema.sh:67-72` (`-ignore-missing-schemas`,
  no minimum-valid assertion).
- **Trigger:** a future `KUBERNETES_VERSION` bump to a version with no published
  kubeconform schemas.
- **Reproduction:** `kubectl kustomize k8s/manifests/echo | docker run --rm -i
  <pinned kubeconform> -strict -summary -ignore-missing-schemas
  -kubernetes-version 1.99.0 -`. Output was `Valid: 0 … Skipped: 2` with exit 0;
  the same run with `1.36.0` gave `Valid: 2`.
- **Consequence for this diff:** none. 1.36.0 schemas exist, and the full run
  shows 104 valid. The check would not catch a bad version string, though,
  which is what Priority 5 asks about.
- **Smallest correction (follow-up, not this change):** fail when the summary
  reports `Valid: 0`, or drop `-ignore-missing-schemas` in favour of explicit
  `-skip` for the CRD kinds.

## Unanswered questions and known deferrals

- **Exact add-on builds.** I could not independently confirm
  `v1.36.0-eksbuild.25` or `v1.14.3-eksbuild.23`. My read-only
  `aws eks describe-addon-versions` call was blocked by this session's
  permission classifier. AWS's public pages list older builds as "latest":
  kube-proxy `v1.36.0-eksbuild.17` and CoreDNS `v1.14.3-eksbuild.16`, both on
  1.36. The base commit's 1.35 pins (`.29`, `.31`) were also newer than those
  pages, so the pages lag the API. That is consistent with Codex's API result
  but does not confirm it. The runbook already requires saving dated
  `describe-addon-versions` output at staging (`runbook-aws-relay.md:90-91`).
  That step settles this.
- **Worker runtime.** containerd's release matrix lists 2.3.0+ and 2.2.0+ for
  Kubernetes 1.36. The amazon-eks-ami v20260917 release notes, read through the
  newreleases.io mirror rather than GitHub directly, list containerd
  `2.2.7-1.amzn2023.0.1` for AL2023. Local minikube reported containerd 2.3.4
  (Codex's record). Both are inside the supported range on paper. Worker boot
  on 1.36 stays a live question, as the ADR says.
- **1.36 behaviour changes against the manifests.** The gitRepo volume is
  removed, `Service.spec.externalIPs` is deprecated, StrictIPCIDRValidation is
  on by default, and SELinux mount labelling defaults changed. A
  case-insensitive search of `k8s/` for `gitRepo`, `externalIPs`, `ipBlock`,
  CIDR fields and `seLinux` found no matches. The only CIDRs are Terraform
  (AWS API) inputs, which the Kubernetes validation does not govern. Rendered
  upstream charts were covered only by kubeconform, which does not evaluate
  these semantics.
- **Version skew on staging day.** The patch observed on EKS was 1.36.4, local
  is v1.36.5, and schemas target 1.36.0. Patch differences within one minor do
  not change API schemas, and the kube-proxy/kubelet skew policy allows ±3
  minors. I see no practical risk here. The local profile is simply not a
  byte-for-byte proxy for EKS.
- **Deferred live checks, correctly recorded by the ADR:** EKS worker boot and
  runtime, add-on readiness in the declared order, Pod Identity, three-worker
  pod capacity on the new AMI, and the full relay/KEDA/ArgoCD/telemetry
  demonstration. Findings 2 and 4 add one item: the local qualification on 1.36
  must include KEDA and record the version. `docs/application-validation.md:19`
  calls for local qualification before a live check, so this belongs to the
  free local step, not to the paid run.
- **Saved-plan gate scope.** `check-aws-plan.py` requires known add-on versions
  but does not bind kube-proxy's minor to the cluster minor. The new
  kube-proxy assertion lives only in the mocked tftest. The ADR phrases this
  accurately ("The mocked plan now also rejects…"). `runtime_shape`'s version
  comes from `var.eks_kubernetes_version` (`outputs.tf:74`), not from the
  planned `aws_eks_cluster.version`. Today the module input is that same
  variable (`expensive.tf:183`), so they cannot differ without an edit there. I
  am noting it, not raising it as a finding.

## Trace of the effective version

| Stage | Source of 1.36 | Result |
|---|---|---|
| Terraform input | `variables.tf:48` default | changed |
| Cluster | `expensive.tf:183` `kubernetes_version = var…` | follows input |
| Runtime output | `outputs.tf:74` from the same variable | follows input |
| Pre-plan/apply support check | `aws-terraform-guard.sh` reads the variable, then the saved summary | follows input; live |
| Account receipt | `m4-aws-account.py:26` literal | changed; not bound to plan (F3) |
| Saved-plan review | `check-aws-plan.py`: version not checked; add-ons need known versions | unchanged |
| GO | `m4-stage.py:1008` literal | changed; the new test rejects a 1.35 plan |
| kube-proxy pin | tftest prefix assertion | new; mutation-tested |
| Local cluster | `Makefile:307` | changed; in-place upgrade risk (F1) |
| Schema and Helm | `validate-k8s-schema.sh:8` one constant for both | changed |

A sweep with `git grep -nE '1\.35|v1\.35|1\.13\.2-eksbuild|1\.35\.3-eksbuild'`
(excluding `docs/reviews`) found no stale live constant. The remaining hits are
dated historical records (ADRs 0005, 0008 and 0009, ADR 0010's earlier
sections, `docs/evidence/m4-staging/20260920T155738Z`), the intended 1.35
rejection fixtures, and "1.35 GiB" in a memory table.

VPC CNI `before_compute`, Pod Identity `before_compute`, the CNI policy gate,
the three-worker shape and the abort/cleanup code are untouched by the diff.
The changed plan-checker fixtures keep every placement assertion.

## Commands run and results

| Command | Result |
|---|---|
| `bash -c 'git diff --binary c842e51… -- <15 files> \| shasum -a 256'` (start and end) | `deb12eb5…c4ae` both times |
| `python3 -m unittest scripts.tests.test_m4_stage scripts.tests.test_m4_aws_account scripts.tests.test_check_aws_plan` | 91 tests OK |
| `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts/tests` | 224 tests OK |
| `terraform init -backend=false`, `validate`, `test` in a scratch copy of `infra/` (dev only) | valid; 7/7 dev runs pass. Guardrails stack not rerun. |
| Mutant: kube-proxy pin back to `v1.35.3-eksbuild.29` | `live_runtime_matches_the_accepted_shape` fails with the new message |
| Mutant: CoreDNS back to `v1.13.2-eksbuild.31` | 7/7 pass. No offline check binds CoreDNS; its version does not encode the Kubernetes minor, so the tftest cannot either. |
| Mutant: `EKS_VERSION = "1.35"` in the account collector | `test_m4_aws_account` fails (6 failures, 2 errors) |
| Mutant: GO literal `"1.35"` | `test_m4_stage` fails, including `test_go_packet_rejects_previous_eks_minor`; baseline OK |
| Disposable GO test with a 1.35 identity receipt (F3) | GO built without error |
| `make k8s-validate` | Go invariants OK; `163 resources … Valid: 104, Invalid: 0, Errors: 0, Skipped: 59`; Tempo and Collector config checks passed (exit 0) |
| kubeconform on the echo root with `1.36.0` versus `1.99.0` (F5) | `Valid: 2` versus `Valid: 0, Skipped: 2`, both exit 0 |
| `docker run … markdownlint-cli2:v0.23.2` on the five changed docs | 0 issues |
| `terraform fmt -check -recursive infra/terraform/envs/dev` | clean |
| `aws eks describe-cluster-versions` / `describe-addon-versions` (read-only) | **not run**: blocked by the session permission classifier |
| Pinned Trivy scans (see below) | as reported below |

The mutation copies, the Terraform copy and the cloned Trivy cache were
deleted after use. No implementation file, cluster, AWS resource or backlog
item was changed.

## Lint disposition

I reproduced the unresolved Trivy finding and located it. The failure is
unrelated to this diff, and the alternate scan preserves the checks this diff
needs.

I ran pinned Trivy 0.74.0 against a copy-on-write clone of the complete pinned
cache (580 `.rego`, metadata digest `sha256:1583562f…`), with lint.sh's exact
options (`--scanners vuln,misconfig,secret`, `.trivyignore.yaml`,
`MEDIUM,HIGH,CRITICAL`, `--skip-dirs '**/.terraform'`):

1. Working tree with `.claude` skipped: exit 1, one finding. AWS-0132 on target
   `main.tf`, **type `terraformplan`**, resource
   `aws_s3_bucket_server_side_encryption_configuration.state`. The ten
   KSV-0125 findings therefore come only from `.claude/worktrees`. The
   path-scoped entries in `.trivyignore.yaml` do not match nested copies.
2. `.evidence` alone: the same AWS-0132 `terraformplan` finding.
3. That one file, scanned in isolation:
   `.evidence/worktree-archives/mlp-m4-96-d63d028/.evidence/m4/20260920T050446Z/bootstrap-plan-private.json`
   reproduces it. It is an archived bootstrap plan in a git-ignored
   directory. Trivy scans saved plan JSON as `terraformplan`, where the
   source's `#trivy:ignore:AWS-0132` comments do not apply. I did not print
   the file's contents, and the scratch copy was deleted.
4. Working tree with both `.claude` and `.evidence` skipped: **exit 0**. That
   scan covers every file in the working tree, tracked or not, including this
   diff and the untracked review documents. It is broader than Codex's
   tracked-files copy, and it agrees with it.

The diff changes only version strings and comments in Terraform. None of them
touch S3 or pod specs, so it cannot affect either finding. Full-workspace
`make lint` stays red on this host until ignored local evidence and worktrees
are excluded from the scan, for example by skipping git-ignored paths. That is
a lint-framework change and is out of scope here. It is not a waiver of Trivy
for this diff. By inference, not checked, a CI checkout has neither directory
and would not see these findings. Markdown now passes on the changed files.

## Verdict

**Ready to commit: yes, after two doc-only corrections.** Fix Finding 1, so the
runbook stops saying existing clusters are not migrated when the documented
resume path upgrades them. Fix Finding 2, so KEDA's untested 1.36 window is
recorded as an open dependency. The code changes are correct and consistent. I
found no stale version constant. Tests, schema validation, Terraform tests and
lint all hold for this diff under the reproduced checks. Findings 3–5 can be
follow-ups. Finding 3 is a one-line hardening worth doing before the next
staging run.

**Ready to stage or execute on AWS: no.** All of the following must come
first:

- a fresh local qualification of the new commit on a 1.36 profile, including
  KEDA scaling and recording the cluster version (Findings 2 and 4);
- a refreshed, saved `describe-addon-versions` and AMI observation for the
  staged candidate;
- new staging and GO receipts;
- a separate owner approval for the paid run.

The ADR already names the live questions that follow (worker boot on the new
AMI, add-on readiness order, Pod Identity, pod capacity, the full demo).
Existing approvals do not carry over, and the ADR and runbook say so correctly.
