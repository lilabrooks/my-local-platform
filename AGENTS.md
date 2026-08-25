# Working in this repository

Shared instructions for coding agents (Codex, Claude Code, and anything else
that reads `AGENTS.md`). Claude Code also reads `CLAUDE.md`, which points here.

## What this is

A local-first distributed-systems platform. Kafka, RabbitMQ, Postgres, an AWS
API surface (floci), and an OpenTelemetry stack run in docker-compose. ArgoCD
deploys to a local minikube profile from git. Real AWS is opt-in and ephemeral,
behind Terraform flags that default to `false`.

Start with the [README](README.md), then `docs/adr/` for why each choice was
made and what evidence backs it.

## Cost guardrails — read before touching `infra/terraform/`

This bills a personal AWS account. Two rules:

1. **Never run `terraform apply` or `make aws-up` without explicit permission.**
   The default tier is ~$0/month, but `enable_rds` (~$15/mo) and `enable_eks`
   (~$110/mo) create real hourly charges.
2. **The EKS `kubernetes_version` must be in STANDARD support.** A version in
   extended support bills at $0.60/cluster-hour instead of $0.10 — $438/month
   rather than $73, applied automatically with no approval step. Check first:

   ```bash
   aws eks describe-cluster-versions \
     --query 'clusterVersions[?status==`STANDARD_SUPPORT`].clusterVersion'
   ```

`make aws-cost` shows month-to-date spend. Everything Terraform creates is
tagged `Project=my-local-platform`, so nothing left running can hide.

## Finding code

1. **`codegraph_explore`** first — `.codegraph/` is indexed here. One call
   returns verbatim source plus call paths and blast radius.
2. **`semble` search** when you cannot name the symbol and are searching by
   intent.
3. **grep** only for exhaustive literal sweeps, such as every caller of a name
   being renamed.

`token-savior` is registered for this project as `my-local-platform`. Prefer it
for questions about consequence rather than location: `get_change_impact`,
`find_impacted_test_files`, `detect_breaking_changes`, `get_routes`,
`get_env_usage`. Do not use its editing tools — use `Edit`/`Write`, which the
harness tracks.

## Verifying a change

Run what the change touches. All of these are free and local:

```bash
make lint          # 10 checks: go, yaml, shell, md, actions, docker, tf, security, secrets
make test          # Go tests across all three modules
make k8s-validate  # manifest invariants
make smoke         # end-to-end against the running local stack
```

Two linting details that will waste your time otherwise:

- **`trivy` ignore directives must be the LAST comment line before a resource.**
  Prose between the directive and the resource silently disables it, as does a
  reason that wraps onto a second line. The directive also has to sit on the
  resource trivy anchors the finding to, which for S3 encryption is the
  `aws_s3_bucket_server_side_encryption_configuration`, not the bucket.
- **Do not turn on errcheck's `check-blank`.** It flags `_ = x.Close()`, which
  is the explicit-discard pattern this repo uses deliberately. Tried, reverted.

Accepted security findings are annotated inline with the reason. If you add a
`#trivy:ignore:`, say why in the comment above it -- an unexplained suppression
is worse than the finding.

`make smoke` needs the stack up (`make up`). CI runs all of these.

## Conventions that are load-bearing

**Every image and module version is pinned.** Floating `:latest` is how a local
stack rots. Verify a tag exists before pinning it.

**ADRs record evidence, not intent.** Each `docs/adr/*.md` has a Verification
section naming the command that produced the result. A claim that cannot be
checked from the repository is one a reviewer has to re-derive. If you change
a decision, update its ADR and say what you actually ran.

**Checks write and read back.** `services/smoke` never just opens a connection —
it round-trips a payload and asserts it matches. A check that only connects
proves a port is listening, which is a different claim.

**Placeholders in Kubernetes manifests follow one rule:**

| Directory | Applied by | `__REPO_URL__` |
|---|---|---|
| `k8s/argocd/` | `install.sh`, which substitutes | works |
| `k8s/apps/` | ArgoCD, read from git | must be a real URL |

**Do not put a mutable label in a Deployment selector.** Selectors are immutable
after creation, so it applies once and fails forever after. `k8s/validate`
tests this; run it with `-count=1`, because Go's test cache does not track the
YAML those tests read.

## Things that will bite you

- **floci drops privileges to uid 1001** even started as root, and defaults to
  in-memory storage. Both are handled in `local/docker-compose.yml`; do not
  "simplify" them away.
- **The OTel collector validates every *defined* exporter at startup**, not only
  those a pipeline uses. That is why Datadog lives in a separate config file
  rather than behind a comment.
- **`kubectl apply` fails on the ArgoCD manifests** — the ApplicationSet CRD
  exceeds the annotation size limit. `--server-side` is required.
- **`unset AWS_PROFILE`** before pointing the AWS CLI at the local emulator, or
  a real SSO profile shadows the fake credentials.
- **Real AWS requires `MLP_USE_REAL_AWS=1`.** An empty `AWS_ENDPOINT_URL` means
  local, deliberately, so a stray `export` cannot hit a live account.

## Known defects and tracking

[docs/backlog.md](docs/backlog.md) lists deferred work with the reason it was
deferred and what "done" means. Read it before assuming a failure is new.

GitHub Issues are the tracker; backlog.md is the copy that survives without
network access and that agents read. Two records of the same thing drift unless
something holds them together — issue #1 sat open for a while after backlog.md
called it resolved — so:

**End the commit or PR that resolves an item with `Closes #N`.** GitHub closes
the issue on merge, and that is the only part of this arrangement that does not
depend on someone remembering to do it.

Milestones group issues by roadmap stage. The roadmap document holds the detail;
a milestone is a pointer to it, never a second copy.

The Kafka smoke check defect that used to be described here was fixed in M0. Its
measurements are in
[ADR 0004](docs/adr/0004-real-kafka-not-emulated.md#verification).

## Secrets

Never commit credentials. `make lint` runs `gitleaks` over full history. The
account id is deliberately absent from tracked files. `.env`, `*.tfstate` and
agent caches are gitignored — check `git status` before `git add -A`.
