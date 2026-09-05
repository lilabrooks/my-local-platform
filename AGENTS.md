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
make lint          # lint, docs, actions, Docker, Terraform, security, secrets
make test          # Go tests across all five modules
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

**Every image, module and GitHub Action is pinned.** Floating `:latest` is how a
local stack rots. Verify a tag exists before pinning it.

Actions are pinned to a **full commit SHA**, with the human-readable version in
a trailing comment:

```yaml
- uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
```

A tag is not a pin. `actions/checkout@v7` is a branch-like ref the publisher can
move, so CI behaviour can change with no commit in this repository — which is
the same argument this convention already makes for images. Dependabot's
`github-actions` group updates the SHA and the comment together, so pinning does
not mean going stale.

**ADRs record evidence, not intent.** Each `docs/adr/*.md` has a Verification
section naming the command that produced the result. A claim that cannot be
checked from the repository is one a reviewer has to re-derive. If you change
a decision, update its ADR and say what you actually ran.

**A condition that closes work names the evidence, not a string.** When an issue
carries an acceptance criterion for an ADR, write it as "a dated command, the
topology, and the measured result" — or, if the answer turns out to be no, "an
explicit deferral with an observable revisit condition". Do not write "the
heading `Still planned` no longer appears in `docs/adr/`".

That criterion was real, on [#73](https://github.com/lilabrooks/my-local-platform/issues/73),
and it did no harm: the work was done and the heading went with it. The problem
is that it could have been satisfied by a rename, and it does not assert the
thing anyone cares about. A condition standing in for the evidence is a
condition that can come apart from it.

This is the same defect as an unobservable backlog trigger. Issue
[#21](https://github.com/lilabrooks/my-local-platform/issues/21) needed three
attempts before its trigger admitted that no existing signal could surface the
condition. Both are conditions written to sound checkable rather than to be the
thing being checked.

**A document that states intent carries a status line, and the commit that
fulfils it updates that line.** `docs/goal-relay.md` and
`docs/roadmap-relay.md` are written before the work and describe what is
supposed to happen. That is what makes them worth having, and it is also what
makes them wrong the moment the work lands.

An audit on 2026-08-27 found six of these at once: a roadmap header still
reading "Proposed — no code written" with three milestones built, two exit
criteria unmarked while a third said "met", a goal document still marked
Proposed, and a README claiming eight CI jobs against thirteen and describing
linter behaviour that had been replaced two days earlier. Each was correct when
written. None survived the work it described.

Two rules, because they pull in opposite directions and both matter:

- **Update the status, not the substance.** A goal rewritten in the past tense
  to match its outcome cannot be used to judge that outcome. The value of a
  prediction is that it was made first. Mark it built; leave what it predicted
  alone.
- **When you delete a section, grep for what pointed at it.** Removing the
  backlog's Resolved section left three links in the roadmap aimed at nothing,
  one of them announcing a move to a section that no longer existed.

**Checks write and read back.** `services/smoke` never just opens a connection —
it round-trips a payload and asserts it matches. A check that only connects
proves a port is listening, which is a different claim.

**Placeholders in Kubernetes manifests follow one rule:**

| Directory | Applied by | `__REPO_URL__` |
|---|---|---|
| `k8s/argocd/` | `install.sh`, which substitutes | works |
| `k8s/apps/` | ArgoCD, read from git | must be a real URL |

**ArgoCD control and workload permissions stay separate.** The root Application
uses `mlp-root`, which can create only `Application` objects in `argocd`.
Children use `mlp`, which can deploy only into namespace `mlp`; the built-in
`default` project is disabled. `install.sh` and `repo-creds.sh` apply the root
project, root Application, workload project, and default project in that order
so an existing cluster can migrate without stranding the root Application.
`k8s/validate` tests the boundary. See ADR 0009.

**Metrics libraries are a per-service call, not a repository default.**
`services/relay` uses `prometheus/client_golang` because it needs a latency
histogram, labelled counters and per-partition gauges — bucket arithmetic and
label handling are what the library is for, and those numbers are the evidence
the M2 demo rests on. `services/echo` and `services/sink` hand-write the
exposition format and stay standard-library-only, which is what keeps them on
`scratch` with a fast build. Do not "unify" these; each is written down where it
is, and the split is the decision.

**Consumer lag is measured once, by `relay-ingest`, from the broker.** Not by
the consumers. A deliver pod knows only its own partitions, KEDA moves that
group between one and twelve members, and the sum of appearing-and-vanishing
per-pod series is least trustworthy exactly while the demo is being watched.
Reading the group's committed offsets from the broker is also where KEDA reads
them, so the panel and the scaler cannot disagree.

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

## Work selection and backlog authority

GitHub Issues and milestones are the backlog authority. Issue
[#84](https://github.com/lilabrooks/my-local-platform/issues/84) records the
migration from the retired repository document. Git history retains that file
and its earlier deferral rationale.

When no issue is named, inspect the current milestone, open issues, stated
dependencies, owner gates, and triggers. Recommend the next item and explain
the choice. GitHub mutations require separate authority.

Each planned issue starts with these fields:

- **Stable ID:** authority-independent identity used across migrations and
  cross-issue relations.
- **Outcome and Trigger:** the result sought and the observable condition that
  schedules it. Use `none` when work is ready or waits only for an owner.
- **Decision owner:** `none` for an implementation decision already made, or
  the role that must choose between materially different outcomes.
- **Governing anchors and Seam:** the documents and code boundary the work must
  keep consistent.
- **Relation and Evidence:** machine-readable dependency shape and the checked
  facts that justify the issue.

Use exactly one `state:*` label and one `kind:*` label. `state:ready` means no
unresolved wait blocker. `state:waiting` carries at least one of
`wait:trigger`, `wait:owner`, or `wait:dependency`. `horizon:later` keeps
deliberately unscheduled work outside the governed milestone sequence. Keep
component labels such as `relay` and classifications such as `security` where
they help searches.

**End the commit or PR that resolves an item with `Closes #N`.** GitHub closes
the issue on merge, and that is the only part of this arrangement that does not
depend on someone remembering to do it.

Milestones group issues by roadmap stage. The roadmap document holds the detail;
a milestone is a pointer to it, never a second copy. Roadmaps may describe
unscheduled options; an option enters the backlog when an issue is filed.

The Kafka smoke check defect was fixed in M0. Its measurements are in
[ADR 0004](docs/adr/0004-real-kafka-not-emulated.md#verification), which is
where resolved work's durable evidence belongs.

## Secrets

Never commit credentials. `make lint` runs `gitleaks` over full history. The
account id is deliberately absent from tracked files. `.env`, `*.tfstate` and
agent caches are gitignored — check `git status` before `git add -A`.
