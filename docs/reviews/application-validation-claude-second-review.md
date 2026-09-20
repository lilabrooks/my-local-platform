# Claude second review: application validation guidance

Date: 2026-09-20
Reviewer: Claude Opus 5 (read-only review; no candidate file was modified)
Verdict: **REVISE**

Scope: `docs/application-validation.md` (new, untracked), and the working-tree
diffs to `README.md` and `AGENTS.md`. Source identity was recorded before and
after the review and is unchanged; all three SHA-256 hashes match the handoff
table exactly. See [Source identity](#source-identity).

Most of this patch is sound. The opening trigger is well chosen, the
proportionality language is deliberate, the reuse section draws the right line
between a tested transport and an untested integration, and the final paragraph
preserves #97 accurately against ADR 0010. The findings below are concentrated
in one place: what happens when a second application shares the AWS account
with relay. The guide invites that case and then describes the existing
teardown machinery incorrectly. Every correction proposed here is one or two
sentences of prose; none requires new tooling, a new document, or a new ADR.

---

## Findings

Ordered by severity. All are **introduced defects** in this patch unless marked
otherwise. Line numbers refer to the reviewed bytes.

### F1 — Cleanup verification is described as session-scoped; the tooling is project- and region-scoped, and another app's retained resources make it fail

**File/line:** `docs/application-validation.md:87-91`

**Triggering scenario:** A second application is deployed alongside relay in the
same AWS account, "owning its own data and identities" (the guide's own
coexistence case, line 90). Its resources are tagged `Project=my-local-platform`
— which ADR 0001:37-38 states is applied to *everything* Terraform creates here
— or carry `mlp-` in a name. Relay's #97 session then runs and destroys
correctly.

**Concrete consequence:** Relay's cleanup verification returns non-zero and the
run records `cleanup_failed` even though relay destroyed everything it owns.
The guide's mitigation at lines 90-91 addresses only the opposite direction
(cleanup deleting the other app's resources), so an implementer who follows it
exactly is still blocked.

**Evidence:**

- `scripts/m4-aws-inventory.py:136-147` — `project_tagged()` matches any
  `Project=my-local-platform` tag, any `kubernetes.io/cluster/mlp-*` tag, or any
  `Name` containing `mlp-`; `project_name()` is a substring test, not a prefix
  test, despite `NAME_PREFIX`.
- `scripts/m4-aws-inventory.py:161-262` — `collect()` queries the whole region
  per service and filters only by that project scope. Nothing narrows it to a
  run id or a session.
- `scripts/m4-aws-inventory.py:20-29, 255-262` — `runtime_empty` is false when
  *any* of `eks, msk, rds, ec2, ebs, eip, elb, nat, logs` is non-empty;
  `require_cleanup_inventory()` additionally folds ECR into the failure set for
  `21-inventory-after.json`.
- `Makefile:517-526` — `aws-inventory-empty` passes `--require-no-runtime`.
- `tools/m4-live-run/main.go:1016-1034, 1053, 1063` — the controller retries
  that command and sets `CleanupVerified=false` when it does not reach 0.
- ADR 0010:399-402 — empty inventories are a closure requirement for #97.

**Smallest correction:** At line 87-88, replace "inventories covering the
session's resources" with a phrase that matches the tool — for example
"inventories covering every project-scoped resource in the region". At lines
90-91, add the second direction: "The current inventory gate also *fails* on
another application's retained project-scoped resources, so a coexisting app
needs its own tag or name scope before relay's cleanup can verify."

---

### F2 — The shared $5 monthly project budget is never mentioned, and a second app's paid run can block relay's remaining gate

**File/line:** `docs/application-validation.md:84-92`

**Triggering scenario:** A second application runs its own authorized paid AWS
session under the guide's step 2. Its spend lands under the same
`Project=my-local-platform` tag. Relay's #97 is authorized later in the same
calendar month.

**Concrete consequence:** If the second app's spend pushes actual past $4 (80%)
or the forecast past $5, a budget notification leaves `OK`, and relay's hourly
plan and apply are refused until AWS returns it to `OK` or the month resets.
The guide's only shared-environment paragraph (lines 90-92) covers resource
ownership and teardown, not the shared allowance, so an owner reading only this
guide has no reason to sequence the two sessions.

**Evidence:**

- `scripts/m4-aws-account.py:354-365` — one `MONTHLY` `COST` budget with a $5
  limit and `CostFilters` of exactly `Project=my-local-platform`. It is
  project-wide, not per-application and not per-run.
- `scripts/m4-aws-account.py:417-418` — `budget()` raises
  `AccountError("AWS budget notifications are not all OK")` on any non-`OK`
  notification state.
- `scripts/aws-terraform-guard.sh:36-43` — `check_budget()` runs
  `m4-aws-account.py --budget-only` immediately before every hourly plan/apply.
- `docs/costs.md:279-286` — "One run can put the forecast notification in
  `ALARM`, which blocks later hourly plans ... Raising the monthly amount
  requires a new owner decision."
- ADR 0010:313-316 — the budget scope was deliberately made project-wide;
  widening it for other project work is called out as a decision to revisit,
  not something a guide can assume.

**Smallest correction:** Add one sentence after line 92: "The $5 monthly
allowance is project-wide and shared by every application here. A paid session
for one app consumes it and can block another's hourly plan for the rest of the
month, so sequence paid sessions and confirm the budget notifications are `OK`
before authorizing one."

---

### F3 — "This follows ADR 0001" attaches ADR authority to a rule ADR 0001 does not contain

**File/line:** `docs/application-validation.md:7-10`

**Triggering scenario:** An implementer or reviewer traces the per-application
local-rehearsal mandate (line 7) to ADR 0001 to see what evidence supports it.

**Concrete consequence:** ADR 0001 is entirely about where infrastructure runs
and what it costs. It says nothing about per-application validation, rehearsals,
or evidence records. Sentence 2 (live AWS when local cannot supply the evidence)
does follow from it; sentence 1 does not. Presenting new policy as an existing
decision is the failure mode AGENTS.md:110-113 exists to prevent — "ADRs record
evidence, not intent ... A claim that cannot be checked from the repository is
one a reviewer has to re-derive."

**Evidence:** `docs/adr/0001-local-first-with-ephemeral-aws.md:25-48` (Decision
and Consequences) — the closest statement is line 43-45, "Anything that depends
on real IAM semantics, real SES sending limits, or real EKS networking has to be
verified against the cheap tier or a temporary expensive-tier apply." No clause
concerns application rehearsals.

**Smallest correction:** Move the attribution so it governs only the AWS
sentence: "... works on AWS, which is the verification ADR 0001 already requires
for behavior local emulation cannot reproduce. The local rehearsal below is new
repository guidance."

---

### F4 — ADR 0001's cheap/expensive tier split is flattened, so a cheap-tier-only check is routed through relay's expensive-tier ceremony

**File/line:** `docs/application-validation.md:18, 76-88`

**Triggering scenario:** The prompt's third case — an app using only S3 that
needs to verify its real IAM permissions. This is the exact case ADR 0001:43-45
names, and S3 sits in the cheap tier that ADR 0001:30-33 calls "serverless and
pay-per-request, safe to leave standing."

**Concrete consequence:** The guide's only AWS path is the #96/#97 two-step.
Step 1 requires the implementer to "stage immutable images" and "review the
deployment and infrastructure plan" and check "availability, support versions" —
none of which exist for an S3/IAM check; the availability and support-version
gates in the tooling are MSK-region and EKS-version checks specifically
(`scripts/m4-aws-account.py:43-77`). Step 2 is titled "the **paid** session" and
mandates destroy plus inventory verification, which contradicts ADR 0001's
statement that the cheap tier is safe to leave standing. An implementer either
performs inapplicable ceremony or silently departs from documented guidance.

**Evidence:** `docs/adr/0001-local-first-with-ephemeral-aws.md:27-35` (two named
tiers); `docs/costs.md:81-96` (cheap tier, ~$0 idle) versus `docs/costs.md:97-111`
(expensive tier, the $1.25/hour gate and destroy deadlines); ADR 0010:14-17 —
the three-way split of "contract approval, cheap-tier staging, and the later
hourly apply" is the existing structure, and the guide collapses it to two.

**Smallest correction:** One sentence before line 80: "A question answerable in
the cheap tier (S3, SNS, SQS, SES, ECR) needs the identity, permission, and cost
checks below, but not immutable images, an hourly deadline, or a destroy gate;
ADR 0001 leaves those resources standing at ~$0. The full two-step below applies
to the expensive tier."

---

### F5 — "limits" is required but unnamed, so a new app's paid session has no mandated deadline or spend cap

**File/line:** `docs/application-validation.md:84-88`

**Triggering scenario:** A second application's first paid AWS session. The owner
approves "the reviewed scope, limits, and abort procedure" as line 85 requires,
without a checklist of what a limit must cover.

**Concrete consequence:** Nothing in the guide requires a wall-clock deadline or
a maximum spend. An implementer can debug on live hourly resources for as long
as the session lasts and remain compliant with the guide as written. This is the
specific hazard ADR 0010 identified: decision driver 4 says the contract must
"bound the paid experiment with resource counts and elapsed time, since an AWS
Budgets alert cannot stop a short session." Relay is protected by ADR 0010; a
new app reading this guide is not.

**Evidence:** ADR 0010:27-32 (decision drivers 4 and 5);
`docs/runbook-aws-relay.md:85-91` (150-minute destroy deadline, 3-hour hard
deadline, $1.25/hour shape cap, $5.00 maximum, "Do not extend the deadline");
`docs/costs.md:105-111`.

**Smallest correction:** At line 85, replace "the reviewed scope, limits, and
abort procedure" with "the reviewed scope, an elapsed-time destroy deadline, a
maximum spend, and the abort procedure".

---

### F6 — The terminal condition for a *failed* demonstration is stated only for #97, not for the general case

**File/line:** `docs/application-validation.md:87-96`, contrast `:100-103`

**Triggering scenario:** A new application's paid session produces a failed
demonstration. Destroy succeeds, the inventories come back empty, and the cost
receipt settles.

**Concrete consequence:** Lines 87-88 make destroy the terminal step on success
or failure; lines 94-96 separate technical completion from billing. A reader can
assemble "clean teardown plus settled cost" into closure. The rule that makes
this safe is stated only for relay at lines 100-103. ADR 0010:399-402 states it
generally — "Overdue cleanup or a controller evidence error cannot publish as a
full pass, even when resources are gone" — and ADR 0010:391-393 says a recovered
run's "demonstration remains failed." The guide does not carry that forward.

**Evidence:** ADR 0010:386-402; `docs/application-validation.md:100-103` is
relay-scoped ("This guide does not relax #97").

**Smallest correction:** Add to line 96: "A failed demonstration stays failed. A
clean teardown and a settled bill close the *session*, not the claim it was run
to establish."

---

### F7 — "Automated checks **and** an end-to-end local rehearsal" does not say whether an existing automated check can be the rehearsal

**File/line:** `docs/application-validation.md:16`

**Triggering scenario:** The prompt's first case — a small Compose-only app using
Postgres that already has an automated round-trip test.

**Concrete consequence:** Two competent implementers read this differently. One
treats the existing `make smoke`-style test as the rehearsal and records its
dated output. The other reads the conjunction as two deliverables and performs a
separate manual ceremony on top. The rest of the section supports the first
reading — line 25 makes a planning document optional, line 26 says keep the work
proportionate, lines 48-49 ask for repeatable checks to be promoted into the
smoke workflow — but line 16 is the row an implementer reads first, and it is the
one that scopes the obligation.

**Evidence:** `AGENTS.md:156-158` — "Checks write and read back. `services/smoke`
never just opens a connection — it round-trips a payload and asserts it matches."
The existing automated check already performs what
`docs/application-validation.md:30-31` asks a rehearsal to perform.

**Smallest correction:** At line 16, append: "An existing automated end-to-end
check satisfies the rehearsal when it covers that path and its dated result is
recorded."

---

### F8 — Telemetry verification reads as unconditional for every application

**File/line:** `docs/application-validation.md:30-33`, escape hatch at `:38`

**Triggering scenario:** The small Compose-only Postgres app again. It has no
tracing and its contract does not mention observability.

**Concrete consequence:** Lines 30-33 state four obligations in the imperative
with no qualifier: representative operation, write and read back real data,
failure or recovery behavior, and "verify that its telemetry lets an operator
follow that same operation." The qualifier that would relieve a small app —
"Another application should select checks from its own contract" (line 38) — sits
inside the relay paragraph and grammatically attaches to relay's retry/DLQ/
replay/KEDA list at lines 37-38, not to the general paragraph above it. An
implementer adds tracing to an app that does not need it in order to pass a
documentation gate. Instrumenting an app solely to satisfy the check also changes
the configuration being evaluated.

**Evidence:** `docs/application-validation.md:30-33` versus `:35-38`; the
per-service split in `AGENTS.md:175-182` establishes that this repository treats
instrumentation as a per-service call, not a repository default — "Do not 'unify'
these; each is written down where it is, and the split is the decision."

**Smallest correction:** Condition the last clause at line 32-33: "and, where the
application's contract includes observability, verify that its telemetry lets an
operator follow that same operation."

---

### F9 — "relying on AWS-specific behavior" is broader than the precise trigger ADR 0001 already provides

**File/line:** `docs/application-validation.md:18`

**Triggering scenario:** Any application in this repository. Every one of them
talks to an AWS API surface locally through floci, and `make smoke` exercises
S3, SNS→SQS, and SES against it.

**Concrete consequence:** Read literally, every app "relies on AWS-specific
behavior" and therefore needs "local qualification, reviewed staging, and a
separately authorized live check." That is the broad reading the guide's own
line 74 ("An application staying local can finish with AWS explicitly untested")
is trying to prevent, so the table row and the prose disagree.

**Evidence:** `services/smoke/internal/checks/aws.go:41-179` — the local smoke
checks are AWS API calls; `docs/adr/0001-local-first-with-ephemeral-aws.md:43-45`
gives the precise existing formulation: behavior that "depends on real IAM
semantics, real SES sending limits, or real EKS networking."

**Smallest correction:** At line 18, replace "relying on AWS-specific behavior"
with ADR 0001's wording — "depending on behavior local emulation cannot
reproduce, such as real IAM semantics".

---

### F10 — Preserved qualification binds to an exact commit read from `HEAD`; the guide's wording implies it follows the branch

**File/line:** `docs/application-validation.md:21`

**Triggering scenario:** The prompt's sixth case, which is this patch itself.
This documentation-only change merges to `main`. Later, #97 is authorized, and
an operator runs the preflight gate from an updated `main` checkout.

**Concrete consequence:** The gate refuses, and the refusal looks like expired
qualification rather than a wrong checkout. `check_clean_head()` derives the
candidate commit from `git rev-parse HEAD` — it is not supplied by the operator —
and requires a worktree clean including untracked files. The local rehearsal
receipts must then carry `source_commit == HEAD`. Relay's qualified source is
`474dca7`; `HEAD` on `main` is now `cdec565`. The correct action is to run from a
checkout pinned at `474dca7`, which is what the staging record actually did, and
the guide never says so.

**Evidence:**

- `scripts/m4-preflight.py:672-681` — `check_clean_head()`, `git rev-parse HEAD`
  plus `git status --porcelain --untracked-files=all`.
- `scripts/m4-preflight.py:576-586` — receipts must satisfy
  `value.get("source_commit") != commit` → error, and `worktree_clean is True`.
- `scripts/m4-preflight.py:696-697` — images must carry the same revision.
- `docs/reviews/m4-96-staging-20260920.md:9-10` — "The candidate checkout
  remained clean; documentation and publication used a separate checkout."
- `docs/reviews/m4-96-staging-20260920.md:91-95` and `docs/roadmap-relay.md`
  (M4, "Keep qualified runtime source `474dca7`") — the policy the guide's line
  21 correctly summarizes but does not operationalize.

This is a **pre-existing mechanic** surfaced by the patch, not a defect the patch
created; the guide's line 21 is substantively correct. The gap is that the guide
is the new entry point and omits the consequence.

**Smallest correction:** Append to line 21: "Qualification binds to the exact
runtime revision, not to the branch tip — run later gated steps from a checkout
pinned at that revision."

---

### F11 — The README row label sends only "another application" to the guide, but the guide also governs shared-infrastructure changes

**File/line:** `README.md:287`

**Triggering scenario:** The prompt's fifth case — a shared Kafka client or
deployment configuration change affecting both apps. This is table row 4 of the
guide (`docs/application-validation.md:19`) and one of the two cases the AGENTS
paragraph names (`AGENTS.md:54-55`).

**Concrete consequence:** A reader navigating README's "Project documentation"
table to find out what a shared-client change requires will not recognize
"Validate another application on the shared stack" as the right row. The
AGENTS.md entry covers it, so an agent finds it; a human reading README does not.

**Evidence:** `README.md:287` versus `docs/application-validation.md:19` and
`AGENTS.md:54-55`.

**Smallest correction:** Change the README row label to "Validate an application
or a shared-stack change".

---

## Scenario results

Each case gives: local evidence needed, whether live AWS is required, reusable
prior evidence, and the stopping or closure condition — as the guidance reads
today. "Underdetermined" marks a case with more than one defensible answer.

### 1. Small Compose-only app using Postgres, with an automated round-trip test

- **Local evidence:** Row 1 (line 16). The existing automated round-trip test
  covers lines 30-31 directly (write and read back real data). Plus the failure
  or recovery behavior that matters to it (line 32). Record dated command,
  revision, configuration, expected and observed results in the app's issue
  (lines 46-48). Database ownership and migration boundary per line 41.
- **Live AWS:** Not required. Line 74 explicitly permits finishing with AWS
  untested, and the app has no AWS question.
- **Reuse:** The Postgres service, the `local/bootstrap/` pattern, and compose
  profiles. Lines 53-55 correctly require a fresh check of the new app's own DB
  access rather than inheriting relay's.
- **Stopping condition:** Line 26 — stop when the agreed evidence is complete.
- **Underdetermined:** whether the automated test *is* the rehearsal (F7), and
  whether telemetry verification is mandatory for an app with no telemetry
  contract (F8). **Smallest clarifications:** F7 and F8 corrections.

### 2. New Kafka consumer with its own group and database tables, intended for EKS

- **Local evidence:** Row 1 plus row 2 (line 17) — deployment, configuration,
  readiness, and shutdown on minikube. Boundary checks per lines 40-44: topic
  and consumer-group names, database ownership or migrations, permissions,
  resource use.
- **Live AWS:** Not required by "intended for EKS" alone. The guide handles this
  well: Kubernetes support is a minikube claim (row 2), separate from AWS support
  (row 3). Live AWS enters only if the app makes an AWS claim or hits a question
  local testing cannot answer (lines 8-9, 71-73).
- **Reuse:** Lines 57-58 answer this case almost verbatim — the existing Kafka
  transport supplies a tested implementation, while topic permissions, consumer
  group, database access, and image configuration each need their own check.
  This is the strongest paragraph in the guide.
- **Stopping condition:** Minikube evidence recorded; AWS support explicitly
  stated as untested (line 48, "State untested surfaces explicitly").
- **Underdetermined:** whether "intended for EKS" is itself a claim of AWS
  support under row 3's second clause (F9). **Smallest clarification:** the F9
  correction, which makes the trigger a question about behavior rather than
  about stated intent.

### 3. App using only S3 that needs to verify its real IAM permissions

- **Local evidence:** Row 1 rehearsal against floci. Line 71 is satisfied
  cleanly — "whether the app's role can access its bucket" is exactly the
  well-posed unresolved question the guide asks for.
- **Live AWS:** Yes, and correctly so. ADR 0001:43-45 names real IAM semantics as
  one of the three things local emulation cannot reproduce.
- **Reuse:** Bootstrap state backend, the project tagging convention, and
  `make aws-cost`. The identity and permission checks in
  `scripts/m4-aws-account.py` are partly reusable; the MSK-region and
  EKS-version gates are not.
- **Stopping condition:** **Underdetermined.** The guide's step 2 requires
  destroy plus inventory verification, while ADR 0001:30-33 says cheap-tier
  resources are safe to leave standing. Whether this check ends by tearing down
  the bucket or by leaving it is not answerable from the guide.
- **Missing decision:** whether a cheap-tier-only question needs the #96/#97
  two-step at all. **Smallest clarification:** the F4 correction.

### 4. Second app deployed alongside relay, sharing infrastructure but owning its own data and identities

- **Local evidence:** Lines 40-44 cover this well and are proportionate — check
  the boundaries the newcomer could disturb, exercise concurrent operation only
  "If concurrent operation is a goal", and record which applications were running
  during the observation. That last sentence is the right instinct: it keeps the
  measurement's operating configuration recorded rather than assumed.
- **Live AWS:** Only if the second app has its own AWS question. Relay's passing
  run does not transfer (lines 59-61, correctly stated).
- **Reuse:** **Underdetermined and partly misdescribed.** Lines 63-67 correctly
  warn that M4 commands and receipts are bound to relay. But the guide does not
  say where a second app's AWS resources should live, and the answer is
  load-bearing: `make aws-down` destroys the entire `infra/terraform/envs/dev`
  state (`Makefile:841-847`), so a second app added to that root module is
  destroyed by relay's controller. Prose defining "teardown scope" (line 91)
  cannot change that; a separate Terraform state or root module can.
- **Closure condition:** Blocked by F1 and F2 as written. Relay's
  `aws-inventory-empty` fails on the second app's retained project-scoped
  resources, and the shared $5 monthly budget can put relay's plan/apply guard
  into refusal.
- **Missing decision:** which Terraform state owns a second app's AWS resources.
  **Smallest clarification:** the F1 and F2 corrections, plus one clause at line
  91 — "which in practice means a separate Terraform root module and state, since
  `make aws-down` destroys everything in the dev state."

### 5. Shared Kafka client or deployment configuration change affecting both apps

- **Local evidence:** Row 4 (line 19) — check the changed boundary and run
  regression checks for affected applications. This answers the prompt's
  sub-question directly: it requires regression work for the apps *actually
  affected* and does not demand simultaneous operation, because line 42's
  concurrency clause is conditioned on "If concurrent operation is a goal."
  That is the proportionate design.
- **Live AWS:** Only when the changed boundary cannot be verified locally
  (line 19, second sentence). A Kafka client change is verifiable locally except
  for MSK IAM transport, which #92 already proved without a cluster
  (`docs/roadmap-relay.md`, M4 handoff table).
- **Reuse:** Row 5 (line 20) is the governing rule — repeat rehearsal steps whose
  evidence the change invalidates. For relay specifically, a shared-client change
  is a runtime source change, which per
  `docs/reviews/m4-96-staging-20260920.md:93-95` "requires a new candidate
  decision" and invalidates the `474dca7` qualification bound into the preflight
  receipts. The guide is correct but delegates this to ADR 0010 (line 98-99)
  without flagging that a shared-component change resets relay's paid-run
  candidate.
- **Stopping condition:** Both apps' affected checks pass; relay's candidate
  re-qualified if its runtime source moved.
- **Discoverability gap:** F11 — README sends only "another application" here.

### 6. Documentation-only merge after exact-source local qualification and staging

- **Local evidence:** Row 6 (line 21) — check the documentation only. Correct and
  consistent with `docs/roadmap-relay.md` M4 ("The evidence-only merge does not
  replace that candidate or require another local rehearsal") and
  `docs/reviews/m4-96-staging-20260920.md:92-93` ("Later documentation-only
  merges do not invalidate this local qualification").
- **Live AWS:** No.
- **Reuse:** All of it. The `474dca7` qualification and the `20260920T155738Z`
  staging GO survive; only expired or changed AWS observations and their
  dependent plan/GO inputs need refreshing (guide line 83, matching the staging
  record).
- **Closure condition:** Documentation checks pass. Relay's gates are untouched.
- **Operational caveat:** F10. The merge moves `main` past `474dca7`, so the
  later paid run must execute from a checkout pinned at the qualified revision —
  the preflight gate reads `HEAD` and refuses any untracked file. As a concrete
  illustration, this working tree currently holds ten untracked files and would
  be refused today.

---

## Checks actually run

Read-only throughout. No AWS CLI, Terraform, workload, deployment, load,
cleanup, or container/cluster operation was run. No candidate file was modified,
nothing was staged, committed, pushed, or mutated on GitHub.

| Check | Command | Result |
|---|---|---|
| Source identity, before and after | `git rev-parse HEAD`, `git rev-parse --abbrev-ref HEAD`, `git status --porcelain`, `shasum -a 256` on the three files | Unchanged; all three hashes match the handoff table |
| Whitespace | `git diff --check` | Clean |
| ADR index | `bash scripts/check-adr-index.sh` | Passed — "covers all 10 discovered ADR files (Accepted: 10)" |
| GFM parse | `pandoc 3.11 -f gfm -t html` on each of the three files | All parse; the guide renders 7 table rows (header plus six) |
| Relative links | Shell loop extracting every non-`http` Markdown target and testing existence, per file base directory | All resolve: 4 targets from the guide, 30 from README, 3 from AGENTS.md |
| Anchor | `pandoc -f gfm -t html docs/costs.md \| grep id="a-note-on-billing-alerts"` | Present; `docs/costs.md:262` |
| Markdown structure | Disposable Python check for MD009/MD012/MD022/MD025/MD041/MD047, table column consistency, fence balance | No issues in any of the three files |
| Live issue state | `gh issue view 96/97 --repo lilabrooks/my-local-platform --json ...` | #96 `CLOSED`/`COMPLETED`; #97 `OPEN`, labels `wait:owner` + `state:waiting`. Matches the handoff evidence |
| M4 tooling claims | Read `scripts/m4-aws-inventory.py`, `scripts/m4-preflight.py`, `scripts/m4-aws-account.py`, `scripts/aws-terraform-guard.sh`, `Makefile` (aws-down, aws-inventory-empty), `tools/m4-live-run/main.go` `cleanup()` | Supports F1, F2, F4, F5, F10 |

---

## Source identity

Recorded at the start and end of the review; unchanged between them and
identical to the prompt's handoff table.

- HEAD: `cdec565e693839940627340c67a7dfe30ba866a0`
- Branch: `main`
- Status: 2 modified (`AGENTS.md`, `README.md`), 10 untracked (including
  `docs/application-validation.md` and this report's prompt)

| File | SHA-256 | Matches prompt |
|---|---|---|
| `AGENTS.md` | `e461a9b0127157088ae55a5b62eb4ca08235fd74e0a8c7439ae9ee0bc16f70ed` | yes |
| `README.md` | `15f771c5e9f3956e5d7bb787304fe2afff0aa4da879097d960b5cd0b717b57ce` | yes |
| `docs/application-validation.md` | `2e665264759749b8060bda57831207af02cecd4cc6e2df39cd4de04b38055f0c` | yes |

---

## Unverified assumptions and evidence limits

- **markdownlint-cli2 0.23.2 was not run.** It is not installed on this host
  (no `node_modules`, no global binary), and installing tools is outside this
  review's authority. The disposable structure check above is a proxy covering
  the rules most likely to trip a new document; it is not equivalent. The
  implementer's reported lint result is not contradicted, merely not reproduced.
- **No fresh-task instruction-loading test.** I verified static navigation only:
  README:287 and README:360 link the guide, AGENTS.md:55 links it, and the guide
  links back to ADR 0001, ADR 0010, the AWS runbook, and `costs.md`. Whether a
  new agent session actually loads `AGENTS.md:54-59` and follows it is untested,
  as the prompt anticipated. Reported separately from the link results above.
- **No rendered-browser review.** I confirmed by reading that the second README
  reference sits inside the collapsed `<details>` block at README:348-364, and
  that the uncollapsed "Project documentation" table at README:287 also links the
  guide — so discoverability does not depend on the collapsed section. I did not
  render the page.
- **GitHub connector unavailable.** `plugin:engineering:github` requires an OAuth
  flow this non-interactive session cannot complete. I used the local `gh` CLI
  read-only instead, which returned live state for #96 and #97.
- **No live AWS state was inspected.** Current budget notification state, the
  account inventory, tag-activation status, and any credential or account
  identifier were not read. F1 and F2 are derived from repository code and
  accepted contracts, not from observed account state.
- **No runtime experiment was performed, and none is needed for this patch.**
  The unresolved behavior that would justify one is not documentation-shaped: it
  is whether a second application's resources can coexist with relay's teardown
  and budget gates. That question is answerable from the code above, and the
  answer is that they currently cannot without a scope change. If the owner
  later wants that coexistence, the experiment worth running is a cheap-tier
  one — apply a second tagged resource, run `make aws-inventory-empty`, and
  confirm the refusal — not a paid session.
- **Not audited:** the rest of the repository. `docs/validators-and-dependencies.md`
  and the other untracked review documents predate this change and were left
  alone, per the prompt.

---

## Verdict

**REVISE.**

**Is the guidance usable for the next application?** For a local-only
application, yes, with two wording ambiguities to resolve first (F7, F8). For an
application with an AWS question, not yet: the guide has one AWS path, built
around relay's expensive-tier session, and a small cheap-tier check does not fit
it (F4). For a second application sharing relay's AWS account, no — the two
couplings that would actually block it are missing or misdescribed (F1, F2).

**Does it preserve relay's current gates?** As written on the page, yes, and
precisely. Lines 98-104 restate #97's closure requirements correctly against
ADR 0010:399-402, the 48-hour floor and `Estimated: false` requirement match
ADR 0010:391-397 and `docs/costs.md:264-268`, and "48 hours alone does not
establish settlement" is a fair reading of the accepted shared-account
limitation. The guide does not imply a guaranteed settlement date, exact session
attribution, or that resources must stay alive while billing settles — lines
95-96 say the opposite, correctly. It also does not impose relay's billing
contract on local or cheap-tier apps; lines 98-99 scope it to M4.

The risk to relay's gates is operational rather than textual. If someone follows
the shared-environment paragraph at lines 90-92 and stands up a second
application in the same account, relay's cleanup verification and its hourly
plan guard can both start refusing for reasons that have nothing to do with
relay. Fixing that is two sentences.

None of the eleven findings requires new tooling, a new general-purpose runner,
a new ADR, or a change to #97's closure criteria. F3 is the one finding that
touches policy authority: the per-application rehearsal mandate is reasonable
new guidance and belongs in this guide and the AGENTS paragraph, but it should
be presented as new rather than as something ADR 0001 already decided. No
substantive change here needs an ADR of its own on the evidence I have; the
guide's scope is process guidance over existing accepted decisions, not a new
architectural choice.

Acceptance of this documentation would grant no AWS or publication authority.
