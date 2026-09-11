# Claude second review: M4 preparation implementation

Review the current working tree in `/Users/lilabrooks/code/my-local-platform`
against the revised M4 staging and live-validation plan. This is the second
review, after implementation began in response to the first BLOCKED verdict.
Determine which findings are actually fixed, find regressions, and identify
the smallest remaining work before the candidate can be frozen.

## Authority and review method

This is a read-only review. Do not edit repository files, commit, push, change
GitHub issues or PRs, run AWS commands, start or stop containers, change cluster
state, run bootstrap, or repeat the demonstration load. The local stack may
already be running. Leave it untouched.

Read `AGENTS.md` and `CLAUDE.md` and follow their navigation rules. Record the
actual HEAD, branch, and working-tree status. Review both `git diff HEAD` and
untracked source files; most of the new capture implementation is untracked
and will not appear in an ordinary diff. Do not assume HEAD identifies the
contents that were tested.

You may run focused offline unit tests with temporary fixtures after checking
that they do not contact AWS or mutate the running stack. Do not run broad
Make targets without checking their effects. Use the GitHub connector for
read-only history if needed. For uncertain AWS API or IAM semantics, consult
current official documentation without querying the account. Do not install
tools or repair the environment as part of the review.

Treat the implementer's status and prior targeted review as claims to check,
not independent verification. Report unavailable private evidence as a limit.
Do not reproduce account identifiers, endpoints, credentials, or raw secret
carriers in your answer. Use synthetic canaries for adversarial checks.

## Sources and current claims

Read these documents completely:

- `docs/plan-m4-live-execution.md`
- `docs/adr/0010-live-aws-relay-contract.md`
- `docs/runbook-aws-relay.md`
- `docs/costs.md`

The original review is available locally at
`/Users/lilabrooks/.codex/attachments/9b901984-1931-441c-81ef-c1f1e4cd5155/pasted-text.txt`.
Use its finding IDs when reporting disposition. If unavailable, say so and use
the finding categories below without inventing missing wording. PR #137 and
issues #95, #96, #97, and #136 provide history, not permission to execute.

The owner explicitly approved adding a dedicated operator-only capture Pod
Identity, scoped to the two relay topics and proof operations. Assess whether
the implementation stays within that approval. Do not reopen the existence of
the role as an unresolved decision; flag any broader authority you discover.

The implementation report says:

- The working tree adds bounded local/AWS capture, a capture binary and role,
  secret-fingerprint publication, failure-to-cleanup handling, stronger
  rehearsal and GO checks, and after-destroy ECR rejection.
- Local run `20260911T005220Z` recorded 600 distinct load events, peak lag 593,
  peak replicas 12, and final lag zero with one replica. Its private evidence
  is in `.evidence/m4-local/20260911T005220Z/`. It used a dirty working tree.
  Later cleanup and diagnostic fixes have not had another full cluster run.
- Go race tests, Python tests, mocked Terraform contracts, and Kubernetes
  validation passed at the revisions described in the plan. Lint passed 11
  checks but failed to obtain Trivy's complete pinned checks bundle.
- Final-cost collection and exact-attribution wording remain unfinished.
  No clean candidate has been frozen, and no AWS staging or paid proof ran.

Verify the evidence that is accessible. Do not convert a development pass into
candidate approval, or assume a prior test run covers subsequent edits.

## Implementation paths

Inspect the changed code and its tests, especially:

- `scripts/m4-live-capture.py` and `scripts/tests/test_m4_live_capture.py`
- `services/relay/cmd/relay-capture/` and `services/relay/Dockerfile`
- `tools/m4-bootstrap/main.go`, `secret_scan.go`, and their tests
- `scripts/m4-evidence.py` and its tests
- `scripts/m4-preflight.py`, `scripts/m4-stage.py`, and their tests
- `tools/m4-live-run/main.go` and `scripts/aws-terraform-guard.sh`
- `scripts/m4-aws-inventory.py` and its tests
- `infra/terraform/envs/dev/identities.tf` and `tests/runtime.tftest.hcl`
- `k8s/aws/relay/serviceaccounts.yaml`, the local relay ConfigMap,
  `k8s/validate/aws_test.go`, and the affected `Makefile` targets

Follow dependencies where necessary. File presence and passing mocks do not
by themselves establish that a producer's output satisfies its consumer.

## Review questions

1. **Executable evidence and cohort accounting.** Trace one event through
   request, idempotent repeat, Kafka record, healthy and exhausted deliveries,
   persisted attempts, trace, exported file, sanitizer, and publication
   verification. Check each join and failure condition. Verify that the fixed
   600-event/16-tenant cohort retains the specified sink delay through drain;
   that lag and scaling are fresh observations; and that old counters, missing
   samples, or replayed records cannot satisfy a new cohort. Review poison
   acknowledgement coordinates, bounded DLQ reads, and replay correlation.

2. **Capture identity.** Trace each capture operation through its service
   account, Pod Identity association, IAM actions and resources, and shared
   broker transport. Check the two-topic scope, delivery-only poison writes,
   separate capture group, and replay's delivery identity. Look for missing
   permissions as well as excess ones. Separate offline policy evidence from
   live-only authorization claims.

3. **Secret handoff after destruction.** Trace receipt initialization, exclusive
   bootstrap claim, retrieval/generation, fingerprint persistence, sensitive
   mutations, completion, capture scanning, and post-destroy publication.
   Test interruption, partial coverage, failed persistence, duplicate or unknown
   fields, wrong bindings, placeholders, and concurrent claims. Check the actual
   representations emitted by Go, Python, URLs, base64, and prefixed/nested
   JSON logs. Verify bounded decoding fails closed when it cannot cover a
   carrier, and that errors and bootstrap output cannot expose secrets.
   Distinguish supported text scanning from arbitrary encodings, fragments,
   pixel inspection, and protection against an actor who can rewrite receipts.

4. **First-failure cleanup.** Trace failures before object initialization,
   during deployment, during concurrent load, during capture, and on signals
   or deadline expiry. Confirm no later mutation starts after stop, child
   processes are bounded, and the original failure survives cleanup errors.
   Check one consistent kubeconfig/context through every installer. Distinguish
   stop attempted, stop accepted, port-forward shutdown, local restoration,
   and controller-verified AWS teardown. Verify restoration after a mutation
   succeeds remotely but its response is lost, and that one failed restoration
   does not skip the other.

5. **Candidate and apply gates.** Check the actual rehearsal receipt schemas,
   exact source SHA, clean-tree requirement, image provenance, artifact hashes,
   and passing observations. Trace all staged input hashes and original
   observation ages from GO generation to the final apply boundary. Can a new
   GO refresh stale evidence? Can a rewritten receipt escape validation?
   Does the current IPv4 check run before the session becomes spent? Identify
   time-of-check gaps and distinguish pre-session refusal from failed apply.

6. **Staging instructions.** Walk the runbook literally from backend inspection
   through budget, account/prices, cheap plan/review/apply, inventory, image
   staging, hourly plan without apply, GO, and separate paid approval. Check
   explicit flags, preserved cheap evidence, re-plan invalidation, renderer
   input producers, and credential recovery. Verify that individual commands
   or stale capture-plan entries cannot bypass the bounded workflow.

7. **Cleanup and publication.** Check before/after ECR rules, service coverage,
   persistent-resource exceptions, and unknown or untagged resource limits.
   Can an interrupted or partially bootstrapped attempt publish useful typed
   cleanup evidence without secrets or a false demo pass? Is that evidence
   sufficient for the claims made about cleanup? Check successful publication,
   later verification, raw-file retention, and the separate #96/#97 closure
   paths without modifying the executing checkout during the paid window.

8. **Known unfinished work.** Inspect the final-cost placeholder and all
   consumers. Can provisional/final publication still accept monthly totals or
   arbitrary text as settled attributed cost? Specify the minimum session-date
   collector and validation needed, including UTC month boundaries, pagination,
   collection time, estimated status, and shared-account attribution limits.
   Identify the owner decision separately from code work. Confirm the Trivy
   failure and missing clean-candidate receipts remain visible gates.

## Response format

Return actionable findings first, ordered by severity. Each needs an exact
current file/line reference, a concrete failure mechanism, its consequence,
and the smallest correction or regression test. Mark whether it is a remaining
original defect, an acknowledged unfinished item, or a new regression. Avoid
style-only findings and do not demand a larger experiment or a different
topology without showing why the accepted proof cannot work.

Then provide:

- A compact disposition of the original findings: fixed, partial, open, or
  not verifiable, with evidence. Do not label documentation-only fixes as
  enforcement unless the code checks them.
- Commands you actually ran, their outcomes, and the limits of the review.
- The ordered minimum remaining work before freeze, followed by the evidence
  needed after freeze and before staging authorization. Separate owner
  decisions, code defects, environmental failures, and live-only outcomes.

End with two explicit verdicts:

- Implementation reviewed so far: ACCEPT / REVISE, with reasons.
- Execution: BLOCKED BEFORE FREEZE / READY FOR FROZEN-CANDIDATE REHEARSAL /
  READY FOR STAGING AUTHORIZATION, supported by the evidence you inspected.

Do not execute the plan, authorize spending, or request AWS approval.
