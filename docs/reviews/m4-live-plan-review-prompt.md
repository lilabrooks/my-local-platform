# Independent review prompt for the revised M4 plan

Review `/Users/lilabrooks/code/my-local-platform/docs/plan-m4-live-execution.md`
as the execution plan. Read that file before assessing it. If it is unavailable,
report that the plan could not be reviewed; do not substitute the runbook or
search other task transcripts for an assumed plan.

This is a read-only review of the revised plan and its implementation status.
Preparation is now partly implemented in the working tree; inspect both tracked
diffs and new files, and distinguish them from the committed source. Do not implement changes, edit
files, publish GitHub updates, run AWS commands, or mutate local clusters.
Use repository navigation tools and the GitHub connector where available.

The prior review found a missing `scripts/m4-live-capture.py`, a publisher that
requires plaintext secrets after destroy, stale/unvalidated local rehearsal
evidence, incomplete staging instructions, manual failure-to-cleanup handoff,
undefined render inputs, kubeconfig disagreement, receipt freshness gaps,
incomplete cleanup/publication claims, and shared-account cost ambiguity.
Those findings led to the preparation phase in this revised plan. Its status
section records a dirty-tree local capture pass and remaining work, not a
merged implementation or frozen-candidate pass.

Read the repository instructions, the full revised plan, ADR 0010, the AWS
runbook, costs guide, and relevant Make targets. Verify claims against
`scripts/m4-evidence.py`, `scripts/m4-preflight.py`, `scripts/m4-stage.py`,
`scripts/m4-aws-inventory.py`, `scripts/aws-terraform-guard.sh`,
`tools/m4-bootstrap`, `tools/m4-live-run`, and their tests. Consult PR #137 and
issues #95, #96, #97, and #136 for history and authority. Record your actual
source SHA; do not assume it still equals the plan's inspection baseline.

Determine whether the proposed preparation and execution order is sufficient.
Distinguish a flaw in the plan from an implementation task the plan already
identifies. Do not call the repository ready merely because the plan is sound.

Focus on these questions:

1. Does every live evidence artifact have an implementation and verification
   task before freeze, including event/trace ID handoff, context selection,
   private endpoint access, DLQ reader permissions, replay and the fixed load?
2. Does cohort accounting distinguish idempotent repeats, healthy/failing
   subscriptions, poison records, load and replay from cumulative old activity?
3. Can the proposed secret-scanning handoff survive partial bootstrap,
   interruption and destroyed secrets without writing plaintext credentials?
   Review representation coverage, producer timing, permissions, run/commit
   binding, receipt validation, placeholder rejection, and pixel-review limits.
   Treat fingerprints as a proposed mechanism requiring evidence, not a proven
   solution. Identify concrete defects or missing decisions.
4. Does the failure path stop subsequent commands while preserving the original
   failure? Confirm `command || aws-live-stop` alone is not sufficient.
5. Are exact-commit rehearsal and input freshness checked at the right boundaries?
   Can regenerating GO extend the age of an original observation? Can rewritten
   receipts or changed public IPv4 escape final validation?
6. Is the order backend -> budget -> account/prices -> cheap plan/apply ->
   inventory/images -> expensive plan -> GO -> separate paid approval complete?
   Does it acknowledge that GO, rather than Terraform, binds application images?
7. Are all configuration and authority paths consistent from Terraform/ECR
   through render, bootstrap, ArgoCD, running pods, and exported evidence?
8. Are the 150/180-minute limits, first-failure abort, full dev destroy after a
   refused apply, credential recovery, and spent run IDs described accurately?
9. Does cleanup proof cover exactly what it claims, with phase-appropriate ECR
   checks and explicit persistent-resource exceptions?
10. Can both successful and failed attempts publish truthful sanitized evidence?
    Are #96 and #97 closure evidence and raw-file retention workable without
    changing the executing checkout before cleanup?
11. Is final cost tied to session dates and labeled according to actual
    attribution support, including month boundaries and shared-account usage?
12. Is any proposed preparation disproportionate to this fixed experiment, or
    an unapproved change to the accepted topology, measurement, or cost contract?

Return findings first, ordered by severity, with exact file/line or issue
references and a concrete failure mechanism. State which previous findings
are addressed by the proposed plan and which remain unresolved. Identify
unverified claims and decisions requiring owner input. Supply minimal changes
to the plan for actionable findings.

End with two separate verdicts:

- Plan: READY FOR IMPLEMENTATION / NEEDS PLAN CHANGES.
- Execution: BLOCKED UNTIL PREPARATION AND CANDIDATE EVIDENCE PASS / READY FOR
  STAGING AUTHORIZATION, supported by observed implementation evidence.

Do not run the plan or request AWS approval as part of this review.
