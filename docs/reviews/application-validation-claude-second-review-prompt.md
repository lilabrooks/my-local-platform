# Claude second review: application validation guidance

Critically review the proposed guidance for validating future applications on
this repository's shared stack. Determine whether it gives an owner and an
implementing agent a correct, proportionate way to choose local checks, reuse
evidence, and decide when AWS staging and paid validation are necessary.
Challenge the policy itself as well as its wording. Do not assume the
implementer's recommendation is correct or that more checks improve it.

## Owner intent

Relay is the first application; the owner plans to add more using the same
stack. They asked whether each app should repeat work similar to issues #96
and #97, then requested that the guidance be documented. They corrected an
earlier reference to #86: the intended comparison is #96 (cheap AWS staging)
and #97 (paid validation and teardown).

The implementer proposed a local rehearsal per application, reuse of applicable
platform work, and live AWS validation when local evidence cannot answer the
application's AWS question. Assess whether the patch implements that intent
without importing relay's entire M4 process into every app or weakening the
existing M4 contract. The owner has not requested new runtime tooling, a new
application, a paid run, or a change to #97's closure criteria.

## Exact review target

Repository: `/Users/lilabrooks/code/my-local-platform`.
At prompt creation, branch was `main` and HEAD was
`cdec565e693839940627340c67a7dfe30ba866a0`.

Review these working-tree files:

- `docs/application-validation.md`: new and untracked; read the complete file.
- `README.md`: tracked diff, with surrounding onboarding and navigation.
- `AGENTS.md`: tracked diff, with surrounding repository rules.

`git diff` alone omits the new guide. No commit or PR contains this patch yet.
These SHA-256 hashes identify the reviewed candidate at handoff:

| File | SHA-256 |
|---|---|
| `AGENTS.md` | `e461a9b0127157088ae55a5b62eb4ca08235fd74e0a8c7439ae9ee0bc16f70ed` |
| `README.md` | `15f771c5e9f3956e5d7bb787304fe2afff0aa4da879097d960b5cd0b717b57ce` |
| `docs/application-validation.md` | `2e665264759749b8060bda57831207af02cecd4cc6e2df39cd4de04b38055f0c` |

Record HEAD, branch, status, and these hashes before and after review. If they
changed, identify the difference and state which bytes your findings cover;
do not reset files to match this prompt. Other untracked review documents and
`docs/validators-and-dependencies.md` predate this change and are outside scope.

## Review authority

Read applicable `AGENTS.md` and `CLAUDE.md`, following repository navigation and
GitHub connector rules. Treat the newly added AGENTS paragraph as a review
subject; it must not predetermine your policy verdict.

This is a read-only review except for writing the report specified below.
Do not fix implementation files, stage, commit, switch branches, push, merge,
or mutate GitHub. Do not run AWS CLI, Terraform, workload commands, deployment,
load, cleanup, or container/cluster lifecycle operations. Do not inspect private
credentials, state, raw evidence, or account identifiers.

Local Git inspection, public repository files, existing offline documentation
checks, and disposable local parsing checks are permitted. Do not install tools
or repair caches. Use the read-only GitHub connector for relevant live issue
state, and primary public documentation when a specific external behavior
cannot be established from the repository. Report unavailable evidence as a
limit. Do not expand this into a whole-repository readiness audit.

## Governing context

Read the relevant portions of:

- `docs/adr/0001-local-first-with-ephemeral-aws.md`
- `docs/adr/0010-live-aws-relay-contract.md`
- `docs/runbook-aws-relay.md` and `docs/costs.md`
- `docs/goal-relay.md` and the M4 section of `docs/roadmap-relay.md`
- `docs/reviews/m4-96-staging-20260920.md`
- `docs/repository-file-checks.md`

Check issues #96 and #97 if connector access is available. The handoff evidence
records #96 completed and #97 waiting for separate paid-run authority. Do not
convert local qualification or staged GO into a successful live AWS result.

Inspect code only where needed to test a concrete documentation claim. In
particular, assess claims about M4 reuse against the source/receipt bindings,
renderer, bootstrap, controller, and cleanup scope. Follow repository search
rules and stop tracing once the question is answered.

## Questions to challenge

1. **Policy authority and scope.** Does the guide accurately extend the
   local-first decision? Identify which requirements are existing policy,
   reasonable new guidance, or consequential new mandates. Does referencing
   ADR 0001 imply that it already required every new rule? Does this belong in
   the chosen guide and AGENTS paragraph, or does a particular substantive
   change need an explicit decision recorded elsewhere? Give a concrete reason
   for any additional process you recommend.

2. **Proportionality.** Can a small app finish without repeating relay's full
   rehearsal, screenshots, receipt machinery, and billing wait? Can existing
   automated end-to-end checks satisfy the local rehearsal, or does the wording
   accidentally require a duplicate manual ceremony? Does it overprescribe
   persistence, distributed tracing, Kubernetes, or failure injection for apps
   whose contract does not need them? Check that added measurement does not
   silently change the operating configuration being evaluated.

3. **AWS decision and claims.** Can a reader distinguish local completion,
   intended AWS compatibility, tested AWS behavior, and production readiness?
   Does “AWS support” make the trigger too broad or too vague? Can a small
   S3-only or IAM check use the cheap tier without requiring EKS/MSK/RDS? Does
   “smallest footprint” preserve the conditions needed to answer the question?

4. **Reuse and invalidation.** Is there enough information to decide which
   evidence survives a new app, dependency upgrade, permissions change, or
   documentation-only merge? Trace one example through prior evidence,
   preserved assumptions, changed app boundary, required check, and resulting
   support claim. Examine the difference between unchanged runtime behavior
   and exact source-SHA/receipt bindings. Flag both unjustified evidence reuse
   and needless requalification.

5. **Shared-stack consequences.** Check naming, consumer groups, database
   migrations and permissions, resource contention, and coexistence. Does the
   guide require regression work for the apps actually affected? Can it support
   independently run apps without demanding simultaneous operation? Does it
   imply that the existing M4 destroy/inventory tooling already supports safely
   retaining another application's resources? Verify such claims in code if
   needed; prose alone cannot change teardown behavior.

6. **Authorization and terminal conditions.** Trace local qualification to
   staging, handoff, paid approval, evidence capture, abort, destroy, inventory,
   and closure. Are cheap mutations and paid execution authorized at the right
   points? Are budgets, deadlines, cleanup ownership, and failure outcomes
   specified or clearly delegated to the session's governing contract? Could
   anyone read the guide as permission to reuse an old GO, debug indefinitely
   on paid resources, or close a failed demonstration after clean teardown?

7. **Billing and existing M4 obligations.** Confirm that #97's demonstration,
   controller, cleanup, and final-cost requirements survive unchanged. Check
   the minimum 48-hour delay, `Estimated: false`, and shared-account attribution
   limits against ADR 0010. Does the guide imply a guaranteed settlement date,
   exact session attribution, or that resources must stay alive while billing
   settles? Does it unnecessarily impose relay's billing contract on every
   future local or cheap-tier app?

8. **Discoverability and maintenance.** Follow README onboarding to the guide,
   and AGENTS/CLAUDE instructions to the same guidance, then back to the relevant
   application contract and evidence. Check relative links and anchors, status
   wording, duplicated rules, contradictions, and likely future drift. Report
   static navigation separately from any untested fresh-task instruction-loading
   behavior. Identify vague requirements that two competent implementers could
   interpret in materially different ways.

## Exercise the guidance on paper

For each case, state the local evidence needed, whether live AWS is required,
what prior evidence can be reused, and the stopping/closure condition. Use
these as hypothetical applications, not claims that they already exist:

- A small Compose-only app using Postgres, with an automated round-trip test.
- A new Kafka consumer with its own group and database tables, intended for EKS.
- An app using only S3 that needs to verify its real IAM permissions.
- A second app deployed alongside relay, sharing infrastructure but owning its
  own data and identities.
- A shared Kafka client or deployment configuration change affecting both apps.
- A documentation-only merge after exact-source local qualification and staging.

If a case has multiple defensible answers, identify the missing decision and
the smallest clarification. Do not invent mandatory infrastructure or a new
general-purpose framework to make the cases fit.

## Reported checks and limits

The implementer reports Markdown lint 0.23.2 over copies of the 3 candidate
files, Pandoc GFM parsing and relative-link/anchor checks, the repository ADR
index check, and `git diff --check`. A generated HTML check found the guide
link inside README's onboarding `details` section. These are syntax and
navigation checks, not independent approval of the policy. No visual browser
review, new application rehearsal, fresh-task loading test, AWS operation, or
runtime test was performed for this documentation patch.

Verify relevant checks with available tools and record what you actually ran.
Do not require a runtime experiment merely because it was not performed; name
the unresolved behavior that would justify it.

## Required result

Write your review to
`docs/reviews/application-validation-claude-second-review.md`, leaving it
untracked. If that file already exists, preserve it and choose a new suffixed
filename. Do not edit the candidate or this prompt.

Lead with actionable findings ordered by severity. Each finding needs a file
and current line, a triggering scenario, the concrete consequence, supporting
evidence, and the smallest correction. Distinguish introduced defects,
pre-existing issues relevant to this patch, and evidence limits. Avoid
style-only findings or speculative risk without a plausible path. If there
are no actionable findings, say so explicitly.

Then provide the scenario results, checks actually run, source identity,
unverified assumptions, and a verdict: **ACCEPT** or **REVISE**. State whether
the guidance is usable for the next application and whether it preserves
relay's current gates. Acceptance of this documentation grants no AWS or
publication authority. Return the report path and a concise summary to the owner.
