# Validating applications on the shared platform

Date: 2026-09-20
Status: Repository guidance. Each application's recorded results determine
which environments and behaviors it can claim to support.

This guide introduces a bounded local rehearsal for each new application.
[ADR 0001](adr/0001-local-first-with-ephemeral-aws.md) already requires live
verification of behavior local emulation cannot reproduce, such as real IAM
semantics. Record AWS deployment support only after exercising that app's
deployment on AWS; an intended AWS target can remain explicitly untested.

## Choose the scope

| Change or claim | Validation needed |
|---|---|
| New application using the local stack | Automated checks covering its core behavior and relevant failures. An existing end-to-end check satisfies the rehearsal when it covers that path and its dated result is recorded. |
| Application claiming Kubernetes deployment support | Exercise its deployment, configuration, readiness, and shutdown on minikube. |
| Application claiming tested AWS support, or needing evidence local emulation cannot supply | Qualify locally, then authorize a live check using the appropriate cheap or hourly tier below. Local use of an emulated AWS API alone does not require a live run. |
| Shared infrastructure, dependency, identity, or deployment change | Check the changed boundary and run regression checks for affected applications. Use AWS when that boundary cannot be verified locally. |
| Ordinary application change | Run the checks it touches. Repeat rehearsal steps whose evidence the change invalidates. |
| Documentation-only change | Check the documentation; preserve qualification of the unchanged runtime candidate. M4's later gated steps must use a clean checkout pinned to that qualified revision. |

Before a rehearsal, name the behavior to prove, the environment, the bounded
workload, and the pass, failure, and cleanup conditions. Use the application's
existing issue or runbook; a separate planning document is optional. Keep the
work proportionate to the claim and stop when the agreed evidence is complete.

## Prove each application's behavior

Follow a representative operation from its input through the dependencies to
the result its user receives. For paths that write data, read it back and assert
the result. Check the failure or recovery behavior that matters to the
application. Where its contract
includes observability, verify that its telemetry lets an operator follow that
same operation; adding instrumentation solely for rehearsal changes the
configuration being evaluated.

For relay, that path is acceptance, durable event identity, Kafka consumption,
and signed subscriber delivery, with attempt history and a joined trace.
Retry exhaustion, DLQ, replay, and lag-based scaling belong to relay's stated
contract. Another application should select checks from its own contract.

When applications share a running stack, check the boundaries the newcomer
could disturb: topic and consumer-group names, database ownership or
migrations, permissions, and resource use. If concurrent operation is a goal,
exercise that configuration and check the affected existing applications.
Record which applications were running during the observation.

Store dated commands, source revision, relevant configuration, expected and
observed results, and evidence links in the existing issue, runbook, or ADR
verification section. State untested surfaces explicitly. Promote repeatable
regression checks into the existing test or smoke workflow where practical.

## Reuse platform work with explicit limits

Prior evidence applies to the versions, topology, permissions, and behavior
that were exercised. Reference that evidence and explain which assumptions the
new application preserves. Recheck the parts changed by its integration.

For example, an existing Kafka transport can supply a tested implementation,
but a new app's topic permissions, consumer group, database access, and image
configuration still need their own checks. A passing relay AWS run supplies
evidence for relay's exercised path; it cannot certify a future application's
deployment.

Reuse applicable setup, image publishing, deployment, cleanup, and evidence
collection code. The current M4 commands and receipts are bound to relay's
source, images, topology, and proof steps. Check their assumptions and adapt
them before using them for another application. A general multi-application
runner is not a prerequisite for adding the next app.

M4 qualification and image receipts bind to an exact source commit. Keep that
candidate in a clean checkout for subsequent gated steps, and use a separate
checkout for documentation or publication. A later documentation merge does
not transfer receipts to the branch tip; a runtime change needs a new candidate
decision and the qualification its contract requires.

## When an AWS session is warranted

Write down the unresolved AWS question first, such as whether the app's role
can access its MSK topic or whether its workload reaches RDS with the intended
identity and network rules. Choose the smallest AWS footprint that answers it.
An application staying local can finish with AWS explicitly untested.

For a cheap-tier question, such as S3 IAM permissions, obtain owner authority
for the AWS mutations, check identity, permissions and expected costs, and run
the bounded check. Include images or deployment checks only when the tested
path uses them. Agree which resources and test data to remove or retain, with
their ongoing cost responsibility; cheap-tier retention still has to respect
the shared-state and inventory constraints below. The hourly procedure and
relay's billing wait do not apply solely because a check uses real AWS.

When hourly resources are needed, use the separation established by relay's
[#96](https://github.com/lilabrooks/my-local-platform/issues/96) and
[#97](https://github.com/lilabrooks/my-local-platform/issues/97):

1. **Stage the candidate.** Qualify the runtime source locally and obtain owner
   authority for staging mutations. Stage immutable images and review the
   deployment and infrastructure plan. Check current
   identity, permissions, availability, support versions, costs, and cleanup
   coverage. Refresh expired or changed observations and their dependent inputs.
2. **Authorize and execute the hourly session.** Obtain separate owner approval
   for the reviewed scope, maximum spend, elapsed-time destroy deadline, cleanup
   reserve and completion target, cleanup owner, and abort procedure. Prior
   staging approval and another application's GO do not authorize execution.
   Stop for failure, divergence from the reviewed plan, or the destroy deadline;
   continue cleanup if it exceeds its target. Collect the agreed evidence,
   destroy on success or failure, and verify the full scope required by the
   governing state and service-inventory checks. Budget alerts arrive too late
   to enforce a short session's clock.

### Shared account and cleanup constraints

The current M4 tooling assumes broader scope than a single app or run.
`make aws-down` destroys the resources managed by the dev Terraform state. The
[inventory collector](../scripts/m4-aws-inventory.py) queries supported services
across the selected region and filters by project tags and `mlp-` name rules,
without narrowing to the run ID. Another app's retained matching runtime
resources, or ECR repositories at final cleanup, fail the empty-inventory gate
even if relay's own resources are gone.

Before sharing AWS resources, review both what destruction can remove and what
retained resources make verification fail. Under the current M4 contract,
sequence sessions so the required inventories can become empty. Retaining other
apps in that scope requires a separately reviewed ownership, cleanup, and
verification change before execution. Preserve project tagging and budget
coverage; changing tags or names to evade the gate does not resolve the conflict.

The existing $5 monthly allowance is shared across project-tagged spending by
all applications in the account, before tax. Current hourly plan/apply guards
require every budget notification to be `OK`; another app's spending or forecast
can block relay's remaining #97 run until AWS reports `OK` again or the monthly
period resets. Check this shared allowance when sequencing runs and before
authorizing one. Changing its amount or scope requires an owner decision; see
the [budget and coverage limits](costs.md#a-note-on-billing-alerts).

### Record the outcome

Define the session's billing evidence and closure conditions before execution.
Separate technical completion and verified cleanup from delayed billing
evidence. For hourly sessions, destroy the ephemeral resources before waiting
for that evidence.

A failed demonstration remains failed after successful cleanup and billing
settlement. Preserve its evidence and keep the validation claim open for local
repair and a newly authorized retry, or record an explicit owner decision to
defer it with an observable revisit condition.

Relay's [ADR 0010](adr/0010-live-aws-relay-contract.md) and
[AWS runbook](runbook-aws-relay.md) continue to govern M4. This guide does not
relax #97: closure requires its passing demonstration, successful controller
evidence, empty inventories, and final-cost receipt. That receipt requires at
least 48 hours after cleanup and every cost period marked `Estimated: false`;
48 hours alone does not establish settlement. See the
[cost guide](costs.md#a-note-on-billing-alerts).
