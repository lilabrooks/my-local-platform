# M4 #97 live attempt, 2026-09-22

Status: failed during infrastructure provisioning. The application capture
never started. Recovery cleanup was verified at 18:31:18Z; M4 remains incomplete.

## Approved attempt

The owner approved one paid run, bounded evidence capture and full dev teardown
after the refreshed plan readback. The source was
`474dca7e8e121c08f2951b02871cfe4c4b87e2ee`, using the existing qualified
staging ID `20260920T155738Z` in `us-east-1`.

The refreshed plan passed all existing gates with 82 creates, no updates or
deletes, and the original image digests. Its SHA-256 was
`d2adcee6c8fe226eae99bdd719662dcbcb3ec33fe2b4efc1011f5d2845bf64c7`.
The model remained $1.0219/hour, below the $1.25/hour shape limit; the session
maximum remained $5. These are model and operating limits, not a measured bill.

`make aws-live-run` started the paid clock at **17:38:35Z**. EKS, MSK,
RDS and the NAT gateway were created. The node group launched two Spot
`t3.medium` workers at 17:50:39Z and 17:51:00Z, and read-only EC2 queries at
about 17:54Z verified the project and ephemeral tags on both and on their
volumes. A third Spot instance launched at 17:57:07Z in the other Availability
Zone, and the first instance powered off at about 17:58:25Z. Nothing captured
why; a Spot interruption is plausible but unproven. The tagging data collected
during recovery shows the same tags on all three instances. The worker group
never reached active status.

## Failure and cleanup

A read-only `kubectl get nodes -o json` observation at approximately 18:08Z
found both workers `Ready=False`, with `KubeletNotReady` and:

```text
container runtime network not ready: NetworkReady=false
reason:NetworkPluginNotReady
message:Network plugin returns error: cni plugin not initialized
```

`make aws-live-stop` requested cleanup immediately. Destroy started at
**18:08:13Z**. No workload bootstrap, relay proof, retry or live repair ran.

The stop left the node group outside Terraform state. While apply ran, the
command woke the controller with SIGTERM, and the controller passed SIGTERM on
as its first signal to the whole `make` and Terraform process group. Terraform
began a graceful stop, but the AWS provider plugin (go-plugin v1.7.0, which
ignores only SIGINT) exited during the node-group create. The console shows
`make` reporting `Terminated: 15` before Terraform's `Error: execution halted`
and `Error: Plugin did not respond` for the node group. The controller treated
`make`'s exit as the end of apply: `destroy_started_at` matches the 18:08:13Z
stop request, and the first destroy attempt ran at 18:08:14Z. The deadline
path, by contrast, begins with SIGINT and a 30-second grace. The
evidence strongly supports this stop sequence as the cause; whether a
SIGINT-first stop would have recorded the group in state was not tested.

A read-only state/API comparison at 18:09:43Z found no Terraform node-group
address but one AWS node group belonging to this run. After checking its
account, ARN and project tag against the captured identity, the operator
requested deletion of that exact group as part of the authorized teardown.

Terraform's three destroy attempts could not delete EKS while the group was
attached. The controller finished at **18:14:09Z** with `cleanup_failed` and
`cleanup_verified: false`. This was not the end of the billable resource
lifetime. The failed packet was published and verified before recovery.

The stop command recorded its default `stop_reason: evidence_complete`.
That field does not describe the demonstration outcome: `apply_exit` was `-1`,
the attempt failed, and no capture started. The original controller receipt is
preserved without alteration.

Recovery waited for the known node-group deletion, then verified the intended
account and default Terraform workspace before running `make aws-init` and
`make aws-down AWS_DESTROY_ARGS=-auto-approve`. Both commands passed.
The final log-group check found no matching group left to delete.

`m4-publication-extra.py collect-recovery`, `publish-recovery` and
`verify-recovery` passed for observation `20260922T183110Z`. At **18:31:18Z**,
the recovery result was `recovered`, with `cleanup_verified: true` and
`demonstration_passed: false`. Dev Terraform state was empty, and the complete
scoped inventory found zero EKS, MSK, RDS, EC2, EBS, EIP, ELB, ECR, NAT or
CloudWatch log resources. The state bootstrap and persistent budget were
outside the dev teardown scope. The tagging API still returned 16 entries:
those two retained resources and 14 EC2 entries. Separate native EC2 queries
resolved ten as absent and four as terminal (deleted or terminated), with no
remaining live resource or failed query.

The conservative interval from session start to verified cleanup was
**52 minutes 43 seconds**. `make aws-cost` captured the billing data available
after recovery; it is a shared-account, provisional total, not the final cost
of this attempt.

The [failed packet](../evidence/m4/20260920T155738Z/publication.json) retains
the controller's original unsuccessful cleanup. The linked
[recovery packet](../evidence/m4-recovery/20260920T155738Z/20260922T183110Z/publication.json)
records the later empty checks. Both were verified and copied unchanged into
the repair branch; the raw receipts remain private in the execution checkout.

## Measured waiting

The Terraform console log reports per-resource elapsed create times. These
operations overlapped; their durations must not be added as sequential waits.

| Observation | Duration or UTC time | Evidence |
|---|---|---|
| NAT gateway create | 1m34s | `97-controller-console.log` |
| MSK Serverless create | 1m47s | Same log |
| RDS create | 5m39s | Same log |
| EKS control-plane create | 9m58s | Same log |
| Managed node group still creating | Last emitted wait: 18m30s; never completed | Same log |
| Worker instances launched | 17:50:39Z and 17:51:00Z; a replacement at 17:57:07Z; the first powered off at about 17:58:25Z | `97-live-instances.json`, `21-inventory-after.json`, `97-worker-1-console.json` |
| Node objects in the snapshot | 17:51:18Z (second instance) and 17:57:25Z (replacement) | `97-node-readiness.json` `spec.providerID` and creation timestamps |
| Node group health while creating | `CREATING` with no health issues at about 17:54Z, 18:00:31Z and 18:05:40Z | `97-live-nodegroup-health.json`, `97-worker-health-*.json` |
| Conservative session start to destroy start | 29m38s | `00-session.json` and `controller-state.json` |
| Orphaned node-group deletion | Requested 18:10:29Z; still listed at about 18:13Z; the recovery waiter returned before `aws-init` at about 18:26Z | `97-orphan-nodegroup-delete.json`, `97-cleanup-nodegroups-current.json`, file times |
| Recovery EKS cluster deletion | 3m50s | `97-recovery-aws-down.log` |
| Destroy start to verified recovery | 23m05s | Original controller and recovery `20260922T183110Z` |
| Session start to verified recovery | 52m43s | Same receipts |

The node snapshot was collected around 18:08Z. Its earlier registration and
condition timestamps show when a readiness check became possible; they are
not evidence that an operator inspected those nodes then. The operator did poll
`describe-nodegroup` three times before that. Each response was `CREATING` with
an empty health-issue list, which could not show unready nodes. The 17:57:25Z
node belongs to the replacement instance, so it is not a second worker's
registration time. The live diagnosis came late in provisioning. An earlier
check of the first registered node and the missing add-on could have ended the
attempt sooner; the exact time saving is unknown.

EKS provisioning itself took minutes, consistent with
[AWS's creation guidance](https://docs.aws.amazon.com/eks/latest/userguide/create-cluster.html).
One failed sample cannot establish normal worker readiness or teardown
percentiles. The 23m05s includes the orphan's deletion, manual recovery and
verification. It is consistent with keeping the existing 30-minute reserve and
continuing cleanup past it when needed; it does not measure a normal teardown.

## Cause and local repair

The saved plan had `bootstrap_self_managed_addons: false` and planned only
`eks-pod-identity-agent`. EKS module 21 sets that bootstrap field to false.
The required VPC CNI, CoreDNS and kube-proxy add-ons were absent. The missing
CNI explains the observed network-initialization failure; no further live
repair was attempted to test that diagnosis.

The local repair explicitly declares all four managed add-ons. VPC CNI and the
Pod Identity agent use the module's before-compute placement, so they do not
wait for the node group. That placement does not make compute wait for them:
the module starts them with the cluster and delays compute by 30 seconds.
CoreDNS and kube-proxy wait for the node group.
AWS `describe-addon-versions` confirmed these default, amd64-compatible
versions for Kubernetes 1.35 in `us-east-1` on 2026-09-22; that response was
not saved:

| Add-on | Version |
|---|---|
| VPC CNI | `v1.22.4-eksbuild.3` |
| CoreDNS | `v1.13.2-eksbuild.31` |
| kube-proxy | `v1.35.3-eksbuild.29` |
| Pod Identity agent | `v1.3.10-eksbuild.3` |

The mocked Terraform runtime test now requires the complete pinned add-on set.
`make terraform-check` passed for all three stacks, including the guardrail
test and seven dev tests. Replacing only the fixed Terraform configuration
with its original version made the new assertion fail; the passing fixed
configuration was restored afterward. These checks do not prove live readiness.

Second reviews by Claude and Codex found gaps that the same change now closes:

- The mocked test cannot see add-on placement or the worker role's policies,
  and it still passed with `before_compute` removed from the CNI. The plan gate
  in `scripts/check-aws-plan.py` now reviews the saved plan's end state. With
  self-managed bootstrap disabled, it requires the four add-ons with known
  versions, the CNI as a before-compute add-on, and `AmazonEKS_CNI_Policy` on
  every managed node group's role. Run against this attempt's saved plan, it
  rejects the missing CNI, CoreDNS and kube-proxy and the unresolved Pod
  Identity version.
- Every stop of a running apply now starts with SIGINT, and the controller
  waits for the whole apply process group to exit before cleanup, killing
  members that outlive the grace. Heartbeats do not reset escalation timers.
  If termination remains unconfirmed after escalation, cleanup is blocked and
  the owner must confirm termination before manual recovery. The controller
  and failed packet preserve the unknown exit and unverified cleanup.
  `make aws-live-stop` takes an
  `AWS_STOP_REASON`, and the capture helper records `capture_failed`.
- The EKS and VPC modules are pinned exactly. `~> 21.25` had resolved 21.25.1
  during September 20 staging, retained for this September 22 run; the
  September 22 repair resolved 21.25.3.

New tests cover the gate, the stop signal, the process-group wait and the
stop reason. The gate and controller tests were run against the old behavior
and fail there. These checks still do not prove live readiness.

The follow-up controller repair on 2026-09-22 passed `make test` (all seven Go
modules with the race detector and 216 Python tests), `make lint` (12 passed,
none failed or skipped) and `make terraform-check` (all three stacks; eight
mocked tests). It adds tests with heartbeats running during escalation and
checks the complete apply-to-cleanup handoff. Restoring the timer reset,
allowing cleanup after an unconfirmed apply, accepting a surviving process
group, or letting repeated signals skip exit confirmation in disposable copies
made the corresponding tests fail. The failed-publication test preserves an
unknown apply exit, cleanup that was not run, and the original failed snapshot
after a later recovery. No AWS calls were made for these checks.

A second review of that repair found four smaller issues, fixed the same day:

- A test now covers an apply exit that arrives with the final deadline.
- `WaitDelay` bounds the wait for an output pipe a group member keeps open, so
  the group check applies to every writer.
- Unconfirmed-exit errors name their process group, and the runbook shows how
  to check it.
- The README and roadmap say that unconfirmed termination blocks destroy.

Checking those fixes exposed a fifth issue, which the review had missed. macOS
reports EPERM for a process group whose last member has exited but awaits
reaping, and the group waiter treated EPERM as unconfirmed exit. A probe
landing in that window could block cleanup falsely. The waiter now keeps
polling through EPERM, and still fails if the group outlasts `SIGKILL`.

Removing each protection in a disposable copy made its new test fail. The
controller suite also passed on Linux in a container with an init process.
Without one, unreaped zombies kept process groups alive, and the controller
blocked cleanup instead of destroying.

AWS documents the functions and permissions of these components in
[AWS add-ons](https://docs.aws.amazon.com/eks/latest/userguide/workloads-add-ons-available-eks.html).

## Lessons applied to later applications

The review checked shape, cost, tags, identity and image provenance without
asserting the complete cluster dependency set. The full plan already exposed
the disabled bootstrap and missing networking add-ons. Those were available
to detect before any hourly apply; additional paid discovery was unnecessary
for that omission.

Local rehearsal exercised an existing local cluster. Its success did not prove
that the AWS module would supply networking, DNS or service routing. CNI was
the observed failure; CoreDNS and kube-proxy were also absent from the plan,
but their application effects were never exercised.

The shared [application guide](../application-validation.md#review-runtime-dependencies)
now asks each app to trace capability ownership, version/order, IAM/network
access, readiness and cleanup through its actual deployment. The
[AWS runbook](../runbook-aws-relay.md#observe-infrastructure-while-apply-runs)
adds early read-only node and system-workload observations, time budgeting
backward from cleanup, and partial-create recovery. `AGENTS.md` directs new
app and shared-stack work to those checks; the PR template links the evidence.

The add-on assertion, the plan gate's dependency review and the graceful stop
are automated. Provisioning health observations and reconciliation of
resources left outside state remain operator steps: a graceful stop makes a
dropped create less likely but cannot rule it out. The controller has no
Kubernetes health watchdog and does not discover or delete untracked
resources. The documentation does not claim those protections are automated.

Local rehearsal also hid a capacity limit. Minikube allows 110 pods per node;
each `t3.medium` worker here reported 17. After the host-network DaemonSets and
the platform stack, two workers leave room for about four delivery pods, while
KEDA may request 12. The capture's replica series counts desired replicas, so
it would not show the shortfall. The owner then chose a third worker, which
fits all 12; see ADR 0010's
[worker capacity amendment](../adr/0010-live-aws-relay-contract.md#worker-capacity-amendment-accepted-2026-09-22).

Successful cleanup preserved the original failed demonstration. The generic
`evidence_complete` stop string, a passing Terraform check, and an accepted
delete request each prove less than their names might suggest. Closure still
depends on the actual application evidence, final state/inventory and billing
conditions, with failures preserved separately from recovery.

## Remaining work

Keep #97 open. The original source and run ID cannot be reused for a paid retry:
the session was spent and the staged images were deleted. A future attempt
needs an explicitly selected repaired candidate, the required qualification,
new staging and a newly reviewed plan, plus separate paid-run approval.

Before another GO, also:

- record running replicas and Pending pods beside desired replicas before
  relying on scaling evidence, and check the three-worker arithmetic in the
  runbook's [infrastructure prerequisites](../runbook-aws-relay.md#infrastructure-prerequisites)
  against the saved plan;
- save the dated `describe-addon-versions` output for the four pins with the
  staging evidence;
- update #97's body, which still describes the pre-attempt approval wait.

This attempt's settled cost needs the runbook's
[failed-attempt path](../runbook-aws-relay.md#settled-cost-after-a-failed-attempt),
no earlier than 2026-09-24T18:31:18Z. The scripted collector requires a
completed controller, and this receipt stays `cleanup_failed`.

The application-level AWS behavior, trace, autoscaling, DLQ, replay and visual
evidence remain untested. Preserve this failed attempt and its recovery
evidence. Any later completion claim also needs the required settled billing
evidence and dated documentation updates.
