# 10. Live AWS relay validation contract

Date: 2026-09-05
Status: Accepted

## Context

M3 proved the relay application locally against real Kafka and Postgres, then
repeated the Kubernetes demonstration with KEDA, Prometheus, Grafana, Tempo,
and ArgoCD. M4 has a narrower purpose: test the AWS control plane, IAM
transport, private networking, and teardown path that local infrastructure
cannot reproduce.

That test creates EKS, MSK Serverless, RDS, a NAT gateway, and worker nodes in
a personal AWS account. The contract must therefore be fixed before code makes
those choices implicit. It also has to separate contract approval, cheap-tier
staging, and the later hourly apply into three owner decisions.

The repository owner accepted this contract on 2026-09-05. No AWS resource was
created while preparing this decision.

## Decision drivers

1. Exercise the same relay behavior and evidence path that passed in M3.
2. Give each pod only the AWS permissions needed for its role.
3. Keep account identifiers and secret values out of git and Terraform output.
4. Bound the paid experiment with resource counts and elapsed time, since an
   AWS Budgets alert cannot stop a short session.
5. Make destroy the terminal step after either success or failure.
6. Leave enough evidence to tell an application failure from identity,
   networking, scaling, or cleanup failure.

## Decision

### One fixed topology

The M4 runtime uses one `us-east-1` VPC and the existing two private subnets.
Its only public entry point is the EKS API endpoint, restricted to the
operator's current address during the session. Workloads and data services
stay private.

| Surface | Contract |
|---|---|
| EKS | One Kubernetes 1.35 cluster in standard support; one managed Spot node group with two desired `t3.medium` nodes, minimum one and maximum three; default API-data envelope encryption with an AWS-owned key |
| Relay | Two `relay-ingest` replicas and a KEDA-managed `relay-deliver` Deployment with 1 to 12 replicas; both use the same image digest |
| Sink | One controlled sink behind a `ClusterIP` Service; no ingress or load balancer |
| Kafka | One MSK Serverless cluster; `mlp.relay.deliveries` has 12 partitions and the DLQ has one |
| Database | One private, single-AZ RDS PostgreSQL `db.t4g.micro` instance with 20 GB gp3 storage and no final snapshot |
| Deployment | ArgoCD app-of-apps with the `mlp-root`, `mlp`, and disabled `default` project boundary from ADR 0009 |
| Evidence | In-cluster Prometheus, Grafana, and Tempo, reached with authenticated `kubectl port-forward` |
| Images | Two private immutable ECR repositories, `mlp-dev/relay` and `mlp-dev/sink`; exact 40-character git SHA tags, deployed by digest |

The operator creates both topics before workloads start. Runtime pods do not
receive topic-management permissions. The sink remains internal because the
question is whether an EKS pod can deliver, sign, retry, dead-letter, and
replay through the private AWS path. Public ingress and delivery across the
internet are outside this experiment.

ArgoCD continues to read tracked manifests from git. A generated, untracked AWS
Application supplies the two ECR digests through Kustomize image overrides.
The generated file also names stable ConfigMap and Secret objects created by
the staging runbook. Account-specific registry hosts and service endpoints do
not enter git.

The worker type differs from the current Terraform placeholder. M3 measured a
3.64 GiB peak and could not start the supporting stack under a 3 GiB limit.
Two `t3.small` nodes provide only 4 GiB before EKS system overhead. Two
`t3.medium` nodes provide 8 GiB and preserve useful headroom while keeping Spot
capacity and the three-node ceiling.

Kubernetes 1.35 encrypts all Kubernetes API data with an AWS-owned key by
default. The EKS module's optional customer-managed key is disabled because it
would outlive this short run in `PendingDeletion`; M4 has no requirement for a
customer-managed key.

### EKS Pod Identity

M4 uses EKS Pod Identity rather than IRSA. AWS recommends Pod Identity for new
EKS workloads when the SDK supports it, and the pinned KEDA 2.20.2 build uses
an AWS SDK version newer than the published Pod Identity minimum.

Four service accounts define the boundary:

| Service account | AWS role authority |
|---|---|
| `relay-ingest` | connect to MSK, describe the delivery topic and consumer group, write the delivery topic, and read broker group offsets for metrics |
| `relay-deliver` | connect to MSK, describe/read the delivery topic and group, alter the consumer group, and write the DLQ |
| `keda-operator` | connect to MSK and read only the delivery topic and `relay-deliver` group lag |
| `sink` | none |

Policies use the exact cluster, topic, and group ARNs created for the run. KEDA
uses `identityOwner: keda`, so the scaler uses the operator association rather
than borrowing workload credentials. The implementation rehearsal must prove
that path before an hourly apply is authorized. EC2 instance metadata is
unavailable to ordinary pods, preventing a failed pod association from falling
through to the node role.

IRSA is the fallback if the rehearsal cannot make KEDA's operator-owned Pod
Identity path work with the pinned chart. Changing to IRSA amends this ADR and
requires owner approval before #96; it is not an in-session experiment.

### Images and configuration

Both ECR repositories use immutable tags and scan-on-push. A build pushes the
commit SHA tag once, records the returned digest, and deploys only that digest.
The ingest and deliver Deployments must name the same relay digest.

The EKS node group uses `t3.medium`, so the staged relay and sink images use
`linux/amd64` even when the operator's Docker host is `arm64`. The staging
helper checks a new local image before push. If the immutable commit tag already
exists, it pulls that digest for `linux/amd64` instead. Both paths check the OCI
revision label and repository controls.

The local and AWS paths keep one application image. #92 adds only this runtime
switch:

| Setting | Local | AWS |
|---|---|---|
| `KAFKA_AUTH_MODE` | `none` | `aws_msk_iam` |
| `KAFKA_BOOTSTRAP` | compose or minikube listener | MSK Serverless IAM bootstrap brokers |
| `AWS_REGION` | unset | `us-east-1` |
| `DATABASE_URL` | local Postgres | private RDS endpoint and staged credentials |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | local collector | in-cluster collector |

IAM mode always dials TLS. A plaintext broker fails the TLS handshake. Every
other relay setting, event contract, topic name, consumer-group name, retry
schedule, metric name, and trace contract stays the same.

The #92 implementation keeps the unauthenticated path on kafka-go's existing
TCP defaults. IAM mode installs one shared transport on ingest production, lag
polling, deliver consumption, DLQ production, and replay. It uses the pinned
AWS signer through kafka-go's per-connection SASL start boundary. Relay loads
the ambient AWS credential provider once at startup and keeps the AWS SDK's
refresh-aware credential cache. Each new connection signs a fresh token with
the current cached credentials instead of reloading the provider chain. The
signer currently issues 15-minute tokens.
TLS uses system roots, a minimum of TLS 1.2, and kafka-go's per-address server
name inference; certificate and hostname verification cannot be disabled by
runtime configuration.

### Secret delivery

RDS manages its master password in Secrets Manager. A separate Secrets Manager
secret holds the controlled sink signing key. Neither value appears in source,
an image, a Terraform variable, Terraform output, a command argument, or shell
history.

The staging command disables shell tracing, sets `umask 077`, retrieves both
values into a temporary directory, and streams a Kubernetes Secret manifest to
`kubectl apply` over standard input. A trap removes the temporary directory on
success, error, or interruption. The Secret supplies:

- `DATABASE_URL` to relay and the database seed Job;
- the signing key to the sink and seed Job.

The seed Job inserts the same signing key into the subscription row. Relay
therefore continues to read subscriber secrets from Postgres, as it does
locally. The temporary Kubernetes Secret is scoped to namespace `mlp`, read by
only those service accounts, and disappears with the cluster.

Mounting Secrets Manager through the Secrets Store CSI driver was considered.
The current scratch images read environment variables, so that choice would
add a driver and file-based configuration changes to a short experiment.
Direct Secrets Manager calls from application code would add AWS SDK authority
to the sink and relay. Both alternatives are deferred unless a persistent AWS
deployment makes rotation during pod lifetime a real requirement.

### Spend boundary

Rates were checked on 2026-09-05 against AWS's public `us-east-1` price pages
and price-list file. The fixed shape is approximately $1.02/hour before small
data-transfer, request, log, and image-storage charges:

| Item | Checked rate | Contract quantity | Approximate hourly cost |
|---|---:|---:|---:|
| MSK Serverless cluster | $0.75/cluster-hour | 1 | $0.750 |
| MSK partitions | $0.0015/partition-hour | 13 | $0.020 |
| EKS standard-support control plane | $0.10/cluster-hour | 1 | $0.100 |
| `t3.medium` on-demand upper bound | $0.0416/instance-hour | 2 | $0.083 |
| NAT gateway | $0.045/hour | 1 | $0.045 |
| NAT public IPv4 address | $0.005/hour | 1 | $0.005 |
| RDS `db.t4g.micro` | $0.016/instance-hour | 1 | $0.016 |
| RDS gp3 | $0.115/GB-month | 20 GB | $0.003 |

The plan gate rejects any shape above $1.25/hour, more than one EKS, MSK, RDS,
or NAT resource, more than three worker nodes, a non-Spot worker group, more
than 13 Kafka partitions, or an EKS version outside standard support. On-demand
node pricing is used for the gate even though the plan requests Spot.

The paid-window target is three hours from the start of apply. At 2 hours 30
minutes the run stops gathering evidence and starts destroy, leaving 30
minutes for cleanup. Crossing three hours is a failed deadline, but the
controller keeps an active destroy running because stopping cleanup would
leave the account in a worse state. The approved maximum is $5.00.
Any plan-gate failure, identity mismatch, unexpected resource, support-status
failure, or modeled rate above $1.25/hour blocks apply. Any such discovery
after apply aborts the demonstration and starts destroy.

`make aws-live-run` creates the timestamp immediately before apply and keeps a
Go controller in the foreground. Success, failure, `SIGINT`, `SIGTERM`, an
operator stop, and the 2-hour-30-minute deadline all enter the same cleanup
path. The controller interrupts an apply that is still running at that
deadline, waits 30 seconds, escalates to `SIGTERM`, waits 10 seconds, and then
sends `SIGKILL`; cleanup starts after a final 5-second bound. A second operator
signal advances that sequence immediately. Hourly apply requires a fresh
controller heartbeat bound to the run, commit, and live controller process.
Cleanup runs state-backed destroy, removes project-prefixed EKS and MSK log
groups, requires empty dev Terraform state, checks the service-native inventory
including project-tagged Elastic IPs, and captures immediate cost output. It
records an overdue result if cleanup crosses the 3-hour deadline and never
kills an active destroy.

Cleanup verification is the empty Terraform state plus the empty explicit
inventory. Destroy, log cleanup, transcript, and Cost Explorer exits remain
separate evidence. A Cost Explorer failure can therefore fail the evidence run
without telling the operator that resources remain.

The account-wide `mlp-live-aws-monthly` budget lives in a persistent Terraform
stack with its own remote state and destroy protection. It sends actual-spend
email at $4 and $5 and forecast email at $5. Any active budget alarm blocks a
new hourly plan. Billing data arrives too late for a three-hour experiment, so
the budget remains a forgotten-resource alert. Its $5 amount is a monthly
account ceiling, separate from the $5 session maximum. A single run can trip
the forecast alarm and block another hourly run for the rest of the alarm
period. The executing repository owner owns the foreground controller and
cleanup; there is no cleanup handoff.

### Evidence and redaction

Each attempt gets a UTC run id such as `20260905T193000Z`. Raw evidence stays
under ignored `.evidence/m4/<run-id>/`. Sanitized evidence intended for git
lives under `docs/evidence/m4/<run-id>/` and uses these names:

| File | Required content |
|---|---|
| `00-session.json` | run id, approved commit, region, start/deadline/destroy times, and contract limits |
| `01-identity.txt` | redacted repository and caller identity, state backend, budget, quota capacity, regional offerings, and EKS support |
| `02-prices.md` | dated source URLs, rates, quantities, arithmetic, and $1.25/hour gate result |
| `03-plan-summary.json` | capture time, Terraform input hash, resource addresses, types, counts, and enforced topology result; no secret values |
| `04-inventory-before.json` | tagged inventory plus service-native EKS, MSK, RDS, EC2, EBS, Elastic IP, ELB, ECR, NAT, and log-group queries |
| `05-images.json` | source commit, both immutable tags, and deployed digests |
| `06-go-no-go.json` | one run-and-commit-bound decision over identity, prices, plan, empty inventory, images, capture order, limits, and cleanup owner |
| `10-event.json` | accepted event, idempotent repeat, and persisted event identity |
| `11-attempts.json` | successful and exhausted subscriber attempt histories |
| `12-metrics.txt` | lag, group members, assignments, idle members, and KEDA replica series |
| `13-trace.json` | one complete ingest, produce, consume, and webhook-attempt trace |
| `14-keda.txt` | scale from one, drain to zero lag, and return to one |
| `15-dlq.json` | exhausted and poison records with source coordinates |
| `16-replay.json` | replayed event ids and resulting deliveries |
| `20-destroy.txt` | destroy command, exit status, and empty dev Terraform state |
| `21-inventory-after.json` | the same inventories as `04`, with no runtime resource remaining |
| `22-cost-immediate.txt` | provisional Cost Explorer and month-to-date output |
| `23-cost-final.txt` | final attributed cost captured after billing data has settled |

The screenshot set is `grafana-lag.png`, `tempo-trace.png`,
`argocd-apps.png`, and `terminal-demo.png`. Console screenshots are optional;
the command output above is the source evidence.

Sanitization replaces account ids, account-bearing ARNs, ECR registry hosts,
RDS and MSK endpoints, usernames, email addresses, public IP addresses, and all
secret values with stable bracketed tokens. It preserves timestamps, region,
resource types and counts, non-account resource names, commit SHAs, image
digests, event ids, trace ids, partition numbers, offsets, metrics, and command
exit status. A scan for the known account id, endpoints, and secret values must
return no match before sanitized evidence is staged.

The tagging API is not cleanup proof because it omits some untagged or
service-created resources. The before and after inventories therefore pair it
with service-native queries. Destroy is complete only when Terraform reports
no dev resources and the explicit queries find no M4 EKS cluster, MSK cluster,
RDS instance, NAT gateway, Elastic IP, load balancer, worker instance or
volume, dev ECR repository, or M4 CloudWatch log group. The bootstrap state
bucket survives.

The immediate cost capture is provisional. `23-cost-final.txt` is captured no
earlier than 48 hours after destroy, or later if AWS still reports incomplete
data. Closing #97 requires that final cost and the empty inventories.

## Issue handoff and terminal conditions

| Issue | Produces | Gate handed to the next issue |
|---|---|---|
| #91 | this contract and the AWS runbook | explicit owner acceptance |
| #92 | locally tested MSK IAM transport | TLS/IAM behavior proven without a cluster |
| #93 | opt-in Terraform runtime | default plan has no hourly resources; enabled plan passes shape checks |
| #94 | rendered AWS workload and evidence path | one digest/config/identity path traced through every consumer |
| #95 | local rehearsal | deploy, evidence, abort, destroy, and redaction scripts pass without AWS |
| #96 | cheap-tier stage and reviewed plan | separate owner approval, immutable images, budget alarm, identity, prices, and exact plan captured |
| #97 | paid run and cleanup | demonstration evidence, successful destroy, empty inventory, and final settled cost |

Every issue after #91 must preserve the topology and terminal conditions above.
A change to identity provider, public exposure, resource count, partition count,
paid duration, maximum spend, or destroy test returns to the repository owner
before it enters a plan.

## Consequences

The live run answers a precise question and cannot quietly grow into a hosted
service. Pod Identity and private endpoints exercise the intended AWS access
path. The internal sink removes public-load-balancer cost and separates private
delivery from an unrelated ingress decision. Two ECR repositories make image
immutability and digest evidence direct.

The secret staging helper puts secret values in the Kubernetes API and pod
environment for the lifetime of the experiment. Namespace RBAC, the short
lifetime, and complete cluster teardown bound that exposure, but they do not
provide live rotation. The EKS API endpoint remains public and operator-
restricted for this run. Single-AZ RDS, one NAT gateway, and Spot nodes are not
a production availability design.

Pod Identity for KEDA is the largest remaining implementation uncertainty. The
contract fixes operator ownership and requires a rehearsal; it does not treat
published SDK support as proof that the complete chart path works.

## Alternatives considered

### IRSA

IRSA is documented directly by KEDA and remains a credible option. It needs a
cluster OIDC provider and per-role trust policy, while Pod Identity gives this
new cluster one association mechanism and AWS's preferred current path.
Rejected for the initial contract, retained as the pre-staging fallback.

### One ECR repository with prefixed tags

This uses the existing singular Terraform resource. It also makes retention,
immutability, and service ownership indirect. Two repositories cost only for
stored bytes and give each deployed digest one obvious source, so the shared
repository was rejected.

### Public controlled sink

A public sink would test internet egress and ingress controls, but it adds a
load balancer, certificate or plaintext exception, public DNS choice, and
another cleanup surface. It does not help answer the private EKS-to-relay
question and was rejected.

### Secrets Store CSI driver

CSI mounting avoids a Kubernetes Secret value but adds a driver and requires
the current images to read mounted files. It is deferred until a persistent
deployment needs rotation without pod recreation.

### Debug until the session succeeds

Extending the session could produce a cleaner demo but would turn a failed
experiment into an unbounded bill. The fixed timer and destroy-on-failure rule
were chosen instead. A failed attempt keeps its evidence and returns to local
rehearsal before another separately approved run.

## Rollback

Before #96, rollback is documentation-only: mark this proposal Superseded and
restore the M4 issues to waiting. After cheap-tier staging, remove the staged
images and dev ECR repositories through the reviewed Terraform path. After an
hourly apply, rollback is the same action as successful completion: stop the
demo, run the state-backed destroy, delete service-created log groups, run the
explicit inventories, and capture the final cost later.

The bootstrap state bucket and persistent cost budget are never part of
rollback or dev-stack destroy.

## Revisit when

- KEDA 2.20.2 cannot use an operator-owned EKS Pod Identity association in the
  local rehearsal;
- the controlled sink must receive traffic from outside the VPC;
- secrets must rotate while pods remain running;
- an AWS deployment is intended to persist beyond one owner-attended session;
- a required resource count or current price would break the $1.25/hour shape
  gate or $5.00 approved maximum;
- Kubernetes 1.35 leaves EKS standard support before the paid run.

## Verification

Checked on 2026-09-05 without AWS credentials or resource creation:

- current relay configuration, subscription-secret flow, manifests, KEDA
  scaler, ArgoCD projects, Terraform flags, EKS shape, RDS shape, tags, and
  destroy behavior were traced from the repository;
- KEDA 2.20.2 source uses the AWS SDK default credential chain and its
  `apache-kafka` scaler supports MSK IAM with TLS;
- AWS's EKS documentation recommends Pod Identity when supported and lists
  Kubernetes 1.35 in standard support until 2027-03-27;
- AWS's public rates were rechecked for MSK Serverless, EKS, NAT gateway,
  public IPv4, RDS compute, and RDS gp3 storage;
- the arithmetic above totals approximately $1.02/hour before variable usage.

The repository checks run for this proposal are recorded in the closing commit.
The AWS-specific claims remain predictions until #97 records the live result.

Checked on 2026-09-06 for the #92 transport implementation, without AWS
credentials or resource creation:

- `make test` passed, including an in-process fake Kafka broker that completed
  API-version negotiation, selected `OAUTHBEARER`, matched the exact initial
  response bytes, accepted authentication, and received a metadata request;
- adapter tests generated a different token on each SASL `Start`, rejected an
  expired token without putting it in the error, and checked the TLS minimum,
  system-root configuration, and server-name inference boundary;
- `make lint` passed all 11 repository checks;
- `make smoke` passed every compose check with `KAFKA_AUTH_MODE=none`, and
  `make relay-replay-verify` reset all 12 partitions through `/relay-replay`
  before redelivering the selected events;
- `make relay-demo` raised lag to 596, scaled `relay-deliver` from one to twelve
  consumers, drained lag to zero, returned to one consumer at 140 seconds,
  exercised the DLQ, and completed cluster-mode replay through the same binary.

Checked on 2026-09-06 for the #93 Terraform implementation, without creating
an AWS resource:

- an isolated `terraform init -backend=false`, `terraform validate`, and
  `terraform test` passed seven mocked runs. They checked the disabled default,
  enabled resource counts, node and partition shape, exact MSK resource ARNs,
  Pod Identity association names, the MSK subnet and security-group boundary,
  both configuration refusals, and partial-state removal;
- the plan-summary and wrapper tests passed 14 cases, including replacement and
  incomplete-plan refusal, budget and subscriber refusal, stock-system Bash,
  support-check ordering, stale-pair removal, and changed-plan rejection;
- `make test` passed every Go module and the wrapper tests, and `make lint`
  passed all 11 repository checks, including TFLint, Trivy, and Gitleaks;
- an authenticated default `make aws-plan` stopped during backend
  initialization because this account has no bootstrap state bucket. No live
  plan was produced, and creating that prerequisite remains part of the
  separately authorized #96 staging work.

Checked on 2026-09-07 for the first #95 preflight slice, without AWS
credentials or a Kubernetes cluster:

- `make aws-preflight-check` accepted the recorded M3 closure commit, disabled
  hourly flags, generated AWS objects, tracked AWS manifests, evidence roots,
  and the dry-run destroy sequence;
- the same Make target exited non-zero when `AWS_TF_ARGS` set
  `enable_eks=true`;
- a full preflight attempt from the dirty implementation branch stopped before
  lint, tests, or image builds and wrote a mode-0600 failed receipt in its
  mode-0700 run directory;
- disposable relay and sink image builds returned the supplied 40-character
  revision from `org.opencontainers.image.revision`;
- `make test` passed every Go module and 41 Python tests, `make lint` passed all
  12 checks, and `make k8s-validate` parsed 161 objects with no invalid object
  or error.

The next #95 slice fixed the capture order before the local rehearsal:

- `scripts/m4-evidence.py check-protocol` checked 16 provisional text files,
  17 final text files, 4 required visual captures, and 4 optional AWS-console
  captures that must yield to destroy;
- the capture plan puts identity, price, plan, inventory, and image work before
  the billable start, then orders event, attempt, Prometheus, trace, Kubernetes,
  DLQ, replay, log, destroy, inventory, and cost exports; its failure transition
  skips unfinished live captures and jumps directly to destroy;
- the local evidence fixture redacted account, ECR, RDS, MSK, email, public-IP,
  operator, and secret values while preserving commit SHAs, image digests,
  timestamps, and private addresses;
- publication required the 4 reviewed screenshots, checked their complete PNG
  chunk streams and dimensions, hashed every output, and detected a later file
  edit;
- provisional publication completed without the delayed final-cost file, then
  final publication added and verified `23-cost-final.txt`;
- `make test` passed every Go module and 51 Python tests, and `make lint`
  passed all 12 checks.

The 2026-09-08 #95 slice added and ran the controlled minikube SIGTERM
rehearsal:

- `make m4-k8s-sigterm M4_LOCAL_RUN_ID=20260908T010013Z` passed against the
  `mlp` context and wrote the private receipt under `.evidence/m4-local/`; the
  [sanitized tracked receipt](../evidence/m4-local/20260908/k8s-sigterm.json)
  preserves the result;
- relay-ingest returned readiness failure before exiting cleanly in 6.305
  seconds of its 45-second grace period; its blocked request then returned 202,
  published, and reached the sink;
- relay-deliver returned readiness failure before exiting cleanly in 25.278
  seconds of its 60-second grace period; the owned record completed with one
  healthy delivery and retained both successful and exhausted subscriber
  outcomes;
- cleanup released the database lock, restored the sink baseline and the prior
  KEDA annotation, and stopped every port-forward;
- the receipt binds both terminated pods and the sink to running-image revision
  `84566a7`, a prefix of its recorded source commit;
- the receipt recorded source commit
  `84566a70702b180b9a884fe2b78bada44544ad0b` and `worktree_clean: false`, so
  this is implementation evidence rather than the required clean-commit
  closure run.

A second independent review found five reachable gaps. The follow-up added
`terraform.tfvars.json` to both hourly-flag discovery and gitignore, bound the
SIGTERM receipt to the revision labels of the images actually running in
minikube, made identifier preservation yield to any overlapping declared
secret, moved tracked local evidence outside `docs/evidence/m4/`, and replaced
header-only PNG checks and fixtures with complete chunk, CRC, image-data, and
end-marker checks. The enabled-EKS JSON fixture was refused before any AWS
command. `make test` then passed every Go module and 61 Python tests, and
`make lint` passed all 12 checks.

The next 2026-09-08 slice exercised the abort controller without AWS:

- `make m4-local-abort M4_LOCAL_RUN_ID=20260908T011847Z` sent SIGTERM after
  simulated deployment began; the
  [tracked receipt](../evidence/m4-local/20260908/abort-rehearsal.json) records
  the six resulting transitions;
- the interruption skipped all eight live exports and all four required plus
  four optional screenshots, then ran destroy, explicit after-inventory, and
  immediate cost capture in order;
- the rehearsal removed mode-0600 temporary credential files and their private
  workspace, found no simulated hourly resource, and wrote no credential value
  or derivative to its receipt;
- it validated `make --dry-run aws-down` contains identity, initialized-state,
  Terraform destroy, and state-backup checks in that order.

This is evidence for local controller behavior only. Partial-resource removal
and empty service-native inventories remain live #97 outcomes. A clean-commit
preflight and the full local deploy, demo, evidence, and empty-volume bootstrap
run still remained for #95 at this point.

The 2026-09-08 full local rehearsal then ran
`make m4-local-demo M4_LOCAL_RUN_ID=20260908T014300Z`. The
[tracked receipt](../evidence/m4-local/20260908/demo-rehearsal.json) records a
203.12-second pass against minikube. Its private transcript records broker lag
peaking at 596, relay-deliver scaling from 1 to 12 and returning to 1 after lag
drained to zero, the healthy subscriber advancing while the failing subscriber
produced a fresh DLQ record, and replay completing. Its provenance inventory
bound every ready relay and sink pod to image revision `84566a7`, matching the
recorded source commit. Future receipts extract those observations from the
transcript and fail if any terminal outcome is absent.

Two failed rehearsals were useful evidence before that pass. The first found
that `relay-demo.sh` still read the retired `relay` ConfigMap after #94 moved
runtime values to `relay-runtime`. The second found that its lag-freshness query
included default-zero gauges from deliver processes. The corrected query joins
the freshness gauge to `relay_build_info{role="ingest"}` by scrape instance,
matching the readiness check. Regression tests cover both boundaries.

The ArgoCD view showed all 5 Applications Healthy and Synced. Grafana retained
the full run with maximum lag 596, maximum delivery instances 12, and final
values 0 and 1. Those views were inspected in the required order, but the
receipt remains `pending-human-capture`: the clean live run still owns the
reviewed screenshot files. The receipt also says `worktree_clean: false`, so it
is implementation evidence rather than the clean-commit closure run.

After minikube stopped, `make smoke-traces` passed against the unchanged
Compose path. Event `evt_39ef79d26d8ece21f23f17e3b1d97711` was returned by
2 concurrent requests, appeared once in Kafka, reached the healthy subscriber
once, and preserved a poison record plus 4 persisted attempts. Tempo trace
`1fb6a506089da44360c3f2fdbeac04fd` contained 16 spans across 3 services;
Grafana Explore visibly joined `relay.ingest`, `kafka.produce`,
`relay.consume`, and all 4 `relay.webhook.attempt` spans. Keeping minikube
stopped during this phase prevented the Kubernetes and Compose consumers from
sharing the same consumer group.

The final #95 closure rehearsal ran from clean commit
`6e0ae4314e770838c7008e5cefab1a94e8b3e6cd`. Preflight run
`20260908T022035Z` passed lint, 73 tests, commit-labelled image builds, both
Terraform validations, 7 Terraform contract tests, and rendered Kubernetes
validation without an AWS account call. The
[empty-volume receipt](../evidence/m4-local/20260908/empty-volume-smoke.json)
records the next 3 operator commands: `make clean`, `make up`, and `make smoke`.
They took 30.542 seconds of command runtime. All 7 smoke components passed from
new volumes; relay returned one idempotent event for 2 concurrent requests,
published one Kafka record at the first topic offset, delivered once to the
healthy subscriber, persisted 4 attempts, and produced a fresh DLQ record.

The 2026-09-08 #96 staging slices remained outside the paid window:

- local relay and sink builds for commit
  `c899375d5017247b6840ee295c37077574f14663` were inspected as
  `linux/amd64` with that exact OCI revision label;
- the inventory helper's ten service-native and tagged query paths were tested,
  including an empty runtime with cheap-tier ECR repositories still present;
- reviewed plans and inventory receipts now carry the run id and source commit,
  and plan or apply refuses a dirty worktree or a different HEAD;
- the price helper requires a reviewer, official source URLs, a check no older
  than 24 hours, and decimal-string rates. Its test fixture recomputed the fixed
  topology as $1.021850684931506849315068493/hour and refused a total over
  $1.25/hour;
- the release gate rebuilt that arithmetic, matched it to Terraform's cost
  model, checked the fixed hourly resource counts, empty runtime inventory,
  identity gates, two distinct digest-pinned images, capture order, deadlines,
  and cleanup owner, then hashed all eight inputs;
- the account helper was exercised against local fakes for repository and SSO
  identity, state-bucket controls, budget subscribers, five remaining-capacity
  checks, EKS standard support, and the fixed RDS and EC2 offerings. It refused
  an account mismatch, missing public-access block, missing budget subscriber,
  insufficient Elastic IP or six-vCPU Spot capacity, an unfulfilled Spot
  request, and a missing fixed RDS zone;
- the apply wrapper now consumes the final decision, compares its plan and
  summary hashes, rejects Terraform CLI environment arguments, and preserves
  the last-moment EKS support check. It checks the fixed plan, summary, and
  decision-packet paths before removing or reading them. The AWS renderer checks
  both supplied image digests against the same decision packet;
- `make aws-preflight-check` accepted 19 ordered captures, 17 provisional text
  files, 18 final text files, and the required visual sequence;
- `make test` passed every Go module and 121 Python tests, and `make lint`
  passed all 12 checks.

The 2026-09-08 live-run control slice remained outside AWS:

- the new Go controller tests fixed the 150- and 180-minute boundaries,
  escalated a stuck apply through the bounded signal sequence, exercised the
  live stop-file, deadline, real process group, pending-signal reset, exclusive
  lock, nested-module repository lookup, identity retries, heartbeats, and
  distinct cleanup and evidence failures;
- the apply-wrapper tests required a fresh run-, commit-, region-, and
  process-bound controller permit for hourly resources. They also rejected a
  missing or changed budget, any notification state other than `OK`, and a
  notification without a subscriber. The notification fixture uses the AWS
  CLI's floating-point threshold form;
- the persistent guardrail stack passed `terraform validate` and its 1 mocked
  contract test. The dev stack passed validation and all 7 mocked tests after
  the budget moved out of its destroy scope, including the deprecated dev
  email input as a tested no-op;
- `make aws-live-rehearse` passed the Go controller suite and 18 focused
  evidence and abort tests. `make aws-preflight-check` accepted the repository
  and capture protocol;
- `make test` passed all 6 race-enabled Go modules and 132 Python tests.
  `make lint` passed all 12 checks.

No AWS command ran for these checks. Real account identity, state, budget,
quota, regional availability, inventory, ECR, and price evidence still belong
to the separately authorized staging run.

## Sources

- [EKS Pod Identity](https://docs.aws.amazon.com/eks/latest/userguide/pod-identities.html)
- [EKS Pod Identity supported SDK versions](https://docs.aws.amazon.com/eks/latest/userguide/pod-id-minimum-sdk.html)
- [EKS Kubernetes versions](https://docs.aws.amazon.com/eks/latest/userguide/kubernetes-versions.html)
- [EKS service quotas](https://docs.aws.amazon.com/eks/latest/userguide/service-quotas.html)
- [EKS default envelope encryption](https://docs.aws.amazon.com/eks/latest/userguide/envelope-encryption.html)
- [EKS pricing](https://aws.amazon.com/eks/pricing/)
- [MSK IAM authorization actions](https://docs.aws.amazon.com/msk/latest/developerguide/iam-access-control.html)
- [MSK Serverless regions](https://docs.aws.amazon.com/msk/latest/developerguide/serverless.html)
- [MSK service quotas](https://docs.aws.amazon.com/msk/latest/developerguide/limits.html)
- [MSK pricing](https://aws.amazon.com/msk/pricing/)
- [Amazon VPC pricing](https://aws.amazon.com/vpc/pricing/)
- [RDS for PostgreSQL pricing](https://aws.amazon.com/rds/postgresql/pricing/)
- [ECR tag immutability](https://docs.aws.amazon.com/AmazonECR/latest/userguide/image-tag-mutability.html)
- [ECR `DescribeImages`](https://docs.aws.amazon.com/AmazonECR/latest/APIReference/API_DescribeImages.html)
- [Docker build platform option](https://docs.docker.com/reference/cli/docker/buildx/build/#set-the-target-platforms-for-the-build---platform)
- [AWS Budgets data refresh](https://docs.aws.amazon.com/cost-management/latest/userguide/budgets-managing-costs.html)
- [Resource Groups Tagging API `GetResources`](https://docs.aws.amazon.com/resourcegroupstagging/latest/APIReference/API_GetResources.html)
