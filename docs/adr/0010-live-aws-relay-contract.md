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
| EKS | One Kubernetes 1.35 cluster in standard support; one managed Spot node group with two desired `t3.medium` nodes, minimum one and maximum three |
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

The local and AWS paths keep one application image. #92 adds only this runtime
switch:

| Setting | Local | AWS |
|---|---|---|
| `KAFKA_AUTH_MODE` | `none` | `aws_msk_iam` |
| `KAFKA_BOOTSTRAP` | compose or minikube listener | MSK Serverless IAM bootstrap brokers |
| `AWS_REGION` | unset | `us-east-1` |
| `DATABASE_URL` | local Postgres | private RDS endpoint and staged credentials |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | local collector | in-cluster collector |

IAM mode requires TLS and rejects a plaintext broker address. Every other
relay setting, event contract, topic name, consumer-group name, retry schedule,
metric name, and trace contract stays the same.

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

The paid window is three hours from the first hourly resource entering a
billable state. At 2 hours 30 minutes the run stops gathering evidence and
starts destroy, leaving 30 minutes for cleanup. The approved maximum is $5.00.
Any plan-gate failure, identity mismatch, unexpected resource, support-status
failure, or modeled rate above $1.25/hour blocks apply. Any such discovery
after apply aborts the demonstration and starts destroy.

AWS Budgets is still configured before staging as a forgotten-resource alarm,
but it does not enforce this window. Billing data arrives too late for a
three-hour experiment. The executing repository owner owns the timer and
cleanup; there is no cleanup handoff.

### Evidence and redaction

Each attempt gets a UTC run id such as `20260905T193000Z`. Raw evidence stays
under ignored `.evidence/m4/<run-id>/`. Sanitized evidence intended for git
lives under `docs/evidence/m4/<run-id>/` and uses these names:

| File | Required content |
|---|---|
| `00-session.json` | run id, approved commit, region, start/deadline/destroy times, and contract limits |
| `01-identity.txt` | redacted caller identity and EKS standard-support result |
| `02-prices.md` | dated source URLs, rates, quantities, arithmetic, and $1.25/hour gate result |
| `03-plan-summary.json` | resource addresses, types, counts, and enforced topology result; no secret values |
| `04-inventory-before.json` | tagged inventory plus service-native EKS, MSK, RDS, EC2, EBS, ELB, ECR, NAT, and log-group queries |
| `05-images.json` | source commit, both immutable tags, and deployed digests |
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
RDS instance, NAT gateway, load balancer, worker instance or volume, dev ECR
repository, or M4 CloudWatch log group. The bootstrap state bucket survives.

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

The bootstrap state bucket is never part of rollback or dev-stack destroy.

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

## Sources

- [EKS Pod Identity](https://docs.aws.amazon.com/eks/latest/userguide/pod-identities.html)
- [EKS Pod Identity supported SDK versions](https://docs.aws.amazon.com/eks/latest/userguide/pod-id-minimum-sdk.html)
- [EKS Kubernetes versions](https://docs.aws.amazon.com/eks/latest/userguide/kubernetes-versions.html)
- [EKS pricing](https://aws.amazon.com/eks/pricing/)
- [MSK IAM authorization actions](https://docs.aws.amazon.com/msk/latest/developerguide/iam-access-control.html)
- [MSK pricing](https://aws.amazon.com/msk/pricing/)
- [Amazon VPC pricing](https://aws.amazon.com/vpc/pricing/)
- [RDS for PostgreSQL pricing](https://aws.amazon.com/rds/postgresql/pricing/)
- [ECR tag immutability](https://docs.aws.amazon.com/AmazonECR/latest/userguide/image-tag-mutability.html)
- [AWS Budgets data refresh](https://docs.aws.amazon.com/cost-management/latest/userguide/budgets-managing-costs.html)
- [Resource Groups Tagging API `GetResources`](https://docs.aws.amazon.com/resourcegroupstagging/latest/APIReference/API_GetResources.html)
