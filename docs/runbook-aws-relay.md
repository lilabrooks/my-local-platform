# Live AWS relay validation runbook

Status: Contract accepted on 2026-09-05. No command on this page authorizes an
AWS mutation.

This runbook implements the contract in
[ADR 0010](adr/0010-live-aws-relay-contract.md). It is the shared handoff for
issues #92 through #97. Commands that do not exist yet are named as contracts;
their implementing issue must keep the behavior and terminal conditions here.

The live run needs three separate approvals:

1. accept the contract in #91;
2. authorize cheap-tier staging in #96;
3. authorize the hourly EKS, MSK, and RDS apply in #97.

An earlier approval does not imply a later one.

## Fixed runtime

| Component | Shape |
|---|---|
| EKS | Kubernetes 1.35 in standard support, two desired Spot `t3.medium` nodes, range 1 to 3 |
| MSK Serverless | one cluster, 12-partition delivery topic, one-partition DLQ |
| RDS | one private single-AZ `db.t4g.micro`, 20 GB gp3 |
| Relay | two ingest pods; deliver scales from 1 to 12; one relay image digest |
| Sink | one private `ClusterIP` pod |
| Platform | KEDA, ArgoCD, Prometheus, Grafana, Tempo in EKS |
| ECR | immutable `mlp-dev/relay` and `mlp-dev/sink` repositories; git SHA tags, digest deployments |

Only the EKS API endpoint is public, limited to the operator's current address.
Use `kubectl port-forward` for the sink, ArgoCD, Grafana, and Tempo. Do not add
an ingress or load balancer during the session.

Pod Identity associations belong to `relay-ingest`, `relay-deliver`, and
`keda-operator`. The sink has no AWS role. KEDA uses `identityOwner: keda`.

## Stop conditions

Do not apply if any of these is false:

- `aws sts get-caller-identity` is the intended account and role;
- no account id or credential value is present in a tracked or staged file;
- Kubernetes 1.35 is in EKS standard support in `us-east-1`;
- current published inputs keep the modeled shape at or below $1.25/hour;
- the reviewed plan has no more than one EKS cluster, one MSK cluster, one RDS
  instance, one NAT gateway, three worker nodes, or 13 topic partitions;
- every hourly resource is opt-in, tagged `Project=my-local-platform` and
  `Ephemeral=true`, and appears in the destroy plan;
- the local rehearsal for deploy, demo, abort, evidence, redaction, and cleanup
  passed at the exact commit being staged;
- the repository owner has separately authorized this hourly apply.

After apply, any unexpected resource, public workload endpoint, identity
failure, shape-gate failure, or standard-support mismatch stops the demo and
starts destroy.

## Clock and spend

Create a UTC run id before staging and record it in
`.evidence/m4/<run-id>/00-session.json`. Record the exact commit, region,
operator, start time, 2-hour-30-minute destroy deadline, 3-hour hard deadline,
$1.25/hour shape cap, and $5.00 maximum.

The clock starts when the first hourly resource enters a billable state. Start
destroy at 2 hours 30 minutes even if evidence is incomplete. Do not extend the
sample to obtain a successful result. The executing repository owner owns the
timer and cleanup.

AWS Budgets is a forgotten-resource alarm. It is not the session stop control,
because its billing data cannot arrive fast enough.

## Configuration and secrets

The AWS overlay changes only environment-specific values:

```text
KAFKA_AUTH_MODE=aws_msk_iam
KAFKA_BOOTSTRAP=<MSK IAM bootstrap brokers>
AWS_REGION=us-east-1
DATABASE_URL=<private RDS connection string>
OTEL_EXPORTER_OTLP_ENDPOINT=<in-cluster collector>
```

Topic names, the `relay-deliver` group, retry schedule, application behavior,
metrics, and traces remain the M3 contract.

The relay image contains `/relay` and the operator-only `/relay-replay`
command. Both use the settings above for every broker operation. In IAM mode,
relay loads the ambient AWS SDK credential provider once at startup and keeps
its refresh-aware credential cache. The adapter asks the pinned AWS MSK IAM
signer for a fresh 15-minute token from kafka-go's per-connection SASL `Start`
call. It does not cache or log the signed token. The mechanism name is
`OAUTHBEARER`, and the initial response is `n,,`, a control-A,
`auth=Bearer <token>`, then two control-A bytes.

IAM mode always uses TLS 1.2 or newer with system trust and per-broker server
name verification. There is no insecure-skip or plaintext IAM setting. The
runtime reports whether failure occurred while generating or validating the
token, during the Kafka SASL exchange, or during the requested Kafka operation,
without adding credentials or token bytes to its own errors.

The AWS replay path runs `/relay-replay` in a short-lived Job under the
`relay-deliver` service account. It must not run under `relay-ingest`, whose IAM
role cannot alter consumer-group offsets. #94 owns that Job rendering and the
identity trace; the binary and its shared transport are supplied by #92.

RDS manages its master password in Secrets Manager. A second secret holds the
controlled sink signing key. The staging helper must:

1. disable shell tracing and set `umask 077`;
2. create a private temporary directory;
3. fetch both values without putting them in arguments or logs;
4. stream the namespace-scoped Kubernetes Secret to `kubectl apply` over
   standard input;
5. seed the RDS subscription with the same signing key;
6. remove the temporary directory on success, error, or interruption.

Do not copy secret values into git, images, Terraform variables or outputs,
shell history, screenshots, or evidence files.

## Before staging

The #96 staging issue must capture these files under the raw evidence directory:

- `01-identity.txt`: caller identity with the account id redacted in the
  sanitized copy, plus the EKS standard-support result;
- `02-prices.md`: date, official URLs, rates, quantities, arithmetic, and gate;
- `03-plan-summary.json`: resource types, addresses, counts, and shape result;
- `04-inventory-before.json`: tagged and service-native inventories;
- `05-images.json`: commit SHA, immutable tags, and ECR digests.

The inventory combines `resourcegroupstaggingapi get-resources` with explicit
EKS, MSK, RDS, EC2, EBS, ELB, ECR, NAT gateway, and CloudWatch log-group
queries. The tagging API alone is insufficient.

Trace one image and configuration value through its producer, generated AWS
Application, ArgoCD load, Deployment, running pod, and evidence output. Trace
each Pod Identity association through service account, role, policy resource,
consumer, and a denied action outside its authority.

## Live proof

The paid run repeats M3's outcome on the fixed AWS topology:

1. ArgoCD reports every application synced and healthy.
2. Post one event and repeat it with the same idempotency key. Both responses
   name the same event and MSK contains one delivery record.
3. Read the successful and exhausted subscriber attempt histories from RDS.
4. Follow one complete trace through ingest, Kafka produce, consume, and every
   webhook attempt.
5. Send the 600-event, 16-tenant load. Record lag rising, deliver scaling from
   one toward twelve, lag reaching zero, and replicas returning to one.
6. Capture exhausted and poison records from the DLQ with their source
   coordinates.
7. Replay selected records and match their resulting deliveries.

Save the machine-readable results as `10-event.json`, `11-attempts.json`,
`12-metrics.txt`, `13-trace.json`, `14-keda.txt`, `15-dlq.json`, and
`16-replay.json`. The screenshot set is `grafana-lag.png`, `tempo-trace.png`,
`argocd-apps.png`, and `terminal-demo.png`.

A failed step is valid evidence. It does not authorize more paid debugging.
Record the failure, start destroy, and return to local rehearsal.

## Destroy is part of the run

Success and failure both end in the same sequence:

1. stop evidence collection at the destroy deadline;
2. run the state-backed dev-stack destroy;
3. delete service-created M4 CloudWatch log groups;
4. confirm Terraform has no dev resources;
5. repeat every tagged and service-native inventory from the before snapshot;
6. capture provisional month-to-date and Cost Explorer output;
7. preserve the bootstrap state bucket.

Write the destroy transcript and exit status to `20-destroy.txt`, the complete
after inventory to `21-inventory-after.json`, and the provisional bill to
`22-cost-immediate.txt`.

Cleanup is complete only when no M4 EKS cluster, MSK cluster, RDS instance, NAT
gateway, load balancer, worker instance or volume, dev ECR repository, or M4
log group remains. An empty tagging response on its own does not pass.

No earlier than 48 hours after destroy, capture the settled attributed cost in
`23-cost-final.txt`. Wait longer if AWS still marks the data incomplete. #97
closes only after the final cost and empty inventories are present.

## Sanitizing evidence

Raw files stay in ignored `.evidence/m4/<run-id>/`. Copy only sanitized files
to `docs/evidence/m4/<run-id>/`.

Replace account ids, account-bearing ARNs, ECR registry hosts, RDS and MSK
endpoints, usernames, email addresses, public IP addresses, and secret values
with stable bracketed tokens. Preserve timestamps, region, resource counts,
non-account names, commit SHAs, image digests, event ids, trace ids, partitions,
offsets, metrics, and exit status.

Before staging the sanitized directory, scan it for the known account id,
endpoints, and secret values. Any match blocks the commit.

## Issue handoff

| Issue | Required exit |
|---|---|
| #91 | owner accepts ADR 0010 and this runbook |
| #92 | IAM/TLS transport passes local tests |
| #93 | disabled and enabled Terraform plans pass their resource-shape checks |
| #94 | rendered workloads preserve this topology, identity, and evidence path |
| #95 | the full runbook, including abort and cleanup, passes locally |
| #96 | cheap staging, images, budget alarm, current prices, and exact plan are separately approved and captured |
| #97 | paid proof ends in destroy, empty inventories, and a settled final cost |

Changing identity provider, public exposure, resource or partition counts,
duration, spend limits, or cleanup proof requires a contract amendment and a
new owner decision before staging continues.
