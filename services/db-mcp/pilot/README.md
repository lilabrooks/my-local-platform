# Personal AWS database MCP pilot

**Status, November 28, 2026.** The local rehearsal passed. AWS staging and the
hourly session await owner approval. The first live question is whether two
assumed roles receive different access to the same private dataset.

## Rehearse once locally

From the repository root, run `make db-mcp-pilot-test` with Go, Docker, Bash,
OpenSSL, jq, zip and unzip installed. It packages two Linux arm64 Lambda
binaries and the pinned Linux amd64 MySQL image, then runs the image on a
random loopback port. It removes its container, volume and temporary keys.

On Docker Desktop, the signed MCP reader returned 32 ready readings through
MySQL 26.10 over verified TLS. The observer received a denial before secret
retrieval. The router and executor recorded the reader's request ID, and
only the router recorded the observer's. The fixture also denied the native
reader access to the base table and a zero-row update. An unrelated CA
failed the same allowed query. The local HTTP test verifies SigV4 signatures
with fixture keys and supplies the corresponding AWS event shape.

`make db-mcp-test` covers role/session handling and all target-scoped tools.
The Terraform mock plans cover identity only, package staging, hourly dispatch
and private dependencies. Run `make terraform-check` for those and the
existing platform stacks.

## What the two roles can do

The stdio bridge signs a POST to a public HTTPS Lambda function URL. AWS_IAM
authenticates it. The router reads the account, assumed-role session ARN and
immutable role ID from the AWS event. The reader sees `pilot-mysql`. The
observer sees an empty target list. Each tool that names a target goes
through the same grant check. Session names appear in audit and can be chosen
by the caller. The two roles establish role-level access.
[Function URL authentication](https://docs.aws.amazon.com/lambda/latest/dg/urls-auth.html)
and [role session identifiers](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_variables.html#principaltable)

Only the router's role can invoke the executor within the pilot's roles.
Both caller roles explicitly deny direct Lambda invocation. An existing
administrator or principal with independent direct-invoke permission is
trusted with the event it can submit. Inspect effective permissions on both
functions, qualifiers and role trust before staging. Choose one trusted
operator from this account. Federated employee identity and cross-account
access require a separate qualification.

## Prepare the candidate

Use a clean checkout of the reviewed commit. Save artifacts outside git in
a new private directory. The hourly package includes the synthetic server's
TLS private key. Each packaging run creates a new seven-day certificate.

```sh
bash services/db-mcp/pilot/package.sh hourly "$HOME/db-mcp-pilot-candidate"
bash services/db-mcp/pilot/rehearse.sh "$HOME/db-mcp-pilot-candidate"
make db-mcp-build
```

Keep the three printed SHA-256 values with the commit and budget decision.
The rehearsal checks the archive's portable image tag, architecture, the CA
in the executor ZIP, and at least two remaining hours on the CA and leaf.
Repeat it against those exact files immediately before hourly approval.
Use `package.sh identity` when preparing only the router.

| Supplied by | Delivery and first usable observation | Cleanup owner |
|---|---|---|
| Build checkout | Go module pins, including MySQL driver v1.11.0 and Lambda Go v1.55.1. Terraform locks the AWS provider. The ZIPs contain an executable named `bootstrap` for `provided.al2023`. | Operator retires private local artifacts. |
| IAM and Lambda | Two roles trust the specified operator. The router gets immutable role IDs and no DB credentials. Both profiles must return their own STS ARN through signed `whoami`. | Pilot Terraform state owns the roles, policies, functions, URL and log groups. |
| Optional package stage | Private versioned S3 object holds the pinned amd64 MySQL image, seed SQL and TLS leaf. Bucket policy restricts reads to the operator and fixture role. EC2 fetches the exact version through an S3 gateway endpoint and verifies its SHA-256 before loading the image. | Pilot bucket is dedicated to this run. Teardown includes versions and delete markers. |
| Hourly compute | Resolve the regional recommended x86_64 ECS-optimized AL2023 AMI and record its image ID, name, owner and Docker version. Check the selected image supplies Docker, AWS CLI, OpenSSL, tar and sha256sum. Its systemd seed job runs after Docker and cloud-init. First useful signal is the fixture-ready line on the EC2 console. | Pilot state owns one `t3.small`, its encrypted 30 GiB gp3 root volume and instance profile. |
| Private database and reader | Fixture generates both passwords on the host. After TLS, count and grant checks it writes the reader password to Secrets Manager. Only the executor role can get the current value within the runtime roles. Its ZIP carries the CA. One interface endpoint serves Secrets Manager. Security groups allow executor → MySQL on 3306, with one VPC and one AZ. The signed count and a correlated executor audit prove the private path. | Pilot state owns the VPC, two private subnets, security groups, endpoints and secret. Track Lambda-managed network interfaces until deletion. |

The local demo and the hourly fixture use the digest pinned in the
[fixture Compose file](../../../local/docker-compose.db-mcp.yml).
Resolve the AMI before signing off. Keep the same image ID in the saved
plan. A version change requires a fresh comparison.
[AMI metadata](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/retrieve-ecs-optimized_AMI.html)
and [ECS service startup ordering](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ecs-agent-install.html)

## Stage and check identity

1. Obtain approval for staging mutations. Check the personal AWS identity in
   `us-east-1`, current shared budget notifications, exact remaining allowance
   and M4 state/inventory. Use the repository's existing encrypted state
   bucket with a **separate backend key** such as
   `envs/db-mcp-pilot/terraform.tfstate`. Keep its versioning, encryption and
   locking settings. Capture the empty pilot inventory before creating anything.
2. In [the pilot Terraform root](../../../infra/terraform/envs/db-mcp-pilot),
   supply the operator's account-local **IAM role or user ARN** and
   `router_zip`. Record the resolved region, state key and package hash in
   the saved plan. The identity stage contains the router, caller roles and
   logging. Supplying `fixture_bundle = { path = ..., sha256 = ... }` adds
   the private package bucket and object. Leave `hourly` null at this stage.
   Apply only the reviewed plan under the staging approval.
3. Use the two returned role ARNs in separate named AWS profiles with the
   approved source profile. Set a distinct, fixed `role_session_name` in
   each so the CLI and Go SDK report the same session ARN. Set
   `PILOT_READER_PROFILE`, `PILOT_OBSERVER_PROFILE` and `PILOT_URL` from
   this deployment. Run the bounded check below. The live verifier requires
   actual AWS access denial on direct invocation, both signed identities,
   the reader's granted target, empty observer discovery and denial on a
   known target. In the cheap stage the entitled query reports `unavailable`.

```sh
MLP_USE_REAL_AWS=1 PILOT_PHASE=identity go -C services/db-mcp test \
  -tags=awspilot -count=1 -timeout=2m -v -run '^TestLivePilot$' ./internal/pilot
```

If AWS returns different assumed-role context from the locally modeled
event, stop here. An identity failure has no dependency on paid compute.
Store request IDs and outcomes with the run. Avoid saving raw AWS events,
signed headers, result rows and credential payloads.

## Hourly run and cost

The candidate targets `us-east-1`. For one hour, the observed list rate for
a Linux `t3.small` is about $0.021. The single interface endpoint adds
about $0.01, 30 GiB of gp3 about $0.0033 and one secret about $0.00056.
The S3 gateway endpoint has no hourly fee. The fixture archive measured
about 284 MiB. Allow for S3 requests/storage, endpoint data, Lambda, logs,
and partial endpoint hours. With up to 100 short calls and prompt teardown,
**$0.06 is a planning estimate** before tax and credits.
[EC2](https://aws.amazon.com/ec2/instance-types/t3/),
[EBS](https://aws.amazon.com/ebs/pricing/),
[PrivateLink](https://aws.amazon.com/privatelink/pricing/),
[Secrets Manager](https://aws.amazon.com/secrets-manager/pricing/),
[Lambda](https://aws.amazon.com/lambda/pricing/) and
[S3 gateway endpoints](https://docs.aws.amazon.com/vpc/latest/privatelink/vpc-endpoints-s3.html)

Review a saved hourly plan using the same stage inputs and
`hourly = { ami_id = ..., executor_zip = ... }`. Confirm its private subnets,
AMI, SG references, exact artifact version, CA hash, role permissions,
resource tags and cleanup coverage. Download that S3 object version as the
operator and compare its SHA-256 to the qualified archive. Propose a **$0.25 ceiling**, a
25-minute destroy deadline from the start of provisioning and a further
35 minutes for cleanup. The owner approves the actual cap, clock and
cleanup owner against the shared $5 monthly allowance. Verify the AWS
balance before approval. AWS budget notifications are delayed.

Poll status about once a minute. Check instance status and Lambda state as
soon as each exists. Check the EC2 console for the fixture-ready or failed
line. This job has an eight-minute startup deadline. Start proof by minute
20 or begin cleanup. Run the same command with `PILOT_PHASE=hourly`.
Require the signed count of 32 and direct denial on *both* functions for
*both* roles. Match each printed request ID in the router log and the
permitted query in the executor log. The observer's request must have no
executor record. Stop on a confirmed missing prerequisite, wrong identity,
wrong result, unexpected grant, or a missed deadline.

Interrupt an active Terraform apply with SIGINT and allow it to exit before
starting cleanup. Destroy from the pilot's own state. Capture IDs during
the run, including EBS volumes and service-owned ENIs. Reconcile an empty
state with native inventory for Lambda functions and URL, CloudWatch log
groups, caller/runtime roles and policies, instance profile, instance,
volumes, ENIs, endpoints, subnets, security groups, VPC, secret and
dedicated bucket and all object versions. Check by name, tags and captured
IDs for partial creates. A missing permission leaves that check outstanding.
Continue cleanup beyond the reserve when necessary. Lambda can take up to
20 minutes to release an unused network interface. Verify the existing
M4 inventory before ending the session and check delayed billing separately.
[Lambda ENI lifecycle](https://docs.aws.amazon.com/lambda/latest/dg/configuration-vpc.html)
and [shared application validation](../../../docs/application-validation.md)
