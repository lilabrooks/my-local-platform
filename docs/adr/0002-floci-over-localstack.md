# 2. floci instead of LocalStack for the local AWS surface

Date: 2026-08-23
Status: Accepted

## Context

The local stack needs S3, SNS, SQS and SES without touching a real account.
LocalStack is the reflexive choice, but as of 2026 it no longer fits:

- LocalStack retired its open-source Community Edition and consolidated on a
  single image that requires registration and an auth token. Its own licensing
  page (dated 2026-03-23) lists the free tier as "Hobby" and states that a
  license "generates an authentication token that enables access to the
  emulator". Telemetry sharing is listed as `enforced` on that tier.
- RDS and EKS have never been available below the paid tiers.

An auth token is a poor dependency for a repository intended to be public and
clonable: anyone browsing the GitHub profile would have to register before
`docker compose up` worked.

Note on sourcing: several blog posts claim LocalStack 3.0 moved S3 and SQS to
Pro. That is false, and those posts appear to be machine-generated. The claims
above come from LocalStack's own documentation.

## Decision

Use [floci](https://github.com/floci-io/floci), pinned to `1.7.0`.

It is MIT-licensed, requires no account or token, and emulates ~80 services on
`:4566`, including all four needed here plus RDS and EKS.

## Consequences

The repository clones and runs with no signup. Against that: floci is young --
first commit 2026-02-18, roughly six months old at time of writing -- so it has
had far less time to accumulate edge-case fidelity than LocalStack. It is
actively developed (v1.7.0 released 2026-08-18, commits landing daily) and has
~21k stars, but "popular and active" is not the same as "battle-tested". Any
behaviour that matters should be confirmed against the real cheap tier.

Two operational gotchas found while wiring this up, both now handled in
`local/docker-compose.yml`:

- floci's entrypoint **drops privileges to uid 1001** even when the container
  is started as root, so a root-owned named volume makes persistent storage
  fail at boot. A `floci-init` container fixes ownership first.
- **The Docker socket is not mounted by default.** floci's own docs mount
  `/var/run/docker.sock`, which grants a container effective root on the host:
  anything inside it can start a privileged container that mounts the host
  filesystem. The services this repo uses daily do not need it, verified by
  running floci without the socket — S3, SNS, SQS, SES and Secrets Manager all
  work, healthy in 2 seconds.

  floci's **container-backed** services do need it, because they launch real
  sibling containers: RDS, EKS, Lambda, MSK, ElastiCache, OpenSearch. Also
  verified: `rds create-db-instance` without the socket fails inside
  docker-java's `UnixSocket` and creates nothing.

  So it is opt-in rather than removed —
  `local/docker-compose.floci-containers.yml`, or `make up-core-containers`.
  Apps in this repo are expected to use RDS and friends eventually; the point
  is that the grant is deliberate and bounded, not permanent and implicit.
- Storage defaults to **in-memory**, so recreating the container silently
  discards every seeded bucket, topic and queue. `FLOCI_STORAGE_MODE=persistent`
  is set explicitly.

## Verification

`make up-core && make seed` followed by `docker compose restart floci` leaves
the bucket, topic and queue intact. The `s3`, `sns->sqs` and `ses` checks in
`services/smoke` pass against it.

**The fidelity concern above has been tested, not just noted.** The same smoke
binary was run against the real AWS account with `MLP_USE_REAL_AWS=1`, using a
Terraform-created bucket, topic and queue. S3 and SNS-to-SQS passed unchanged --
no code differences, no conditionals, only different environment variables.

Latency is the visible difference, and it is worth knowing before drawing
conclusions from local timings:

| Check | floci | real AWS |
|---|---|---|
| s3 round trip | ~25 ms | ~900-1100 ms |
| sns to sqs fanout | ~15 ms | ~1250 ms |

So floci is faithful enough for these APIs at the level this code uses them.
It is roughly 50x faster, which is exactly why it is worth having, and exactly
why a local pass says nothing about real-world timing. SES is skipped against
real AWS deliberately: sending live email is not a smoke check's business.
