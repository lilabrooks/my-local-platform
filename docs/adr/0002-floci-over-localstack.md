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
- Storage defaults to **in-memory**, so recreating the container silently
  discards every seeded bucket, topic and queue. `FLOCI_STORAGE_MODE=persistent`
  is set explicitly.

## Verification

`make up-core && make seed` followed by `docker compose restart floci` leaves
the bucket, topic and queue intact. The `s3`, `sns->sqs` and `ses` checks in
`services/smoke` pass against it.
