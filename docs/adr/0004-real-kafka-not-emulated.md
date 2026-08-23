# 4. Real Kafka and RabbitMQ rather than their AWS emulations

Date: 2026-08-23
Status: Accepted

## Context

floci emulates both Amazon MSK and Amazon MQ, so the local stack could have
used one container for the entire AWS surface including messaging. Under the
covers, floci backs MSK with Redpanda and Amazon MQ with `rabbitmq:3-management`.

## Decision

Run Apache Kafka (`apache/kafka:4.1.0`, KRaft mode, no ZooKeeper) and RabbitMQ
(`rabbitmq:4.1-management`) as first-class containers instead.

## Consequences

The stated goal is learning Kafka and RabbitMQ, not learning the MSK and Amazon
MQ *control planes*. Redpanda is Kafka-protocol-compatible but is a different
implementation with different internals, so broker configuration, partition
rebalancing, log segment behaviour and JMX metrics would all differ from
Apache Kafka in ways that matter when the point is to understand the thing.

Running them directly also brings better tooling -- Kafka UI on `:8080`, the
RabbitMQ management console on `:15672` -- and the broker's own CLI scripts via
`docker exec`.

The cost is more containers to run and reason about, which is why the compose
file is split into profiles. Memory turned out not to be the issue: the full
stack idles at ~1 GB, with Kafka the largest single consumer at ~330 MB. When the goal shifts to learning MSK's control
plane specifically (cluster provisioning, IAM auth, configuration revisions),
floci's MSK emulation or the real thing via Terraform is the right tool.

Topics are created explicitly by `local/bootstrap/kafka-topics.sh`, with
`KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"`, so a typo in a topic name surfaces
as an error rather than silently creating a new topic.

## Verification

The `kafka` and `rabbitmq` checks in `services/smoke` each produce a message
and consume it back, asserting the payload matches.
