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
file is split into profiles.

Memory is a genuine cost, and an earlier version of this ADR understated it.
It claimed the stack idles at ~1 GB with Kafka at ~330 MB, measured 46 seconds
after startup. After four hours Kafka alone had reached **876 MB**, because the
`apache/kafka` image defaults to `-Xmx1G -Xms1G` and `-Xms` pre-commits the
heap -- the broker trends toward 1 GB regardless of traffic.

Setting `KAFKA_HEAP_OPTS=-Xmx512m -Xms256m` brings it to **~460 MB** while
producing and consuming 60,000 messages, a 48% reduction with no functional
loss at this scale. Raise it if a workload actually needs more; `mem_limit: 1g`
is a backstop, not a target. When the goal shifts to learning MSK's control
plane specifically (cluster provisioning, IAM auth, configuration revisions),
floci's MSK emulation or the real thing via Terraform is the right tool.

Topics are created explicitly by `local/bootstrap/kafka-topics.sh`, with
`KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"`, so a typo in a topic name surfaces
as an error rather than silently creating a new topic.

## A defect this ADR used to carry, now fixed

The `kafka` check consumed from the earliest offset with a fresh group id, so it
replayed the entire topic before reaching its own marker: ~10s on a clean topic,
timing out at ~31s once `mlp.events` held 60,001 messages.

It now reads the partition and offset out of the produce response -- available
because the check writes with `RequireAll` -- and seeks straight to that record,
so it costs one fetch no matter how large the topic is.

Measuring the fix turned up something the original report missed. Replay was not
where most of the time went on a clean topic; two kafka-go defaults were. Its
writer waits a 1s `BatchTimeout` for a batch of 100 that a single-message check
can never fill, and its reader's `Close` blocks on an in-flight fetch sitting out
the 10s default `MaxWait`. Those two accounted for 10.03s of a 10.04s run, with
the actual read at 13.7ms. Setting `BatchSize: 1` and `MaxWait: 250ms` removed
both.

Closes [issue #1](https://github.com/lilabrooks/my-local-platform/issues/1).

## Verification

The `kafka` and `rabbitmq` checks in `services/smoke` each produce a message
and consume it back, asserting the payload matches.

The `kafka` check's independence from topic size was measured on 2026-08-24, by
flooding the topic and running it three times:

```bash
docker exec mlp-kafka /opt/kafka/bin/kafka-producer-perf-test.sh \
  --topic mlp.events --num-records 100000 --record-size 200 \
  --throughput -1 --producer-props bootstrap.servers=localhost:19092
```

| Topic contents | `kafka` check |
|---|---|
| empty | 211ms |
| 100,007 messages | 206ms, 199ms, 208ms |

`kafka-get-offsets.sh` confirmed the end offsets (33391 / 33384 / 33232 across
the three partitions). Flat, where the previous implementation timed out at
60,001.
