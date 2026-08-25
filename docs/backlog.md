# Backlog

Known defects and deferred work. Each entry says what is wrong, why it is not
fixed yet, and what "done" looks like — so the decision to defer is a record
rather than something rediscovered later.

GitHub Issues are the tracker; this file is the copy that survives without
network access and that agents read alongside [AGENTS.md](../AGENTS.md).

## Open

### relay delivery targets are not checked against internal addresses

**Issue:** [#16](https://github.com/lilabrooks/my-local-platform/issues/16) ·
**Found:** 2026-08-25 · **Address:** before anything lets tenants register their
own endpoints

`subscriptions.ValidateURL` checks that a delivery URL parses, uses http or
https, and has a host. Nothing else. A subscription pointing at
`http://169.254.169.254/latest/meta-data/`, `http://localhost:9200` or an
RFC1918 address is delivered to, and relay makes those requests from inside the
cluster network, on a schedule, with retries, signed.

Not exploitable today: subscriptions are operator-seeded configuration in
`local/bootstrap/relay-db.sh` and nothing writes the table at runtime, so every
target was chosen by whoever ran the seed. That is the only thing holding it,
which is why the trigger above is a change rather than a date.

The dynamic half is already closed — `delivery.NewDeliverer` sets `CheckRedirect`
to refuse, so a subscriber cannot bounce a signed payload elsewhere at request
time. This is the static half: the configured URL itself.

Done means the resolved IP is checked at dial time, not the hostname string. A
hostname check is bypassed by DNS resolving to a private address, and
re-resolving after checking is a TOCTOU gap; `net.Dialer.Control` sees the
address actually being dialled. A refused target should dead-letter immediately
rather than retry, since the address will not become public on attempt three.
The local stack legitimately delivers to `http://sink:8081`, so the check needs
an allowlist rather than an off switch. Full acceptance criteria are in the
issue.

## Resolved

### Kafka smoke check degrades as the topic grows

**Issue:** [#1](https://github.com/lilabrooks/my-local-platform/issues/1) ·
**Found:** 2026-08-24 · **Fixed:** 2026-08-24, in M0 of the `relay` roadmap

`services/smoke/internal/checks/messaging.go` consumed with a fresh group id
from `kafka.FirstOffset`, so the check replayed the whole topic before reaching
the marker it had just published. Consume time grew linearly with topic size:
with 60,001 messages on `mlp.events` it timed out at ~31s; on a clean topic it
passed in ~10s.

Fixed by taking the partition and offset from the produce response -- available
because the check writes with `RequireAll` -- and seeking straight to that
record. One fetch, whatever the topic holds. The consumer group is gone
entirely, since one known record on one known partition needs no coordination.

Two things worth keeping from the fix:

- **The original diagnosis was incomplete.** Replay was not the main cost on a
  clean topic. Two kafka-go defaults were: a 1s writer `BatchTimeout` waiting
  for a batch of 100 that a one-message check cannot fill, and a 10s reader
  `MaxWait` that `Close` blocks on. Phase timing put 10.03s of a 10.04s run in
  those two, with the actual read at 13.7ms. `BatchSize: 1` and
  `MaxWait: 250ms` removed both.
- **The check no longer sets its own deadline.** It had a 30s inner timeout on
  top of the runner's 45s `checkTimeout` -- two bounds that could disagree.

Measured after: 211ms on an empty topic, and 206/199/208ms against 100,007
messages. Commands and the table are in
[ADR 0004](adr/0004-real-kafka-not-emulated.md#verification).
