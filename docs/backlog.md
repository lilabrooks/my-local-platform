# Backlog

**This is not a mirror of the issue tracker.** GitHub tracks *work*; this file
records *why something was deliberately not done yet*, and what would change
that. Most open issues never belong here — they are scheduled, and a milestone
already says so.

An entry earns its place when the decision to defer is itself the thing worth
keeping: a defect somebody chose to live with, and the condition under which
that choice stops holding. Each one says what is wrong, why it is not fixed,
and what "done" looks like — so the reasoning survives without the conversation
that produced it, and without network access.

Anything scheduled into a milestone belongs on the tracker alone. Resolved
entries are removed rather than archived here; the commit that closes an issue
is the record, and an ADR carries the measurements where there are any.

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

### relay deliver readiness does not reflect partition assignment

**Issue:** [#21](https://github.com/lilabrooks/my-local-platform/issues/21) ·
**Found:** 2026-08-25 · **Deferred:** 2026-08-26, behind
[#32](https://github.com/lilabrooks/my-local-platform/issues/32) ·
**Address:** if the demo shows pods that are Ready and idle

`Consumer.Run` marks itself ready the instant the loop starts — before joining
the consumer group and before any partition assignment. A consumer holding zero
partitions reports `Ready`, and a Kubernetes readiness probe passes it.

This is the bug CI caught in #10, where relay-deliver joined a group before its
topic existed, was assigned nothing, and sat healthy and idle while every event
went undelivered. It would have reported Ready throughout.

Deferred deliberately, with the demo sequenced ahead of it. The reasoning: KEDA
scaling to twelve pods that all report Ready while lag does not drain would
make the demo show autoscaling that visibly does nothing — but that is a
failure the demo would *surface*, loudly, rather than hide. Fixing it first
would be fixing a predicted problem; running the demo tells us whether it is a
real one.

Done means `/readyz` reflects assignment rather than "the goroutine started",
`/healthz` stays independent because a consumer with no assignment is alive and
restarting it does not help, and the assignment count is in the readiness body
so the failure is diagnosable rather than merely red.

### relay interrupted delivery discards its error and can commit silently

**Issue:** [#24](https://github.com/lilabrooks/my-local-platform/issues/24) ·
**Found:** 2026-08-25 · **Address:** next time
`internal/delivery/consumer.go` is open

`Consumer.handle` sets an `interrupted` flag when `Deliver` returns a non-nil
error, then discards that error and reconstructs one with `context.Cause(ctx)`.

The two correlate today only because `Deliver` returns nothing but `ctx.Err()`
— an invariant maintained in a different function. If it ever returns any other
non-nil error, `context.Cause(ctx)` is nil, `handle` returns nil, and **the
record commits as though every subscriber succeeded**. That is silent data loss.

Deferred because it is currently unreachable, not because it is unimportant.
Commit-only-when-finished is the most important property this consumer has, and
it should not depend on an invariant held somewhere else. Cheap to fix; the
trigger is proximity rather than severity.

Done means capturing and propagating the actual error from `Deliver`, plus a
test where a deliverer returns a non-context error and the offset is not
committed.

### relay poison-record dead letters carry an empty tenant key

**Issue:** [#25](https://github.com/lilabrooks/my-local-platform/issues/25) ·
**Found:** 2026-08-25 · **Address:** when someone first reads the DLQ to
diagnose something

The undecodable-record path builds a dead letter with a zero `Record`, and
`deadLetter` keys the message by `dl.Record.TenantID` — which is `""`. Every
poison record keys identically and none is attributable to a tenant.

No practical impact today, because the DLQ has one partition. It becomes
confusing at exactly the moment someone needs it to be clear, which is why the
trigger is an event rather than a date.

Done means the source partition and offset carried as fields rather than only
inside the reason string, poison records keyed by something meaningful or a note
explaining why an empty key is right for them, and the raw bytes of the
undecodable record preserved so it can actually be diagnosed.
