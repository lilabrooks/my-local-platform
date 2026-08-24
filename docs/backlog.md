# Backlog

Known defects and deferred work. Each entry says what is wrong, why it is not
fixed yet, and what "done" looks like — so the decision to defer is a record
rather than something rediscovered later.

GitHub Issues are the tracker; this file is the copy that survives without
network access and that agents read alongside [AGENTS.md](../AGENTS.md).

## Open

### Kafka smoke check degrades as the topic grows

**Issue:** [#1](https://github.com/lilabrooks/my-local-platform/issues/1) ·
**Found:** 2026-08-24 · **Address:** with the first app that produces to
`mlp.events`

`services/smoke/internal/checks/messaging.go:44-45` consumes with a fresh group
id from `kafka.FirstOffset`, so the check replays the whole topic before
reaching the marker it just published. Consume time grows linearly with topic
size against a 30s deadline.

Measured: with 60,001 messages on `mlp.events` the check times out at ~31s. On
a clean topic it passes in ~10s.

Not urgent because only the smoke check writes to that topic today, one message
per run. It becomes real the moment an application produces at volume.

Preferred fix is to position the reader at the end *before* publishing, which
makes the check independent of topic size while keeping the write-then-read-back
property. Full options and acceptance criteria are in the issue.

## Resolved

Nothing yet. Move entries here with the commit that closed them rather than
deleting, so the reasoning stays available.
