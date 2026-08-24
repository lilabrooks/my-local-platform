# CLAUDE.md

Project instructions live in **[AGENTS.md](AGENTS.md)** — read that file.

It is the shared source of truth for every agent working here, so that Codex,
Claude Code and any reviewer follow the same rules rather than three drifting
copies.

## Claude Code specifics

- `.mcp.json` declares this project's MCP servers (`codegraph`, `semble`,
  `token-savior`, `parallel-search`). `.claude/settings.json` sets
  `enableAllProjectMcpServers`, so they load without per-session approval.
- `.claude/launch.json` has attach-only browser targets for the stack's web UIs
  (Grafana, Kafka UI, RabbitMQ, Prometheus, Tempo, ArgoCD). Bring the stack up
  first; these attach to running services rather than starting anything.
- Personal overrides go in `.claude/settings.local.json`, which is gitignored.
  This repository intentionally ships no command-permission rules — that is
  each user's call, not a project default.
