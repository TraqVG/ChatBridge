# ChatBridge — Agent Entry Point

This repository holds the **independent, reusable ChatBridge messaging core**, not MangMie's operator frontend or the central task ledger.

## Required before EACH task (ChatGPT / Codex / Claude / other agents)

1. Fetch the latest GitHub `main` for **TraqVG/MangMie-Document** and read its [AGENTS.md](https://github.com/TraqVG/MangMie-Document/blob/main/AGENTS.md). Do not rely on a stale local checkout.
2. Follow its links to `TASKS/INDEX.md`, `TASKS/catalog.json`, `SESSIONS/ACTIVE/`, `CURRENT-STATE.md`, `DECISIONS.md`, `WORK-LOG.md` and relevant DD/spec files.
3. Inspect this repository's own HEAD, source and actual implementation evidence. ChatBridge owns channel adapters, messages/history, queue/outbox/retry, connection/session state, claim/release and realtime; it must run independently of an open MangMie browser.
4. Identify your assigned task and unique session ID. Commit central Task/Session state transitions and meaningful checkpoints with source commit/PR, tests, blockers, files touched and next action.
5. QUEUED is an **advisory reservation**, not a lock; prefer eligible UNSTARTED tasks in recommendations, but explicit Owner override can reassign even a reserved task. Surface DOING/file overlaps and work on separate branches.
6. Do not claim a provider API capability, integration test, runtime readiness or deployment PASS without evidence. Do not touch customer data, secrets, production integrations or external messaging absent explicit permission.
7. A ChatBridge Admin Console is a proposed operational feature; do not assume its unreviewed scope or permissions are approved.

Coordination source of truth: **https://github.com/TraqVG/MangMie-Document**. If the central documents are inaccessible, disclose that and do not invent statuses.
