# ChatBridge — Synthetic Test Fixtures Catalog (`QA-01-01`)

> **Task ID:** `QA-01-01` · **Authoritative Plan:** [MangMie-Document/G1/evidence/test-plan.md](https://github.com/TraqVG/MangMie-Document/blob/main/G1/evidence/test-plan.md)  
> **Status:** Fixture directory & catalog initialized (**all G3 cases remain `PENDING_EXECUTION` until verified against executable code and recorded with immutable evidence**).

## 1. Purpose & Safety Guardrails

This directory holds deterministic, version-controlled **synthetic fixtures** for testing standalone ChatBridge (`chatbridge_db`, multi-tenant/multi-site isolation, LINE OA / TikTok / Native Web Chat gateways, transactional Outbox/Inbox, Auto-Claim / Auto-Release CAS leases, `UNKNOWN` outbound delivery reconciliation, historical backfill isolation, and private Ω Data / SeaweedFS media handling).

**Mandatory Safety Rules:**
1. **Zero Real Customer PII or Provider Credentials:** Never commit real customer messages, LINE/TikTok channel secrets, session cookies, or unredacted presigned Ω Data URLs in this directory.
2. **Standalone Execution:** ChatBridge fixtures must run without requiring MangMie UI or MangMie DB to be online, proving standalone 24/7 operation (`DD-03`, `DD-04`, `DD-10`).
3. **Immutable Checksums:** Each G3 evidence record must log the SHA-256 checksum (`fixture_sha256`) of the fixture file(s) used during the test run.

## 2. Planned Fixture IDs Mapped to `P1-08` & `P1-02` Cases

| Fixture ID | Target Case(s) | Domain / Suite | Required Synthetic Data Shape |
| --- | --- | --- | --- |
| `FX-SEC-T01-TWO-SHOPS-TWO-TENANTS` | `T01` | Multi-Tenant Isolation (`QA-01-03`, `QA-01-11`) | `tenant_alpha` and `tenant_beta` integration clients, channel accounts, conversations, and cross-tenant negative ID-guessing vectors. |
| `FX-SEC-T02-WEBCHAT-SITE-ORIGIN` | `T02` | Native Web Chat Guest Recognition (`QA-01-03`, `QA-01-11`) | `web_chat_sites` (`site_key`, `allowed_origins`), 60-day opaque recognition token hashes (`web_chat_browser_links`), and short-lived `web_chat_sessions`. |
| `FX-CB-T03-CONCURRENT-AUTO-CLAIM` | `T03` | Claim-on-Enter CAS (`QA-01-04`, `QA-01-12`) | `UNASSIGNED` `OPEN` conversation + two simultaneous Staff claim requests testing `claim_version` CAS and lease issuance. |
| `FX-CB-T04-STALE-RELEASE-GUARD` | `T04` | Conditional Auto-Release (`QA-01-04`, `QA-01-12`) | Stale `lease_id` / `claim_version` release payload arriving after a newer Staff claim. |
| `FX-CB-T05-OWNER-REPLY-NO-CLAIM` | `T05` | Owner Independent Reply (`QA-01-04`, `QA-01-17`) | Owner outbound command on `UNASSIGNED` and Staff-claimed conversations verifying zero mutation to `conversation_assignments`. |
| `FX-CB-T07-DUPLICATE-REORDERED-WEBHOOK` | `T07` | Ingress Webhook Dedup & Ordering (`QA-01-04`, `QA-01-12`) | Duplicate and out-of-order signed LINE/TikTok webhook payloads testing `webhook_receipts`, monotonic `messages.seq`, and `inbox_receipts`. |
| `FX-CB-T08-OUTBOUND-TIMEOUT-UNKNOWN` | `T08` | Outbound `UNKNOWN` Reconciliation (`QA-01-04`, `QA-01-12`) | Provider timeout mid-send producing `outbound_intents.state = UNKNOWN`, verifying no blind retry on worker restart. |
| `FX-CB-T09-HISTORICAL-IMPORT-NO-SIDE-EFFECTS` | `T09` | Historical Backfill Safety (`QA-01-04`, `QA-01-12`) | Batch history payload (`historical_import=true`, `backward` cursor) asserting 0 outbound sends, 0 reopens of `RESOLVED` rooms, 0 Claims, and 0 downstream Order/AI triggers. |
| `FX-CB-T10-PARTIAL-UNSUPPORTED-COVERAGE` | `T10` | Provider Capability & Gap Honesty (`QA-01-04`, `QA-01-12`) | History range gap and unarchivable sticker reference producing `PARTIAL` / `UNSUPPORTED` checkpoint state. |
| `FX-ST-S01-T17-CROSS-TENANT-MEDIA` | `T17`, `S01` | Cross-Tenant Media Denial (`QA-01-06`, `QA-01-14`) | Tenant B requesting Tenant A `media_id` / `storage_object_key` and log-redaction check for presigned URLs. |
| `FX-ST-S02-PRIVATE-ADMIN-PORTS` | `S02` | Private SeaweedFS Admin Ports (`ST-01-03`, `QA-01-14`) | Direct client probe vectors against SeaweedFS master/filer/S3 admin endpoints. |
| `FX-ST-S03-INVALID-UPLOAD-TICKET` | `S03` | Upload Ticket & MIME/Quota Guard (`ST-01-04`, `QA-01-06`) | Expired/replayed upload ticket and oversized/disallowed MIME payloads. |
| `FX-ST-S04-S08-T18-ORPHAN-RACE` | `T18`, `S04`, `S08` | Staging Crash & Orphan Cleanup Race (`ST-01-02`, `ST-02-05`, `QA-01-14`) | Incomplete upload bytes (must not mark `READY`) + orphan reaper racing a live message attachment reference. |
| `FX-ST-S05-PRESIGN-CORS-MULTIPART` | `S05` | Pinned SeaweedFS Presign vs Relay (`ST-01-01`, `ST-01-04`) | Presigned PUT/GET expiry, CORS headers, and multipart upload vectors against pinned SeaweedFS image digest. |
| `FX-ST-S06-CORRUPT-QUARANTINE` | `S06` | Scan / Decode Failure Quarantine (`ST-02-01`, `QA-01-14`) | Corrupted image / EICAR-style synthetic test payload verifying `QUARANTINED` / `FAILED` state with no unauthorized preview. |
| `FX-ST-S07-DUPLICATE-PROVIDER-MEDIA` | `S07` | Provider Media Callback Dedup (`ST-02-01`, `QA-01-06`) | Duplicate provider media fetch/webhook events verifying single `media_assets` ingest. |
| `FX-ST-S09-RESTART-BOUNDED-RETRY` | `S09` | Worker Restart Recovery (`ST-02-01`, `IF-01-03`) | Pending media fetch/upload job surviving process restart with bounded retry. |
| `FX-IF-S10-CLEANROOM-RESTORE-BUNDLE` | `S10` | Clean-Room DB + Ω Data Restore (`IF-04-06`, `IF-04-07`) | Two-tenant synthetic DB + SeaweedFS catalog/volume snapshot for isolated restore verification. |
| `FX-ST-S11-CREDENTIAL-REVOCATION` | `S11` | Scoped Storage Credential Rotation (`ST-01-03`, `IF-04-08`) | Rotated/revoked `omega-data-api` S3 key pair verifying immediate access denial and clean logs. |
| `FX-ST-S12-LEGACY-R2-MANIFEST` | `S12` | Conditional Legacy R2 Dry-Run (`ST-02-04`, `QA-02-01`) | Synthetic legacy `r2_key` → `storage_object_key` checksum/ACL mapping manifest for dry-run migration rehearsal. |
| `FX-FD-T22-EXPAND-MIGRATE-CONTRACT` | `T22` | Schema & Event Compatibility (`QA-01-02`, `QA-02-01`) | Versioned OpenAPI/Outbox JSON Schema vectors and expand-migrate-contract SQL fixtures for `chatbridge_db`. |
