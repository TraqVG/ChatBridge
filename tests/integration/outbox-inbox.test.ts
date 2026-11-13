import { after, before, describe, it } from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { buildPgEnv } from "../../chat-core/src/receipt-store.js";
import {
  ConsumerInboxProcessor,
  OutboxCoordinator,
  OutboxError,
  type OutboxEventEnvelope,
  PsqlOutboxInboxStore,
} from "../../queue-and-retry/src/outbox.js";

function requireDatabaseUrl(): string {
  const value = process.env.DATABASE_URL;
  if (!value || value.trim().length === 0) {
    throw new Error("DATABASE_URL must be set to execute PostgreSQL outbox/inbox tests");
  }
  return value;
}

const DB_URL: string = requireDatabaseUrl();
const MIGRATIONS_DIR = path.resolve(process.cwd(), "database-adapters/migrations");

const TENANT_A = "11111118-1111-4111-8111-111111111111";
const TENANT_B = "22222228-2222-4222-8222-222222222222";
const ACC_A1 = "aaaaaaa8-0001-4111-8111-111111111111";
const ACC_B1 = "bbbbbbb8-0001-4222-8222-222222222222";
const IDENT_A1 = "ccccccc8-0001-4111-8111-111111111111";

const APP_LOGIN = "cb_ci_outbox_probe";
const APP_PASS = "synthetic_outbox_pw_01";

function runPsql(pgEnv: NodeJS.ProcessEnv, sql: string): string {
  return execFileSync("psql", ["-v", "ON_ERROR_STOP=1", "-X", "-q", "-t", "-A"], {
    env: pgEnv,
    input: `${sql}\n`,
    encoding: "utf8",
  }).trim();
}

function runPsqlFile(pgEnv: NodeJS.ProcessEnv, filePath: string): void {
  execFileSync("psql", ["-v", "ON_ERROR_STOP=1", "-X", "-q", "-f", filePath], {
    env: { ...pgEnv, PGOPTIONS: "-c client_min_messages=warning" },
    encoding: "utf8",
  });
}

function buildAppRoleUrl(adminUrl: string, username: string, password: string): string {
  const u = new URL(adminUrl);
  u.username = username;
  u.password = password;
  return u.toString();
}

function cleanupTestData(pgEnv: NodeJS.ProcessEnv): void {
  runPsql(
    pgEnv,
    `
    DELETE FROM audit_logs WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}');
    DELETE FROM consumer_aggregate_cursors WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}');
    DELETE FROM inbox_receipts WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}');
    DELETE FROM outbox_events WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}');
    DELETE FROM messages WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}');
    DELETE FROM conversations WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}');
    DELETE FROM channel_identities WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}');
    DELETE FROM channel_accounts WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}');
    DELETE FROM tenants WHERE id IN ('${TENANT_A}', '${TENANT_B}');
    `
  );
}

describe("CB-03-03 PostgreSQL transactional outbox, inbox dedup, and crash/replay suite", () => {
  const adminEnv = buildPgEnv(DB_URL);
  const appUrl = buildAppRoleUrl(DB_URL, APP_LOGIN, APP_PASS);
  const store = new PsqlOutboxInboxStore({ connectionUri: appUrl });

  before(() => {
    const migrationFiles = fs
      .readdirSync(MIGRATIONS_DIR)
      .filter((f) => f.endsWith(".sql"))
      .sort();

    for (const file of migrationFiles) {
      runPsqlFile(adminEnv, path.join(MIGRATIONS_DIR, file));
    }

    runPsql(
      adminEnv,
      `
      DO $$
      BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${APP_LOGIN}') THEN
          CREATE ROLE ${APP_LOGIN} LOGIN PASSWORD '${APP_PASS}' NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT;
        ELSE
          ALTER ROLE ${APP_LOGIN} WITH PASSWORD '${APP_PASS}';
        END IF;
        GRANT chatbridge_app TO ${APP_LOGIN};
      END
      $$;
      GRANT USAGE, SELECT ON SEQUENCE outbox_stream_seq TO chatbridge_app;
      GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO chatbridge_app;
      `
    );

    cleanupTestData(adminEnv);

    runPsql(
      adminEnv,
      `
      INSERT INTO tenants (id, slug, name, status) VALUES
        ('${TENANT_A}', 'outbox-tenant-a', 'Outbox Tenant A', 'ACTIVE'),
        ('${TENANT_B}', 'outbox-tenant-b', 'Outbox Tenant B', 'ACTIVE');

      INSERT INTO channel_accounts (id, tenant_id, provider, external_account_id, display_name, status) VALUES
        ('${ACC_A1}', '${TENANT_A}', 'LINE_OA', 'line-outbox-a1', 'Line Outbox A1', 'ACTIVE'),
        ('${ACC_B1}', '${TENANT_B}', 'TIKTOK', 'tt-outbox-b1', 'TikTok Outbox B1', 'ACTIVE');

      INSERT INTO channel_identities (id, tenant_id, channel_account_id, provider_sender_ref, scope_key, display_name) VALUES
        ('${IDENT_A1}', '${TENANT_A}', '${ACC_A1}', 'sender-a1', '${ACC_A1}:sender-a1', 'Sender A1');
      `
    );
  });

  after(() => {
    cleanupTestData(adminEnv);
  });

  it("executes atomic owning transaction + outbox write and rolls back completely on pre-commit crash", async () => {
    const convId = randomUUID();
    const msgId = randomUUID();
    const eventId = randomUUID();

    await assert.rejects(
      store.commitWithOutbox(
        {
          eventId,
          tenantId: TENANT_A,
          channelAccountId: ACC_A1,
          type: "message.created",
          aggregateType: "conversation",
          aggregateKey: `conv:${convId}`,
          aggregateVersion: 1,
          correlationId: "corr-pg-rollback-01",
          payload: { message_id: msgId, message_seq: 1 },
        },
        {
          kind: "CONVERSATION_MESSAGE",
          conversationId: convId,
          channelAccountId: ACC_A1,
          identityId: IDENT_A1,
          messageId: msgId,
          messageSeq: 1,
          direction: "IN",
          senderKind: "CUSTOMER",
          body: "rolled back body",
        },
        { simulateCrashBeforeCommit: true }
      ),
      (err: unknown) =>
        err instanceof OutboxError &&
        err.code === "DEPENDENCY_UNAVAILABLE" &&
        err.status === 503
    );

    assert.equal(
      runPsql(adminEnv, `SELECT COUNT(*) FROM messages WHERE id = '${msgId}';`),
      "0"
    );
    assert.equal(
      runPsql(adminEnv, `SELECT COUNT(*) FROM conversations WHERE id = '${convId}';`),
      "0"
    );
    assert.equal(
      runPsql(adminEnv, `SELECT COUNT(*) FROM outbox_events WHERE id = '${eventId}';`),
      "0"
    );

    const committed = await store.commitWithOutbox(
      {
        eventId,
        tenantId: TENANT_A,
        channelAccountId: ACC_A1,
        type: "message.created",
        aggregateType: "conversation",
        aggregateKey: `conv:${convId}`,
        aggregateVersion: 1,
        correlationId: "corr-pg-commit-01",
        payload: {
          message_id: msgId,
          message_seq: 1,
          storage_object_key: "tenants/a/media/item-01",
        },
      },
      {
        kind: "CONVERSATION_MESSAGE",
        conversationId: convId,
        channelAccountId: ACC_A1,
        identityId: IDENT_A1,
        messageId: msgId,
        messageSeq: 1,
        direction: "IN",
        senderKind: "CUSTOMER",
        body: "committed atomically",
      }
    );

    assert.equal(committed.deduplicated, false);
    assert.equal(committed.record.state, "PENDING");
    assert.ok(committed.record.envelope.stream_cursor.outbox_seq >= 1);
    assert.equal(
      runPsql(adminEnv, `SELECT COUNT(*) FROM messages WHERE id = '${msgId}';`),
      "1"
    );
    assert.equal(
      runPsql(adminEnv, `SELECT version FROM conversations WHERE id = '${convId}';`),
      "1"
    );

    // Repeating same idempotent natural key returns existing row without bumping version.
    const dup = await store.commitWithOutbox(
      {
        eventId: randomUUID(),
        tenantId: TENANT_A,
        channelAccountId: ACC_A1,
        type: "message.created",
        aggregateType: "conversation",
        aggregateKey: `conv:${convId}`,
        aggregateVersion: 1,
        correlationId: "corr-pg-commit-01",
        payload: { message_id: msgId, message_seq: 1 },
      },
      {
        kind: "CONVERSATION_MESSAGE",
        conversationId: convId,
        channelAccountId: ACC_A1,
        identityId: IDENT_A1,
        messageId: randomUUID(),
        messageSeq: 1,
        direction: "IN",
        senderKind: "CUSTOMER",
        body: "should not overwrite",
      }
    );
    assert.equal(dup.deduplicated, true);
    assert.equal(dup.record.envelope.event_id, eventId);
    assert.equal(
      runPsql(adminEnv, `SELECT version FROM conversations WHERE id = '${convId}';`),
      "1"
    );
  });

  it("converges producer crash-after-publish and consumer crash-before-ack/replay to one SQL effect", async () => {
    let clockMs = Date.parse("2026-11-13T14:00:00.000Z");
    const delivered: OutboxEventEnvelope[] = [];
    const eventId = randomUUID();
    const convId = randomUUID();
    const msgId = randomUUID();
    const effectAuditId = randomUUID();

    const producer1 = new OutboxCoordinator(
      store,
      {
        async publish(env) {
          delivered.push(env);
        },
      },
      { workerId: "pg-worker-1", leaseMs: 15_000, now: () => new Date(clockMs) }
    );

    await producer1.commitAndStage(
      {
        eventId,
        tenantId: TENANT_A,
        channelAccountId: ACC_A1,
        type: "message.created",
        aggregateType: "conversation",
        aggregateKey: `conv:${convId}`,
        aggregateVersion: 1,
        occurredAt: new Date(clockMs).toISOString(),
        correlationId: "corr-pg-crash-converge-01",
        payload: { message_id: msgId, message_seq: 1 },
      },
      {
        kind: "CONVERSATION_MESSAGE",
        conversationId: convId,
        channelAccountId: ACC_A1,
        identityId: IDENT_A1,
        messageId: msgId,
        messageSeq: 1,
        direction: "IN",
        senderKind: "CUSTOMER",
        body: "hello live",
      }
    );

    // Producer publishes attempt 1 then crashes before markSucceeded.
    const crashRun = await producer1.publishDueBatch({
      tenantId: TENANT_A,
      stopAfterPublishBeforeMark: true,
    });
    assert.equal(crashRun.stoppedBeforeAckCount, 1);
    assert.equal(delivered.length, 1);

    const consumer = new ConsumerInboxProcessor(store, "pg-effect-consumer");
    let jsSideEffects = 0;

    // 1. Consumer crash before DB commit -> PostgreSQL rolls back inbox_receipts, cursor, AND audit_logs!
    await assert.rejects(
      consumer.process(
        delivered[0]!,
        () => {
          jsSideEffects++;
        },
        {
          nowIso: new Date(clockMs).toISOString(),
          sqlAuditEffect: {
            auditId: effectAuditId,
            actor: "consumer:pg-effect-consumer",
            action: "SIDE_EFFECT_EXECUTED",
            target: `conv:${convId}`,
          },
          simulateCrashBeforeCommit: true,
        }
      ),
      (err: unknown) => err instanceof OutboxError && err.code === "DEPENDENCY_UNAVAILABLE"
    );
    assert.equal(jsSideEffects, 0);
    assert.equal(
      runPsql(adminEnv, `SELECT COUNT(*) FROM audit_logs WHERE id = '${effectAuditId}';`),
      "0"
    );
    assert.equal(
      runPsql(
        adminEnv,
        `SELECT COUNT(*) FROM inbox_receipts WHERE external_event_id = '${eventId}';`
      ),
      "0"
    );

    // 2. Retry succeeds in DB commit, then crashes before acking broker.
    await assert.rejects(
      consumer.process(
        delivered[0]!,
        () => {
          jsSideEffects++;
        },
        {
          nowIso: new Date(clockMs).toISOString(),
          sqlAuditEffect: {
            auditId: effectAuditId,
            actor: "consumer:pg-effect-consumer",
            action: "SIDE_EFFECT_EXECUTED",
            target: `conv:${convId}`,
          },
          crashAfterCommitBeforeAck: true,
        }
      ),
      /Simulated consumer crash after durable commit/
    );
    assert.equal(jsSideEffects, 1);
    assert.equal(
      runPsql(adminEnv, `SELECT COUNT(*) FROM audit_logs WHERE id = '${effectAuditId}';`),
      "1"
    );

    // Active lease prevents another worker from double-claiming immediately.
    const producer2 = new OutboxCoordinator(
      store,
      {
        async publish(env) {
          delivered.push(env);
        },
      },
      { workerId: "pg-worker-2", leaseMs: 15_000, now: () => new Date(clockMs) }
    );
    clockMs += 5_000;
    const lockedOut = await producer2.publishDueBatch({ tenantId: TENANT_A });
    assert.equal(lockedOut.claimedCount, 0);

    // After lease expires, Worker 2 reclaims and replays attempt 2, then marks SUCCEEDED.
    clockMs += 15_000;
    const recovered = await producer2.publishDueBatch({ tenantId: TENANT_A });
    assert.equal(recovered.claimedCount, 1);
    assert.equal(recovered.succeededCount, 1);
    assert.equal(recovered.records[0]!.state, "SUCCEEDED");
    assert.equal(recovered.records[0]!.attempts, 2);
    assert.equal(delivered.length, 2);

    // Replayed delivery converges to DUPLICATE_NOOP and keeps exactly 1 audit effect row.
    const replayConsume = await consumer.process(
      delivered[1]!,
      () => {
        jsSideEffects++;
      },
      {
        nowIso: new Date(clockMs).toISOString(),
        sqlAuditEffect: {
          auditId: randomUUID(),
          actor: "consumer:pg-effect-consumer",
          action: "SIDE_EFFECT_EXECUTED",
          target: `conv:${convId}`,
        },
      }
    );
    assert.equal(replayConsume.status, "DUPLICATE_NOOP");
    assert.equal(replayConsume.effectExecuted, false);
    assert.equal(jsSideEffects, 1);
    assert.equal(
      runPsql(
        adminEnv,
        `SELECT COUNT(*) FROM audit_logs WHERE tenant_id = '${TENANT_A}' AND action = 'SIDE_EFFECT_EXECUTED' AND target = 'conv:${convId}';`
      ),
      "1"
    );

    // Newer aggregate_seq = 5 applies; subsequent out-of-order aggregate_seq = 3 is IGNORED_STALE.
    const seq5Id = randomUUID();
    const seq5Res = await store.commitWithOutbox({
      eventId: seq5Id,
      tenantId: TENANT_A,
      type: "conversation.assignment.changed",
      aggregateType: "conversation",
      aggregateKey: `conv:${convId}`,
      aggregateVersion: 5,
      occurredAt: new Date(clockMs).toISOString(),
      correlationId: "corr-pg-seq-5-01",
      payload: { claim_state: "CLAIMED" },
    });
    const apply5 = await consumer.process(seq5Res.record.envelope, () => {
      jsSideEffects++;
    });
    assert.equal(apply5.status, "APPLIED");
    assert.equal(jsSideEffects, 2);

    const stale3Res = await store.commitWithOutbox({
      eventId: randomUUID(),
      tenantId: TENANT_A,
      type: "conversation.assignment.changed",
      aggregateType: "conversation",
      aggregateKey: `conv:${convId}`,
      aggregateVersion: 3,
      occurredAt: new Date(clockMs).toISOString(),
      correlationId: "corr-pg-seq-3-stale",
      payload: { claim_state: "RELEASED" },
    });
    const ignore3 = await consumer.process(stale3Res.record.envelope, () => {
      jsSideEffects++;
    });
    assert.equal(ignore3.status, "IGNORED_STALE");
    assert.equal(ignore3.effectExecuted, false);
    assert.equal(jsSideEffects, 2);
  });

  it("enforces DB check constraint and preserves historical suppression flags through replay", async () => {
    // Database-level invariant refuses historical_import=TRUE without suppress_side_effects=TRUE.
    assert.throws(() => {
      runPsql(
        adminEnv,
        `
        INSERT INTO outbox_events (
          id, tenant_id, aggregate_key, aggregate_version, type, payload_minimal,
          historical_import, suppress_side_effects, source_kind, correlation_id
        ) VALUES (
          '${randomUUID()}', '${TENANT_B}', 'conv:invalid-hist', 1, 'message.created', '{}'::jsonb,
          TRUE, FALSE, 'INITIAL_BACKFILL', 'corr-db-check-hist'
        );
        `
      );
    }, /chk_outbox_historical_suppress/);

    const delivered: OutboxEventEnvelope[] = [];
    const coordinator = new OutboxCoordinator(
      store,
      {
        async publish(env) {
          delivered.push(env);
        },
      },
      { workerId: "pg-worker-hist" }
    );

    const histEventId = randomUUID();
    const auditId = randomUUID();
    await coordinator.commitAndStage({
      eventId: histEventId,
      tenantId: TENANT_B,
      channelAccountId: ACC_B1,
      type: "message.created",
      aggregateType: "conversation",
      aggregateKey: `conv:${TENANT_B}:backfill-1`,
      aggregateVersion: 1,
      historicalImport: true,
      suppressSideEffects: true,
      sourceKind: "INITIAL_BACKFILL",
      correlationId: "corr-pg-backfill-01",
      payload: { message_seq: 1, sync_run_id: "sync-b-01" },
    });

    await coordinator.publishDueBatch({ tenantId: TENANT_B });
    await coordinator.replayEvent(TENANT_B, histEventId, randomUUID());
    await coordinator.publishDueBatch({ tenantId: TENANT_B });

    assert.equal(delivered.length, 2);
    assert.equal(delivered[0]!.history.historical_import, true);
    assert.equal(delivered[0]!.history.suppress_side_effects, true);
    assert.equal(delivered[1]!.history.historical_import, true);
    assert.equal(delivered[1]!.history.suppress_side_effects, true);
    assert.equal(delivered[1]!.replay.is_replay, true);

    const consumer = new ConsumerInboxProcessor(store, "pg-ai-responder");
    let suppressedEffects = 0;

    const first = await consumer.process(
      delivered[0]!,
      () => {
        suppressedEffects++;
      },
      {
        sqlAuditEffect: {
          auditId,
          actor: "consumer:pg-ai-responder",
          action: "SHOULD_NOT_FIRE",
          target: `conv:${TENANT_B}:backfill-1`,
        },
      }
    );
    assert.equal(first.status, "SUPPRESSED_HISTORY");
    assert.equal(first.effectExecuted, false);
    assert.equal(first.receipt.historicalImport, true);
    assert.equal(first.receipt.suppressSideEffects, true);
    assert.equal(first.receipt.sourceKind, "INITIAL_BACKFILL");

    const replayed = await consumer.process(delivered[1]!, () => {
      suppressedEffects++;
    });
    assert.equal(replayed.status, "DUPLICATE_NOOP");
    assert.equal(replayed.effectExecuted, false);
    assert.equal(suppressedEffects, 0);
    assert.equal(
      runPsql(adminEnv, `SELECT COUNT(*) FROM audit_logs WHERE id = '${auditId}';`),
      "0"
    );

    // Tenant stream read isolation: Tenant B's cursor only lists Tenant B records.
    const streamB = await coordinator.readStreamSince(TENANT_B, 0, 50);
    assert.equal(streamB.length, 1);
    assert.equal(streamB[0]!.envelope.scope.tenant_id, TENANT_B);
  });
});
