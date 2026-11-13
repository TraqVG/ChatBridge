import { after, before, describe, it } from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { buildPgEnv } from "../../chat-core/src/receipt-store.js";
import {
  createPostgresNormalizationService,
  NormalizationError,
  type CanonicalInboundInput,
} from "../../message-normalization/src/envelope.js";

function requireDatabaseUrl(): string {
  const value = process.env.DATABASE_URL;
  if (!value || value.trim().length === 0) {
    throw new Error("DATABASE_URL must be set to execute PostgreSQL message normalization tests");
  }
  return value;
}

const DB_URL: string = requireDatabaseUrl();
const MIGRATIONS_DIR = path.resolve(process.cwd(), "database-adapters/migrations");

const TENANT_A = "11111119-1111-4111-8111-111111111111";
const TENANT_B = "22222229-2222-4222-8222-222222222222";
const ACC_A1 = "aaaaaaa9-0001-4111-8111-111111111111";
const ACC_A2 = "aaaaaaa9-0002-4111-8111-111111111111";
const ACC_A_TT = "aaaaaaa9-0003-4111-8111-111111111111";
const ACC_A_WEB = "aaaaaaa9-0004-4111-8111-111111111111";
const ACC_B1 = "bbbbbbb9-0001-4222-8222-222222222222";

const SITE_A_WEB = "ccccccc9-0001-4111-8111-111111111111";
const GUEST_A_WEB = "ddddddd9-0001-4111-8111-111111111111";
const RECEIPT_A1 = "eeeeeee9-0001-4111-8111-111111111111";

const APP_LOGIN = "cb_ci_norm_probe";
const APP_PASS = "synthetic_norm_pw_01";

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
    DELETE FROM outbox_events WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}');
    DELETE FROM webhook_receipts WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}');
    DELETE FROM conversation_assignments WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}');
    DELETE FROM message_source_refs WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}');
    DELETE FROM message_parts WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}');
    DELETE FROM media_assets WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}');
    DELETE FROM messages WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}');
    DELETE FROM conversations WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}');
    DELETE FROM web_chat_participants WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}');
    DELETE FROM web_chat_sites WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}');
    DELETE FROM channel_identities WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}');
    DELETE FROM channel_accounts WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}');
    DELETE FROM tenants WHERE id IN ('${TENANT_A}', '${TENANT_B}');
    `
  );
}

describe("CB-02-05 PostgreSQL canonical message normalization, multi-account replay, concurrency & rollback suite", () => {
  const adminEnv = buildPgEnv(DB_URL);
  const appUrl = buildAppRoleUrl(DB_URL, APP_LOGIN, APP_PASS);
  const { service } = createPostgresNormalizationService(appUrl);

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
      GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO chatbridge_app;
      GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO chatbridge_app;
      `
    );

    cleanupTestData(adminEnv);

    runPsql(
      adminEnv,
      `
      INSERT INTO tenants (id, slug, display_name, status) VALUES
        ('${TENANT_A}', 'norm-tenant-a', 'Norm Tenant A', 'ACTIVE'),
        ('${TENANT_B}', 'norm-tenant-b', 'Norm Tenant B', 'ACTIVE');

      INSERT INTO channel_accounts (id, tenant_id, provider, external_ref, display_name, connection_status) VALUES
        ('${ACC_A1}', '${TENANT_A}', 'LINE', 'line-norm-a1', 'Line Norm A1', 'CONNECTED'),
        ('${ACC_A2}', '${TENANT_A}', 'LINE', 'line-norm-a2', 'Line Norm A2', 'CONNECTED'),
        ('${ACC_A_TT}', '${TENANT_A}', 'TIKTOK', 'tt-norm-a', 'TikTok Norm A', 'CONNECTED'),
        ('${ACC_A_WEB}', '${TENANT_A}', 'WEB', 'web-norm-a', 'Web Norm A', 'CONNECTED'),
        ('${ACC_B1}', '${TENANT_B}', 'LINE', 'line-norm-b1', 'Line Norm B1', 'CONNECTED');

      INSERT INTO web_chat_sites (id, tenant_id, channel_account_id, site_key, allowed_origins) VALUES
        ('${SITE_A_WEB}', '${TENANT_A}', '${ACC_A_WEB}', 'site-norm-a', '["https://widget.example"]'::jsonb);

      INSERT INTO web_chat_participants (id, tenant_id, site_id, pseudonymous_handle) VALUES
        ('${GUEST_A_WEB}', '${TENANT_A}', '${SITE_A_WEB}', 'guest-norm-01');

      INSERT INTO webhook_receipts (
        id, tenant_id, channel_account_id, provider, event_kind, provider_message_id, canonical_dedup_key, dedup_strategy, body_hash, state, signature_verified
      ) VALUES (
        '${RECEIPT_A1}', '${TENANT_A}', '${ACC_A1}', 'line_oa', 'message.inbound', 'corpus-line-01', 'pmsg:message.inbound:corpus-line-01', 'PROVIDER_MESSAGE_ID', 'hash-corpus-01', 'VERIFIED', TRUE
      );
      `
    );
  });

  after(() => {
    cleanupTestData(adminEnv);
  });

  it("replays multi-account corpus idempotently and stores unsupported media as visible placeholders", async () => {
    const corpus: CanonicalInboundInput[] = [
      {
        tenantId: TENANT_A,
        channelAccountId: ACC_A1,
        provider: "line_oa",
        sourceKind: "WEBHOOK",
        providerMessageId: "corpus-line-01",
        webhookReceiptId: RECEIPT_A1,
        correlationId: "corr-pg-corpus-01",
        sender: { providerSenderRef: "U-shared-corpus", displayName: "Shared Customer" },
        parts: [
          { kind: "TEXT", text: "Multi-part item" },
          {
            kind: "IMAGE",
            contentType: "image/png",
            bytes: 2048,
            storageObjectKey: "tenants/a/media/corpus-img-1.png",
          },
          { kind: "STICKER", packageId: "1", stickerId: "2" },
          { kind: "LINK", url: "https://shop.example/product/1", title: "Product 1" },
          {
            kind: "VIDEO",
            unavailable: true,
            unavailableReason: "PROVIDER_MEDIA_EXPIRED",
          },
        ],
      },
      {
        tenantId: TENANT_A,
        channelAccountId: ACC_A2,
        provider: "line_oa",
        sourceKind: "WEBHOOK",
        providerMessageId: "corpus-line-01",
        correlationId: "corr-pg-corpus-02",
        sender: { providerSenderRef: "U-shared-corpus" },
        parts: [{ kind: "TEXT", text: "Second account same ID" }],
      },
      {
        tenantId: TENANT_A,
        channelAccountId: ACC_A_TT,
        provider: "tiktok",
        sourceKind: "WEBHOOK",
        providerMessageId: "corpus-tt-01",
        correlationId: "corr-pg-corpus-03",
        sender: { providerSenderRef: "U-shared-corpus" },
        parts: [{ kind: "RAW_PROVIDER_UNSUPPORTED", rawType: "tiktok_carousel" }],
      },
      {
        tenantId: TENANT_A,
        channelAccountId: ACC_A_WEB,
        provider: "web_chat",
        sourceKind: "WEBHOOK",
        clientMessageId: "web-corpus-01",
        correlationId: "corr-pg-corpus-04",
        sender: { webParticipantId: GUEST_A_WEB },
        parts: [{ kind: "TEXT", text: "Web guest inquiry" }],
      },
      {
        tenantId: TENANT_B,
        channelAccountId: ACC_B1,
        provider: "line_oa",
        sourceKind: "WEBHOOK",
        providerMessageId: "corpus-line-01",
        correlationId: "corr-pg-corpus-05",
        sender: { providerSenderRef: "U-shared-corpus" },
        parts: [{ kind: "TEXT", text: "Tenant B isolated message" }],
      },
    ];

    const initial = [];
    for (const item of corpus) {
      initial.push(await service.normalizeAndPersist(item));
    }

    assert.equal(initial[0]!.message.parts.length, 5);
    assert.equal(initial[0]!.message.parts[4]!.kind, "UNSUPPORTED");
    assert.equal(initial[0]!.message.parts[4]!.payload.availability, "UNAVAILABLE");
    assert.equal(initial[2]!.message.kind, "UNSUPPORTED");
    assert.equal(
      runPsql(adminEnv, `SELECT state FROM webhook_receipts WHERE id = '${RECEIPT_A1}';`),
      "PROCESSED"
    );

    const distinctConvIds = new Set(initial.map((r) => r.message.conversationId));
    assert.equal(distinctConvIds.size, 5);

    const beforeCounts = runPsql(
      adminEnv,
      `SELECT (SELECT COUNT(*) FROM messages WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}')) || ':' ||
              (SELECT COUNT(*) FROM message_parts WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}')) || ':' ||
              (SELECT COUNT(*) FROM media_assets WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}')) || ':' ||
              (SELECT COUNT(*) FROM outbox_events WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}'));`
    );
    assert.equal(beforeCounts, "5:9:1:5");

    // Replay the exact corpus -> 0 new rows, identical message & conversation IDs.
    for (let i = 0; i < corpus.length; i++) {
      const replay = await service.normalizeAndPersist(corpus[i]!);
      assert.equal(replay.deduplicated, true);
      assert.equal(replay.message.messageId, initial[i]!.message.messageId);
      assert.equal(replay.message.conversationId, initial[i]!.message.conversationId);
      assert.equal(replay.message.seq, 1);
    }

    const afterCounts = runPsql(
      adminEnv,
      `SELECT (SELECT COUNT(*) FROM messages WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}')) || ':' ||
              (SELECT COUNT(*) FROM message_parts WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}')) || ':' ||
              (SELECT COUNT(*) FROM media_assets WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}')) || ':' ||
              (SELECT COUNT(*) FROM outbox_events WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}'));`
    );
    assert.equal(afterCounts, "5:9:1:5");
  });

  it("serializes simultaneous inserts into a single conversation with contiguous seq numbers", async () => {
    const senderRef = `U-concurrent-${randomUUID().slice(0, 8)}`;

    const concurrentInputs: CanonicalInboundInput[] = [
      {
        tenantId: TENANT_A,
        channelAccountId: ACC_A1,
        provider: "line_oa",
        sourceKind: "WEBHOOK",
        providerMessageId: "sim-msg-1",
        correlationId: "corr-sim-0001",
        sender: { providerSenderRef: senderRef },
        parts: [{ kind: "TEXT", text: "Simultaneous 1" }],
      },
      {
        tenantId: TENANT_A,
        channelAccountId: ACC_A1,
        provider: "line_oa",
        sourceKind: "WEBHOOK",
        providerMessageId: "sim-msg-2",
        correlationId: "corr-sim-0002",
        sender: { providerSenderRef: senderRef },
        parts: [{ kind: "TEXT", text: "Simultaneous 2" }],
      },
      {
        tenantId: TENANT_A,
        channelAccountId: ACC_A1,
        provider: "line_oa",
        sourceKind: "WEBHOOK",
        providerMessageId: "sim-msg-3",
        correlationId: "corr-sim-0003",
        sender: { providerSenderRef: senderRef },
        parts: [{ kind: "TEXT", text: "Simultaneous 3" }],
      },
      {
        tenantId: TENANT_A,
        channelAccountId: ACC_A1,
        provider: "line_oa",
        sourceKind: "WEBHOOK",
        providerMessageId: "sim-msg-4",
        correlationId: "corr-sim-0004",
        sender: { providerSenderRef: senderRef },
        parts: [{ kind: "TEXT", text: "Simultaneous 4" }],
      },
      {
        tenantId: TENANT_A,
        channelAccountId: ACC_A1,
        provider: "line_oa",
        sourceKind: "WEBHOOK",
        providerMessageId: "sim-msg-1",
        correlationId: "corr-sim-dup1",
        sender: { providerSenderRef: senderRef },
        parts: [{ kind: "TEXT", text: "Simultaneous 1 duplicate" }],
      },
    ];

    const outcomes = await Promise.all(
      concurrentInputs.map((item) => service.normalizeAndPersist(item))
    );

    const convIds = new Set(outcomes.map((o) => o.message.conversationId));
    assert.equal(convIds.size, 1);

    const convId = outcomes[0]!.message.conversationId;
    const fresh = outcomes.filter((o) => !o.deduplicated);
    const dupes = outcomes.filter((o) => o.deduplicated);
    assert.equal(fresh.length, 4);
    assert.equal(dupes.length, 1);

    const seqs = fresh.map((o) => o.message.seq).sort((a, b) => a - b);
    assert.deepEqual(seqs, [1, 2, 3, 4]);

    assert.equal(
      runPsql(adminEnv, `SELECT last_seq FROM conversations WHERE id = '${convId}';`),
      "4"
    );
    assert.equal(
      runPsql(adminEnv, `SELECT COUNT(*) FROM messages WHERE conversation_id = '${convId}';`),
      "4"
    );
    assert.equal(
      runPsql(
        adminEnv,
        `SELECT COUNT(*) FROM outbox_events WHERE tenant_id = '${TENANT_A}' AND aggregate_key = 'conv:${convId}';`
      ),
      "4"
    );
  });

  it("rolls back completely on commit failure and enforces live vs historical RESOLVED reopen rules", async () => {
    const crashSender = `U-pg-crash-${randomUUID().slice(0, 8)}`;
    const crashInput: CanonicalInboundInput = {
      tenantId: TENANT_A,
      channelAccountId: ACC_A2,
      provider: "line_oa",
      sourceKind: "WEBHOOK",
      providerMessageId: "pg-crash-msg-01",
      correlationId: "corr-pg-norm-crash",
      sender: { providerSenderRef: crashSender },
      parts: [
        { kind: "TEXT", text: "Must roll back atomically" },
        {
          kind: "FILE",
          contentType: "application/pdf",
          bytes: 512,
          storageObjectKey: "tenants/a/files/crash.pdf",
        },
      ],
    };

    await assert.rejects(
      service.normalizeAndPersist(crashInput, { simulateCrashBeforeCommit: true }),
      (err: unknown) =>
        err instanceof NormalizationError &&
        err.code === "DEPENDENCY_UNAVAILABLE" &&
        err.status === 503
    );

    assert.equal(
      runPsql(
        adminEnv,
        `SELECT COUNT(*) FROM channel_identities WHERE tenant_id = '${TENANT_A}' AND provider_sender_ref = '${crashSender}';`
      ),
      "0"
    );
    assert.equal(
      runPsql(
        adminEnv,
        `SELECT COUNT(*) FROM message_source_refs WHERE tenant_id = '${TENANT_A}' AND source_fingerprint = 'pmsg:pg-crash-msg-01';`
      ),
      "0"
    );

    // Recovery after crash commits single conversation, message, parts, media_asset, and outbox_event.
    const committed = await service.normalizeAndPersist(crashInput);
    assert.equal(committed.deduplicated, false);
    assert.equal(committed.message.seq, 1);
    const convId = committed.message.conversationId;

    // Resolve conversation and attach an active claimant in PostgreSQL.
    runPsql(
      adminEnv,
      `
      UPDATE conversations SET status = 'RESOLVED' WHERE id = '${convId}';
      INSERT INTO conversation_assignments (tenant_id, conversation_id, claimant_principal_id, claim_version, claimed_at)
      VALUES ('${TENANT_A}', '${convId}', 'staff-assignee-01', 1, NOW());
      `
    );

    // HISTORY_SYNC must append seq=2 while preserving status='RESOLVED' and active claimant.
    const histRes = await service.normalizeAndPersist({
      tenantId: TENANT_A,
      channelAccountId: ACC_A2,
      provider: "line_oa",
      sourceKind: "HISTORY_SYNC",
      providerMessageId: "pg-hist-msg-02",
      conversationId: convId,
      correlationId: "corr-pg-hist-backfill",
      sender: { providerSenderRef: crashSender },
      parts: [{ kind: "TEXT", text: "Backfill stays resolved" }],
    });
    assert.equal(histRes.message.seq, 2);
    assert.equal(histRes.message.historicalImport, true);
    assert.equal(histRes.message.reopenedConversation, false);
    assert.equal(histRes.message.conversationStatus, "RESOLVED");
    assert.equal(
      runPsql(
        adminEnv,
        `SELECT COALESCE(claimant_principal_id, 'NULL') FROM conversation_assignments WHERE conversation_id = '${convId}';`
      ),
      "staff-assignee-01"
    );

    // Genuine live inbound must reopen RESOLVED -> OPEN and clear active claimant atomically.
    const liveReopen = await service.normalizeAndPersist({
      tenantId: TENANT_A,
      channelAccountId: ACC_A2,
      provider: "line_oa",
      sourceKind: "WEBHOOK",
      providerMessageId: "pg-live-msg-03",
      conversationId: convId,
      correlationId: "corr-pg-live-reopen",
      sender: { providerSenderRef: crashSender },
      parts: [{ kind: "TEXT", text: "Live message reopens room" }],
    });
    assert.equal(liveReopen.message.seq, 3);
    assert.equal(liveReopen.message.reopenedConversation, true);
    assert.equal(liveReopen.message.conversationStatus, "OPEN");
    assert.equal(
      runPsql(
        adminEnv,
        `SELECT COALESCE(claimant_principal_id, 'NULL') FROM conversation_assignments WHERE conversation_id = '${convId}';`
      ),
      "NULL"
    );
  });
});
