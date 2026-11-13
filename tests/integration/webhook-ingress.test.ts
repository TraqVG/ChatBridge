import { after, before, describe, it } from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHmac } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import {
  buildPgEnv,
  PsqlReceiptStore,
  WebhookIngressService,
  type AccountBindingResolver,
  type ChannelAccountBinding,
} from "../../chat-core/src/ingress.js";

function requireDatabaseUrl(): string {
  const value = process.env.DATABASE_URL;
  if (!value || value.trim().length === 0) {
    throw new Error("DATABASE_URL must be set to execute PostgreSQL webhook ingress tests");
  }
  return value;
}

const DB_URL: string = requireDatabaseUrl();
const MIGRATIONS_DIR = path.resolve(process.cwd(), "database-adapters/migrations");

const TENANT_A = "11111117-1111-4111-8111-111111111111";
const TENANT_B = "22222227-2222-4222-8222-222222222222";
const ACC_A1 = "aaaaaaa7-0001-4111-8111-111111111111";
const ACC_A2 = "aaaaaaa7-0002-4111-8111-111111111111";
const ACC_B1 = "bbbbbbb7-0001-4222-8222-222222222222";

const APP_LOGIN = "cb_ci_ingress_probe";
const APP_PASS = "synthetic_ingress_pw_01";
const LINE_SECRET_A1 = "pg-line-secret-a1";
const LINE_SECRET_A2 = "pg-line-secret-a2";
const TIKTOK_SECRET_B1 = "pg-tiktok-secret-b1";

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

function signLine(body: string, secret: string): string {
  return createHmac("sha256", secret).update(body).digest("base64");
}

function signTiktok(body: string, secret: string): string {
  return createHmac("sha256", secret).update(body).digest("hex");
}

function cleanupTestData(pgEnv: NodeJS.ProcessEnv): void {
  runPsql(
    pgEnv,
    `
    DELETE FROM webhook_receipts WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}');
    DELETE FROM channel_accounts WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}');
    DELETE FROM tenants WHERE id IN ('${TENANT_A}', '${TENANT_B}');
    `
  );
}

describe("CB-02-01 PostgreSQL 16 durable webhook receipt & deduplication", () => {
  const adminEnv = buildPgEnv(DB_URL);
  const appUrl = new URL(DB_URL);
  appUrl.username = APP_LOGIN;
  appUrl.password = APP_PASS;
  const appUri = appUrl.toString();

  const resolver: AccountBindingResolver = {
    async resolveAccountBinding(tenantId, channelAccountId, provider) {
      const list: ChannelAccountBinding[] = [
        {
          tenantId: TENANT_A,
          channelAccountId: ACC_A1,
          provider: "line_oa",
          connectionStatus: "CONNECTED",
          signingSecret: LINE_SECRET_A1,
        },
        {
          tenantId: TENANT_A,
          channelAccountId: ACC_A2,
          provider: "line_oa",
          connectionStatus: "CONNECTED",
          signingSecret: LINE_SECRET_A2,
        },
        {
          tenantId: TENANT_B,
          channelAccountId: ACC_B1,
          provider: "tiktok",
          connectionStatus: "CONNECTED",
          signingSecret: TIKTOK_SECRET_B1,
        },
      ];
      return (
        list.find(
          (b) =>
            b.tenantId === tenantId &&
            b.channelAccountId === channelAccountId &&
            b.provider === provider
        ) ?? null
      );
    },
  };

  before(() => {
    const files = fs
      .readdirSync(MIGRATIONS_DIR)
      .filter((f) => f.endsWith(".sql"))
      .sort();
    for (const file of files) {
      runPsqlFile(adminEnv, path.join(MIGRATIONS_DIR, file));
    }

    runPsql(
      adminEnv,
      `
      DO $$
      BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${APP_LOGIN}') THEN
          CREATE ROLE ${APP_LOGIN} LOGIN PASSWORD '${APP_PASS}' INHERIT IN ROLE chatbridge_app;
        ELSE
          ALTER ROLE ${APP_LOGIN} WITH LOGIN PASSWORD '${APP_PASS}' INHERIT;
          GRANT chatbridge_app TO ${APP_LOGIN};
        END IF;
      END
      $$;
      `
    );

    cleanupTestData(adminEnv);
    runPsql(
      adminEnv,
      `
      INSERT INTO tenants (id, slug, display_name, status)
      VALUES
        ('${TENANT_A}', 'tenant-cb0201-a', 'CB-02-01 Tenant A', 'ACTIVE'),
        ('${TENANT_B}', 'tenant-cb0201-b', 'CB-02-01 Tenant B', 'ACTIVE');

      INSERT INTO channel_accounts (id, tenant_id, provider, external_ref, display_name, connection_status)
      VALUES
        ('${ACC_A1}', '${TENANT_A}', 'LINE', 'oa-a1', 'LINE A1', 'CONNECTED'),
        ('${ACC_A2}', '${TENANT_A}', 'LINE', 'oa-a2', 'LINE A2', 'CONNECTED'),
        ('${ACC_B1}', '${TENANT_B}', 'TIKTOK', 'tt-b1', 'TikTok B1', 'CONNECTED');
      `
    );
  });

  after(() => {
    runPsql(
      adminEnv,
      "DROP TRIGGER IF EXISTS trg_fail_webhook_commit_probe ON webhook_receipts; DROP FUNCTION IF EXISTS fn_fail_webhook_commit_probe();"
    );
    cleanupTestData(adminEnv);
  });

  it("persists one canonical receipt per scoped identity under duplicate/reordered/missing-ID/cross-account fixtures", async () => {
    const store = new PsqlReceiptStore({ connectionUri: appUri });
    const service = new WebhookIngressService({ store, resolver });

    const body1 = JSON.stringify({ delivery: 1 });
    const ack1 = await service.ingestProviderWebhook({
      provider: "line_oa",
      tenantId: TENANT_A,
      channelAccountId: ACC_A1,
      rawBody: body1,
      signatureHeader: signLine(body1, LINE_SECRET_A1),
      events: [
        { eventKind: "message.inbound", providerEventId: "pg-evt-2", providerMessageId: "pg-msg-2", payload: { text: "two" } },
        { eventKind: "message.inbound", providerEventId: "pg-evt-1", providerMessageId: "pg-msg-1", payload: { text: "one" } },
      ],
    });
    assert.equal(ack1.httpStatus, 202);

    // Reordered replay + missing providerEventId on pg-msg-1 must map to original PostgreSQL rows.
    const body2 = JSON.stringify({ delivery: 2 });
    const ack2 = await service.ingestProviderWebhook({
      provider: "line_oa",
      tenantId: TENANT_A,
      channelAccountId: ACC_A1,
      rawBody: body2,
      signatureHeader: signLine(body2, LINE_SECRET_A1),
      events: [
        { eventKind: "message.inbound", providerMessageId: "pg-msg-1", payload: { text: "one" } },
        { eventKind: "message.inbound", providerEventId: "pg-evt-2", providerMessageId: "pg-msg-2", payload: { text: "two" } },
      ],
    });
    assert.equal(ack2.httpStatus, 202);
    if (ack1.httpStatus === 202 && ack2.httpStatus === 202) {
      assert.equal(ack2.data.deduplicated, true);
      assert.equal(ack2.data.receipt_ids[0], ack1.data.receipt_ids[1]);
      assert.equal(ack2.data.receipt_ids[1], ack1.data.receipt_ids[0]);
    }

    // Missing both IDs on TikTok converges via CANONICAL_BODY_FALLBACK even with reordered keys.
    const noIdBody1 = JSON.stringify({ text: "missing-ids", seq: 1 });
    const tt1 = await service.ingestProviderWebhook({
      provider: "tiktok",
      tenantId: TENANT_B,
      channelAccountId: ACC_B1,
      rawBody: noIdBody1,
      signatureHeader: signTiktok(noIdBody1, TIKTOK_SECRET_B1),
      events: [{ eventKind: "message.inbound", payload: { b: 2, a: 1, isRedelivery: false } }],
    });
    const tt2 = await service.ingestProviderWebhook({
      provider: "tiktok",
      tenantId: TENANT_B,
      channelAccountId: ACC_B1,
      rawBody: noIdBody1,
      signatureHeader: signTiktok(noIdBody1, TIKTOK_SECRET_B1),
      events: [{ eventKind: "message.inbound", payload: { a: 1, isRedelivery: true, b: 2 } }],
    });
    assert.equal(tt1.httpStatus, 202);
    assert.equal(tt2.httpStatus, 202);
    if (tt1.httpStatus === 202 && tt2.httpStatus === 202) {
      assert.equal(tt1.data.deduplicated, false);
      assert.equal(tt2.data.deduplicated, true);
      assert.equal(tt2.data.receipt_id, tt1.data.receipt_id);
    }

    // Cross-account same ID on ACC_A2 creates an independent row.
    const acc2Ack = await service.ingestProviderWebhook({
      provider: "line_oa",
      tenantId: TENANT_A,
      channelAccountId: ACC_A2,
      rawBody: body1,
      signatureHeader: signLine(body1, LINE_SECRET_A2),
      events: [{ eventKind: "message.inbound", providerEventId: "pg-evt-1", providerMessageId: "pg-msg-1", payload: { text: "one" } }],
    });
    assert.equal(acc2Ack.httpStatus, 202);
    if (ack1.httpStatus === 202 && acc2Ack.httpStatus === 202) {
      assert.equal(acc2Ack.data.deduplicated, false);
      assert.notEqual(acc2Ack.data.receipt_id, ack1.data.receipt_ids[1]);
    }

    const totalCommitted = runPsql(
      adminEnv,
      `SELECT COUNT(*) FROM webhook_receipts WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}');`
    );
    assert.equal(totalCommitted, "4");
  });

  it("rolls back PostgreSQL transaction and denies durable ACK on injected DB failure before commit", async () => {
    const preFaultCount = runPsql(
      adminEnv,
      `SELECT COUNT(*) FROM webhook_receipts WHERE tenant_id = '${TENANT_A}' AND canonical_dedup_key = 'evt:message.inbound:pg-db-fail-1';`
    );
    assert.equal(preFaultCount, "0");

    // 1. Simulated pre-commit hook failure.
    const hookFailStore = new PsqlReceiptStore({
      connectionUri: appUri,
      beforeCommit() {
        throw new Error("simulated pre-commit DB connection loss");
      },
    });
    const hookFailService = new WebhookIngressService({ store: hookFailStore, resolver });
    const body = JSON.stringify({ event: "pg-db-fail-1" });
    const hookRes = await hookFailService.ingestProviderWebhook({
      provider: "line_oa",
      tenantId: TENANT_A,
      channelAccountId: ACC_A1,
      rawBody: body,
      signatureHeader: signLine(body, LINE_SECRET_A1),
      events: [{ eventKind: "message.inbound", providerEventId: "pg-db-fail-1", payload: { text: "fail-1" } }],
    });
    assert.equal(hookRes.httpStatus, 503);
    assert.equal(hookRes.persisted, false);

    // 2. Real PostgreSQL BEFORE INSERT/COMMIT trigger failure ensuring atomic rollback inside DB.
    runPsql(
      adminEnv,
      `
      CREATE OR REPLACE FUNCTION fn_fail_webhook_commit_probe() RETURNS TRIGGER AS $$
      BEGIN
        IF NEW.provider_event_id = 'pg-db-trigger-fail-2' THEN
          RAISE EXCEPTION 'simulated_postgres_write_failure';
        END IF;
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;

      CREATE TRIGGER trg_fail_webhook_commit_probe
        BEFORE INSERT ON webhook_receipts
        FOR EACH ROW
        EXECUTE FUNCTION fn_fail_webhook_commit_probe();
      `
    );

    const liveStore = new PsqlReceiptStore({ connectionUri: appUri });
    const liveService = new WebhookIngressService({ store: liveStore, resolver });
    const trigBody = JSON.stringify({ event: "pg-db-trigger-fail" });
    const trigRes = await liveService.ingestProviderWebhook({
      provider: "line_oa",
      tenantId: TENANT_A,
      channelAccountId: ACC_A1,
      rawBody: trigBody,
      signatureHeader: signLine(trigBody, LINE_SECRET_A1),
      events: [
        { eventKind: "message.inbound", providerEventId: "pg-db-trigger-ok-first", payload: { text: "rolled-back-with-batch" } },
        { eventKind: "message.inbound", providerEventId: "pg-db-trigger-fail-2", payload: { text: "triggers-abort" } },
      ],
    });

    assert.equal(trigRes.httpStatus, 503);
    assert.equal(trigRes.persisted, false);

    const rolledBackCount = runPsql(
      adminEnv,
      `SELECT COUNT(*) FROM webhook_receipts WHERE tenant_id = '${TENANT_A}' AND provider_event_id IN ('pg-db-trigger-ok-first', 'pg-db-trigger-fail-2', 'pg-db-fail-1');`
    );
    assert.equal(rolledBackCount, "0");
  });
});
