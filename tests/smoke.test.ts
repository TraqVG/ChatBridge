import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  ChatBridgeConfigError,
  loadChatBridgeConfig,
  redactChatBridgeConfig,
  SYNTHETIC_CHATBRIDGE_DEV_ENV,
} from "../src/config.js";
import { startChatBridgeServer } from "../src/server.js";
import { startChatBridgeWorker } from "../src/worker.js";
import { runChatBridgeMigrations } from "../src/migrate.js";

describe("FD-02-02 Standalone ChatBridge server and worker bootstrap", () => {
  it("loads synthetic configuration without requiring any external storefront/Auth Gate settings", () => {
    const config = loadChatBridgeConfig(SYNTHETIC_CHATBRIDGE_DEV_ENV);
    assert.equal(config.serviceName, "chatbridge");
    assert.equal(config.nodeEnv, "development");
    assert.equal(config.allowBlindSendRetryOnUnknown, false);

    const redacted = redactChatBridgeConfig(config);
    assert.equal(redacted.standaloneMode, true);
    const serialized = JSON.stringify(redacted);
    assert.ok(!serialized.includes(SYNTHETIC_CHATBRIDGE_DEV_ENV.CHATBRIDGE_DATABASE_URL));
    assert.ok(
      !serialized.includes(SYNTHETIC_CHATBRIDGE_DEV_ENV.CHATBRIDGE_INTEGRATION_HMAC_SECRET)
    );
    assert.ok(
      !serialized.includes(SYNTHETIC_CHATBRIDGE_DEV_ENV.CHATBRIDGE_WEBHOOK_SIGNING_SECRET)
    );
    assert.ok(
      !serialized.includes(
        SYNTHETIC_CHATBRIDGE_DEV_ENV.CHATBRIDGE_OBJECT_STORAGE_SECRET_ACCESS_KEY
      )
    );
    assert.equal(redacted.credentialsConfigured.databaseUrl, "[REDACTED]");
  });

  it("fails closed on missing or invalid configuration and rejects blind send retry on UNKNOWN", () => {
    assert.throws(
      () => loadChatBridgeConfig({}),
      (err: unknown) => {
        assert.ok(err instanceof ChatBridgeConfigError);
        assert.ok(err.issues.length >= 5);
        return true;
      }
    );

    // Rejects non-chatbridge database URL
    assert.throws(
      () =>
        loadChatBridgeConfig({
          ...SYNTHETIC_CHATBRIDGE_DEV_ENV,
          CHATBRIDGE_DATABASE_URL: "postgres://user:pass@127.0.0.1:5432/other_app_db",
        }),
      ChatBridgeConfigError
    );

    // Rejects blind retry on outbound UNKNOWN outcome
    assert.throws(
      () =>
        loadChatBridgeConfig({
          ...SYNTHETIC_CHATBRIDGE_DEV_ENV,
          CHATBRIDGE_ALLOW_BLIND_SEND_RETRY: "true",
        }),
      ChatBridgeConfigError
    );
  });

  it("boots HTTP server and reports readiness (200) when downstream client backends are absent", async () => {
    const handle = await startChatBridgeServer(
      {
        ...SYNTHETIC_CHATBRIDGE_DEV_ENV,
        CHATBRIDGE_PORT: "0",
      },
      {
        database: "READY",
        outboxQueue: "READY",
        objectStorage: "READY",
        downstreamClients: "ABSENT",
      }
    );

    try {
      const healthRes = await fetch(`${handle.baseUrl}/healthz`, {
        headers: { "x-correlation-id": "cb-smoke-01" },
      });
      assert.equal(healthRes.status, 200);
      assert.equal(healthRes.headers.get("x-correlation-id"), "cb-smoke-01");

      const healthText = await healthRes.text();
      assert.ok(!healthText.includes("synthetic_dev_password_only"));
      assert.ok(
        !healthText.includes(SYNTHETIC_CHATBRIDGE_DEV_ENV.CHATBRIDGE_INTEGRATION_HMAC_SECRET)
      );

      const readyRes = await fetch(`${handle.baseUrl}/readyz`);
      assert.equal(readyRes.status, 200);
      const readyBody = (await readyRes.json()) as {
        status: string;
        standaloneMode: boolean;
        dependencies: { downstreamClients: string };
        safeguards: { allowBlindSendRetryOnUnknown: boolean };
      };
      assert.equal(readyBody.status, "ready");
      assert.equal(readyBody.standaloneMode, true);
      assert.equal(readyBody.dependencies.downstreamClients, "ABSENT");
      assert.equal(readyBody.safeguards.allowBlindSendRetryOnUnknown, false);
    } finally {
      await handle.close();
    }
  });

  it("reports 503 not_ready when ChatBridge database or outbox queue is down", async () => {
    const handle = await startChatBridgeServer(
      {
        ...SYNTHETIC_CHATBRIDGE_DEV_ENV,
        CHATBRIDGE_PORT: "0",
      },
      {
        database: "DEGRADED",
        outboxQueue: "READY",
        objectStorage: "READY",
        downstreamClients: "ABSENT",
      }
    );

    try {
      const res = await fetch(`${handle.baseUrl}/readyz`);
      assert.equal(res.status, 503);
      const body = (await res.json()) as { status: string };
      assert.equal(body.status, "not_ready");
    } finally {
      await handle.close();
    }
  });

  it("starts and stops standalone worker entrypoint using synthetic config", async () => {
    const worker = await startChatBridgeWorker({
      ...SYNTHETIC_CHATBRIDGE_DEV_ENV,
      CHATBRIDGE_WORKER_POLL_INTERVAL_MS: "5000",
    });

    try {
      assert.equal(worker.isRunning(), true);
      const tick = await worker.runTick();
      assert.equal(tick.tickNumber, 1);
      assert.deepEqual(tick.queuesChecked, [
        "chatbridge_outbox",
        "chatbridge_inbox_receipts",
        "outbound_unknown_reconcile",
        "assignment_auto_release",
      ]);
      assert.equal(worker.getTickCount(), 1);
    } finally {
      await worker.stop();
      assert.equal(worker.isRunning(), false);
    }
  });

  it("supports live dependency probe transitions on /readyz and fails closed on invalid migration config", async () => {
    let dbState: "DEGRADED" | "READY" = "DEGRADED";
    const handle = await startChatBridgeServer(
      {
        ...SYNTHETIC_CHATBRIDGE_DEV_ENV,
        CHATBRIDGE_PORT: "0",
      },
      async () => ({
        database: dbState,
        outboxQueue: dbState,
        objectStorage: "READY",
        downstreamClients: "ABSENT",
      })
    );

    try {
      const first = await fetch(`${handle.baseUrl}/readyz`);
      assert.equal(first.status, 503);
      dbState = "READY";
      const second = await fetch(`${handle.baseUrl}/readyz`);
      assert.equal(second.status, 200);
    } finally {
      await handle.close();
    }

    await assert.rejects(
      () => runChatBridgeMigrations({ CHATBRIDGE_DB_APP_PASSWORD: "short" }),
      /CHATBRIDGE_DB_APP_PASSWORD is required/
    );
    await assert.rejects(
      () =>
        runChatBridgeMigrations({
          CHATBRIDGE_MIGRATIONS_DIR: "tests/fixtures",
          CHATBRIDGE_DB_APP_USER: "bad-role;drop",
          CHATBRIDGE_DB_APP_PASSWORD: "synthetic_pg_app_password_change_me",
        }),
      /Invalid PostgreSQL role identifier/
    );
  });
});
