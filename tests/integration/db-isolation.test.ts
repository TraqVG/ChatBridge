import { after, before, describe, it } from "node:test";
import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";

function requireDatabaseUrl(): string {
  const value = process.env.DATABASE_URL;
  if (!value || value.trim().length === 0) {
    throw new Error("DATABASE_URL must be set to execute PostgreSQL isolation integration tests");
  }
  return value;
}

const DB_URL: string = requireDatabaseUrl();

const MIGRATION_PATH = path.resolve(
  process.cwd(),
  "database-adapters/migrations/0001_tenant_and_chat.sql"
);

const TENANT_A = "11111111-1111-4111-8111-111111111111";
const TENANT_B = "22222222-2222-4222-8222-222222222222";
const ACC_A = "aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa";
const ACC_B = "bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb";
const SITE_A = "a517e001-1111-4111-8111-000000000001";
const SITE_B = "b517e002-2222-4222-8222-000000000002";
const GUEST_A = "a9e57001-1111-4111-8111-000000000001";
const GUEST_B = "b9e57002-2222-4222-8222-000000000002";
const IDENT_A = "a1de0001-1111-4111-8111-000000000001";
const IDENT_B = "b1de0002-2222-4222-8222-000000000002";
const CONV_A = "c0a10001-1111-4111-8111-000000000001";
const CONV_B = "c0b20002-2222-4222-8222-000000000002";
const MSG_A1 = "a0000001-1111-4111-8111-000000000001";
const MSG_B1 = "b0000002-2222-4222-8222-000000000002";

const SYNTHETIC_APP_LOGIN = "cb_ci_runtime_probe";
const SYNTHETIC_MANGMIE_LOGIN = "mm_ci_foreign_probe";
const SYNTHETIC_PROBE_PASS = "synthetic_isolation_probe_pw_01";
const SYNTHETIC_MANGMIE_DB = "mangmie_isolation_probe";

function reportCiFailure(context: string, err: unknown): never {
  const raw = err instanceof Error ? `${err.message} | ${(err as { stderr?: unknown }).stderr ?? ""}` : String(err);
  const singleLine = raw.replace(/[\r\n]+/g, " ").slice(0, 800);
  if (process.env.GITHUB_ACTIONS === "true") {
    console.log(`::error file=tests/integration/db-isolation.test.ts::${context}: ${singleLine}`);
  }
  throw err;
}

function buildPgEnv(
  baseUrl: string,
  user?: string,
  password?: string,
  database?: string
): NodeJS.ProcessEnv {
  const u = new URL(baseUrl);
  const env: NodeJS.ProcessEnv = {
    ...process.env,
    PGHOST: u.hostname || "127.0.0.1",
    PGPORT: u.port || "5432",
    PGUSER: user ?? decodeURIComponent(u.username),
    PGPASSWORD: password ?? decodeURIComponent(u.password),
    PGDATABASE: database ?? u.pathname.replace(/^\//, ""),
    PGSSLMODE: u.searchParams.get("sslmode") ?? "disable",
  };
  delete env.DATABASE_URL;
  return env;
}

function runPsqlFile(pgEnv: NodeJS.ProcessEnv, filePath: string): void {
  try {
    execFileSync("psql", ["-v", "ON_ERROR_STOP=1", "-X", "-q", "-f", filePath], {
      env: { ...pgEnv, PGOPTIONS: "-c client_min_messages=warning" },
      encoding: "utf8",
    });
  } catch (err) {
    reportCiFailure(`runPsqlFile(${path.basename(filePath)})`, err);
  }
}

function runPsqlCommand(pgEnv: NodeJS.ProcessEnv, sql: string): string {
  try {
    return execFileSync("psql", ["-v", "ON_ERROR_STOP=1", "-X", "-q", "-t", "-A", "-c", sql], {
      env: pgEnv,
      encoding: "utf8",
    }).trim();
  } catch (err) {
    reportCiFailure(`runPsqlCommand(${sql.slice(0, 60)})`, err);
  }
}

function runPsqlScript(pgEnv: NodeJS.ProcessEnv, sql: string): string {
  try {
    return execFileSync("psql", ["-v", "ON_ERROR_STOP=1", "-X", "-q", "-t", "-A"], {
      env: pgEnv,
      input: `${sql}\n`,
      encoding: "utf8",
    }).trim();
  } catch (err) {
    reportCiFailure(`runPsqlScript(${sql.trim().slice(0, 60)})`, err);
  }
}

function runPsqlExpectFailure(pgEnv: NodeJS.ProcessEnv, sql: string): { status: number | null; stderr: string } {
  const res = spawnSync(
    "psql",
    ["-v", "ON_ERROR_STOP=1", "-v", "VERBOSITY=verbose", "-X", "-q"],
    {
      env: pgEnv,
      input: `${sql}\n`,
      encoding: "utf8",
    }
  );
  return { status: res.status, stderr: res.stderr ?? "" };
}

function cleanupTestData(pgEnv: NodeJS.ProcessEnv): void {
  runPsqlScript(
    pgEnv,
    `
    DELETE FROM outbound_intents WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}');
    DELETE FROM messages WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}');
    DELETE FROM conversations WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}');
    DELETE FROM web_chat_browser_links WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}');
    DELETE FROM web_chat_participants WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}');
    DELETE FROM web_chat_sites WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}');
    DELETE FROM channel_identities WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}');
    DELETE FROM channel_accounts WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}');
    DELETE FROM tenants WHERE id IN ('${TENANT_A}', '${TENANT_B}');
    `
  );
}

describe("ID-01-04 ChatBridge PostgreSQL tenant & ownership boundary", () => {
  const ownerEnv = buildPgEnv(DB_URL);
  const appEnv = buildPgEnv(DB_URL, SYNTHETIC_APP_LOGIN, SYNTHETIC_PROBE_PASS);
  const foreignEnv = buildPgEnv(DB_URL, SYNTHETIC_MANGMIE_LOGIN, SYNTHETIC_PROBE_PASS);
  const appToMangmieDbEnv = buildPgEnv(
    DB_URL,
    SYNTHETIC_APP_LOGIN,
    SYNTHETIC_PROBE_PASS,
    SYNTHETIC_MANGMIE_DB
  );

  before(() => {
    assert.ok(fs.existsSync(MIGRATION_PATH), `Missing migration file: ${MIGRATION_PATH}`);

    // Re-apply migration cleanly to verify idempotency.
    runPsqlFile(ownerEnv, MIGRATION_PATH);
    cleanupTestData(ownerEnv);

    // Clean any leftover probe roles/databases using top-level statements outside transactions.
    runPsqlCommand(ownerEnv, `DROP DATABASE IF EXISTS ${SYNTHETIC_MANGMIE_DB};`);
    runPsqlScript(
      ownerEnv,
      `
      DROP ROLE IF EXISTS ${SYNTHETIC_APP_LOGIN};
      DROP ROLE IF EXISTS ${SYNTHETIC_MANGMIE_LOGIN};
      DROP ROLE IF EXISTS mangmie_app;

      CREATE ROLE mangmie_app NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
      CREATE ROLE ${SYNTHETIC_MANGMIE_LOGIN} LOGIN PASSWORD '${SYNTHETIC_PROBE_PASS}'
        NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS IN ROLE mangmie_app;

      CREATE ROLE ${SYNTHETIC_APP_LOGIN} LOGIN PASSWORD '${SYNTHETIC_PROBE_PASS}'
        NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS IN ROLE chatbridge_app;
      `
    );

    runPsqlCommand(
      ownerEnv,
      `CREATE DATABASE ${SYNTHETIC_MANGMIE_DB} OWNER ${SYNTHETIC_MANGMIE_LOGIN};`
    );
    runPsqlCommand(
      ownerEnv,
      `REVOKE ALL ON DATABASE ${SYNTHETIC_MANGMIE_DB} FROM PUBLIC, chatbridge_app, ${SYNTHETIC_APP_LOGIN};`
    );

    // Re-run migration AFTER mangmie_app exists to verify migration explicitly revokes mangmie_app grants too.
    runPsqlFile(ownerEnv, MIGRATION_PATH);
  });

  after(() => {
    cleanupTestData(ownerEnv);
    runPsqlCommand(ownerEnv, `DROP DATABASE IF EXISTS ${SYNTHETIC_MANGMIE_DB};`);
    runPsqlScript(
      ownerEnv,
      `
      DROP ROLE IF EXISTS ${SYNTHETIC_APP_LOGIN};
      DROP ROLE IF EXISTS ${SYNTHETIC_MANGMIE_LOGIN};
      DROP ROLE IF EXISTS mangmie_app;
      `
    );
  });

  it("creates non-privileged runtime and migrator roles without superuser/DDL escalation", () => {
    const row = runPsqlCommand(
      ownerEnv,
      `
      SELECT rolname || ':' || rolsuper || ':' || rolcreatedb || ':' || rolcreaterole || ':' || rolbypassrls
      FROM pg_roles
      WHERE rolname IN ('chatbridge_app', 'chatbridge_migrator')
      ORDER BY rolname;
      `
    );
    assert.deepEqual(row.split(/\r?\n/), [
      "chatbridge_app:false:false:false:false",
      "chatbridge_migrator:false:false:false:false",
    ]);

    const ddlFail = runPsqlExpectFailure(appEnv, "CREATE TABLE illegal_runtime_ddl (id INT);");
    assert.notEqual(ddlFail.status, 0);
    assert.match(ddlFail.stderr, /42501|permission denied/i);

    const dropFail = runPsqlExpectFailure(appEnv, "DROP TABLE tenants;");
    assert.notEqual(dropFail.status, 0);
    assert.match(dropFail.stderr, /42501|must be owner/i);
  });

  it("blocks MangMie runtime credentials from accessing ChatBridge DB and blocks ChatBridge runtime from MangMie DB", () => {
    const foreignConnect = runPsqlExpectFailure(foreignEnv, "SELECT COUNT(*) FROM tenants;");
    assert.notEqual(foreignConnect.status, 0);
    assert.match(foreignConnect.stderr, /42501|permission denied/i);

    const foreignTableSelect = runPsqlExpectFailure(
      ownerEnv,
      "BEGIN; SET LOCAL ROLE mangmie_app; SELECT COUNT(*) FROM public.tenants; ROLLBACK;"
    );
    assert.notEqual(foreignTableSelect.status, 0);
    assert.match(foreignTableSelect.stderr, /42501|permission denied/i);

    const crossDbConnect = runPsqlExpectFailure(appToMangmieDbEnv, "SELECT 1;");
    assert.notEqual(crossDbConnect.status, 0);
    assert.match(crossDbConnect.stderr, /42501|permission denied/i);
  });

  it("supports two independent tenants sharing external sender refs and idempotency keys under runtime role", () => {
    const count = runPsqlScript(
      appEnv,
      `
      BEGIN;
      INSERT INTO tenants (id, slug, display_name) VALUES
        ('${TENANT_A}', 'tenant-alpha', 'Alpha Tenant'),
        ('${TENANT_B}', 'tenant-beta', 'Beta Tenant');

      INSERT INTO channel_accounts (id, tenant_id, provider, external_ref, display_name) VALUES
        ('${ACC_A}', '${TENANT_A}', 'LINE', 'line-oa-shared-ref', 'Alpha LINE OA'),
        ('${ACC_B}', '${TENANT_B}', 'LINE', 'line-oa-shared-ref', 'Beta LINE OA');

      INSERT INTO channel_identities (id, tenant_id, channel_account_id, provider_sender_ref, scope_key) VALUES
        ('${IDENT_A}', '${TENANT_A}', '${ACC_A}', 'U-shared-sender-001', 'sender:001'),
        ('${IDENT_B}', '${TENANT_B}', '${ACC_B}', 'U-shared-sender-001', 'sender:001');

      INSERT INTO web_chat_sites (id, tenant_id, channel_account_id, site_key, allowed_origins) VALUES
        ('${SITE_A}', '${TENANT_A}', '${ACC_A}', 'site-alpha-public', '["https://alpha.example"]'::jsonb),
        ('${SITE_B}', '${TENANT_B}', '${ACC_B}', 'site-beta-public', '["https://beta.example"]'::jsonb);

      INSERT INTO web_chat_participants (id, tenant_id, site_id, pseudonymous_handle) VALUES
        ('${GUEST_A}', '${TENANT_A}', '${SITE_A}', 'guest-alpha-1'),
        ('${GUEST_B}', '${TENANT_B}', '${SITE_B}', 'guest-beta-1');

      INSERT INTO web_chat_browser_links (
        id, tenant_id, site_id, participant_id, recognition_secret_hash, issued_at, expires_at
      ) VALUES (
        'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1',
        '${TENANT_A}', '${SITE_A}', '${GUEST_A}', 'sha256:secret-same-hash',
        '2026-11-01T00:00:00Z'::timestamptz, '2026-11-01T00:00:00Z'::timestamptz + INTERVAL '59 days'
      );

      INSERT INTO conversations (id, tenant_id, channel_account_id, identity_id, status, last_seq) VALUES
        ('${CONV_A}', '${TENANT_A}', '${ACC_A}', '${IDENT_A}', 'OPEN', 1),
        ('${CONV_B}', '${TENANT_B}', '${ACC_B}', '${IDENT_B}', 'OPEN', 1);

      INSERT INTO messages (id, tenant_id, conversation_id, seq, direction, sender_kind, body) VALUES
        ('${MSG_A1}', '${TENANT_A}', '${CONV_A}', 1, 'IN', 'CUSTOMER', 'Hello Alpha'),
        ('${MSG_B1}', '${TENANT_B}', '${CONV_B}', 1, 'IN', 'CUSTOMER', 'Hello Beta');

      INSERT INTO outbound_intents (
        id, tenant_id, conversation_id, idempotency_key, payload_hash, actor_ref, source, state
      ) VALUES
        ('aaaa1111-1111-4111-8111-111111111111', '${TENANT_A}', '${CONV_A}', 'shared-idem-key', 'hash-a', 'staff:1', 'STAFF', 'QUEUED'),
        ('bbbb2222-2222-4222-8222-222222222222', '${TENANT_B}', '${CONV_B}', 'shared-idem-key', 'hash-b', 'staff:2', 'STAFF', 'QUEUED');

      SELECT COUNT(*) FROM messages WHERE id IN ('${MSG_A1}', '${MSG_B1}');
      COMMIT;
      `
    );
    assert.equal(count.split(/\r?\n/).at(-1), "2");
  });

  it("rejects foreign-tenant composite FK references and policy invariant violations at DB constraint layer", () => {
    const crossTenantMsg = runPsqlExpectFailure(
      appEnv,
      `
      INSERT INTO messages (id, tenant_id, conversation_id, seq, direction, sender_kind, body)
      VALUES ('99999999-9999-4999-8999-000000000001', '${TENANT_B}', '${CONV_A}', 2, 'IN', 'CUSTOMER', 'Forged cross-tenant');
      `
    );
    assert.notEqual(crossTenantMsg.status, 0);
    assert.match(crossTenantMsg.stderr, /23503|foreign key constraint/i);

    const crossTenantOutbound = runPsqlExpectFailure(
      appEnv,
      `
      INSERT INTO outbound_intents (
        id, tenant_id, conversation_id, idempotency_key, payload_hash, actor_ref, source
      ) VALUES (
        '99999999-9999-4999-8999-000000000002', '${TENANT_B}', '${CONV_A}', 'forged-key', 'hash', 'staff:evil', 'STAFF'
      );
      `
    );
    assert.notEqual(crossTenantOutbound.status, 0);
    assert.match(crossTenantOutbound.stderr, /23503|foreign key constraint/i);

    const crossTenantWebLink = runPsqlExpectFailure(
      appEnv,
      `
      INSERT INTO web_chat_browser_links (
        id, tenant_id, site_id, participant_id, recognition_secret_hash, issued_at, expires_at
      ) VALUES (
        '99999999-9999-4999-8999-000000000003',
        '${TENANT_B}', '${SITE_A}', '${GUEST_A}', 'sha256:forged',
        '2026-11-01T00:00:00Z', '2026-11-30T00:00:00Z'
      );
      `
    );
    assert.notEqual(crossTenantWebLink.status, 0);
    assert.match(crossTenantWebLink.stderr, /23503|foreign key constraint/i);

    const excessiveWebTtl = runPsqlExpectFailure(
      appEnv,
      `
      INSERT INTO web_chat_browser_links (
        id, tenant_id, site_id, participant_id, recognition_secret_hash, issued_at, expires_at
      ) VALUES (
        '99999999-9999-4999-8999-000000000004',
        '${TENANT_A}', '${SITE_A}', '${GUEST_A}', 'sha256:too-long',
        '2026-11-01T00:00:00Z', '2027-02-01T00:00:00Z'
      );
      `
    );
    assert.notEqual(excessiveWebTtl.status, 0);
    assert.match(excessiveWebTtl.stderr, /23514|check constraint/i);

    const presignedUrlInMedia = runPsqlExpectFailure(
      appEnv,
      `
      INSERT INTO media_assets (id, tenant_id, content_type, bytes, storage_object_key)
      VALUES (
        '99999999-9999-4999-8999-000000000005',
        '${TENANT_A}', 'image/png', 1024, 'https://seaweed.internal/bucket/obj?X-Amz-Signature=secret'
      );
      `
    );
    assert.notEqual(presignedUrlInMedia.status, 0);
    assert.match(presignedUrlInMedia.stderr, /23514|check constraint/i);
  });
});
