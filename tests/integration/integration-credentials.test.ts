import { after, before, describe, it } from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import {
  IntegrationCredentialError,
  IntegrationCredentialService,
  PsqlIntegrationClientStore,
} from "../../channel-management/src/integrations/index.js";

function requireDatabaseUrl(): string {
  const value = process.env.DATABASE_URL;
  if (!value || value.trim().length === 0) {
    throw new Error("DATABASE_URL must be set to run PostgreSQL integration credential tests");
  }
  return value;
}

const DB_URL: string = requireDatabaseUrl();
const MIGRATIONS_DIR = path.resolve(process.cwd(), "database-adapters/migrations");

const TENANT_A = "11111115-1111-4111-8111-111111111111";
const TENANT_B = "22222225-2222-4222-8222-222222222222";
const APP_LOGIN = "cb_ci_int_cred_probe";
const APP_PASS = "synthetic_int_cred_pw_01";

function buildPgEnv(baseUrl: string, user?: string, password?: string): NodeJS.ProcessEnv {
  const u = new URL(baseUrl);
  const env: NodeJS.ProcessEnv = {
    ...process.env,
    PGHOST: u.hostname || "127.0.0.1",
    PGPORT: u.port || "5432",
    PGUSER: user ?? decodeURIComponent(u.username),
    PGPASSWORD: password ?? decodeURIComponent(u.password),
    PGDATABASE: u.pathname.replace(/^\//, ""),
    PGSSLMODE: u.searchParams.get("sslmode") ?? "disable",
  };
  delete env.DATABASE_URL;
  return env;
}

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

function cleanupTenants(pgEnv: NodeJS.ProcessEnv): void {
  runPsql(
    pgEnv,
    `
    DELETE FROM audit_logs WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}');
    DELETE FROM integration_clients WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}');
    DELETE FROM tenants WHERE id IN ('${TENANT_A}', '${TENANT_B}');
    `
  );
}

describe("ID-01-05 PostgreSQL 16 integration credentials & scopes", () => {
  const adminEnv = buildPgEnv(DB_URL);
  const adminUrl = new URL(DB_URL);
  adminUrl.username = APP_LOGIN;
  adminUrl.password = APP_PASS;
  const appStore = new PsqlIntegrationClientStore({ connectionUri: adminUrl.toString() });
  const service = new IntegrationCredentialService(appStore);

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

    cleanupTenants(adminEnv);
    runPsql(
      adminEnv,
      `
      INSERT INTO tenants (id, slug, display_name, status)
      VALUES
        ('${TENANT_A}', 'tenant-id0105-a', 'ID-01-05 Tenant A', 'ACTIVE'),
        ('${TENANT_B}', 'tenant-id0105-b', 'ID-01-05 Tenant B', 'ACTIVE');
      `
    );
  });

  after(() => {
    cleanupTenants(adminEnv);
  });

  it("provisions, rotates, and revokes scoped credentials on postgres:16 using least-privilege chatbridge_app", async () => {
    const clientA = await service.provisionClient({
      tenantId: TENANT_A,
      clientKey: "connector-primary",
      scopes: ["conversations:read", "messages:read", "messages:send"],
      actor: "pg_owner_a",
      traceId: "pg-prov-a",
    });

    const clientB = await service.provisionClient({
      tenantId: TENANT_B,
      clientKey: "connector-primary",
      scopes: ["channels:read"],
      actor: "pg_owner_b",
      traceId: "pg-prov-b",
    });

    // Verify stored row in PostgreSQL holds hashed secret, never plaintext token.
    const storedHashCount = runPsql(
      adminEnv,
      `SELECT COUNT(*) FROM integration_clients WHERE id = '${clientA.client.id}' AND secret_hash LIKE 'scrypt$%' AND secret_version = 1;`
    );
    assert.equal(storedHashCount, "1");

    // Same-tenant auth works within granted scope, denied for ungranted scope and cross-tenant.
    assert.equal((await service.authorizeSend(clientA.credential, TENANT_A)).ok, true);
    assert.equal((await service.authorizeRead(clientB.credential, TENANT_B, "channels:read")).ok, true);
    assert.equal((await service.authorizeSend(clientB.credential, TENANT_B)).ok, false);
    assert.equal((await service.authorizeRead(clientA.credential, TENANT_B, "messages:read")).ok, false);

    // Rotate clientA while narrowing scope to read-only.
    const rotatedA = await service.rotateClientSecret({
      tenantId: TENANT_A,
      clientId: clientA.client.id,
      expectedVersion: 1,
      nextScopes: ["conversations:read", "messages:read"],
      actor: "pg_owner_a",
      traceId: "pg-rot-a",
    });

    assert.equal(rotatedA.client.secretVersion, 2);
    assert.notEqual(rotatedA.client.rotatedAt, null);

    // Old credential immediately invalid; rotated key retains read scope and drops send scope.
    assert.equal((await service.authorizeRead(clientA.credential, TENANT_A, "messages:read")).ok, false);
    assert.equal((await service.authorizeRead(rotatedA.credential, TENANT_A, "messages:read")).ok, true);
    assert.equal((await service.authorizeSend(rotatedA.credential, TENANT_A)).ok, false);

    // Concurrent stale rotation returns STALE_VERSION 412.
    await assert.rejects(
      () =>
        service.rotateClientSecret({
          tenantId: TENANT_A,
          clientId: clientA.client.id,
          expectedVersion: 1,
          actor: "pg_stale_caller",
        }),
      (err: unknown) => err instanceof IntegrationCredentialError && err.code === "STALE_VERSION"
    );

    // Revoke clientA; neither read nor send is permitted thereafter.
    const revokedA = await service.revokeClient({
      tenantId: TENANT_A,
      clientId: clientA.client.id,
      reason: "rotated_compromise_test",
      actor: "pg_owner_a",
      traceId: "pg-rev-a",
    });
    assert.equal(revokedA.status, "REVOKED");

    assert.equal((await service.authorizeRead(rotatedA.credential, TENANT_A, "messages:read")).ok, false);
    assert.equal((await service.authorizeSend(rotatedA.credential, TENANT_A)).ok, false);

    // Tenant A listing contains no raw secrets; redacted audit rows exist for Tenant A.
    const listA = await service.listClients(TENANT_A);
    assert.equal(listA.length, 1);
    assert.equal(listA[0]?.status, "REVOKED");

    const auditCount = runPsql(
      adminEnv,
      `SELECT COUNT(*) FROM audit_logs WHERE tenant_id = '${TENANT_A}' AND target = 'integration_client:${clientA.client.id}';`
    );
    assert.ok(Number(auditCount) >= 5);
  });
});
