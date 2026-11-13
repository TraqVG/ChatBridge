import { execFile } from "node:child_process";
import { promisify } from "node:util";
import {
  IntegrationCredentialError,
  normalizeIntegrationScopes,
  type ChatBridgeIntegrationScope,
  type IntegrationClientAuditRecord,
  type IntegrationClientRecord,
  type IntegrationClientStatus,
} from "./types.js";
import type {
  CreateIntegrationClientInput,
  IntegrationClientStore,
  RevokeIntegrationClientInput,
  RotateIntegrationClientInput,
} from "./store.js";

const execFileAsync = promisify(execFile);

export interface PsqlConnectionConfig {
  readonly connectionUri?: string;
  readonly host?: string;
  readonly port?: number;
  readonly user?: string;
  readonly password?: string;
  readonly database?: string;
  readonly sslMode?: string;
}

interface RawPgRow {
  readonly id: string;
  readonly tenant_id: string;
  readonly client_key: string;
  readonly secret_hash: string;
  readonly secret_version: number;
  readonly scopes: readonly string[];
  readonly status: IntegrationClientStatus;
  readonly rotated_at: string | null;
  readonly created_at: string;
  readonly updated_at: string;
}

/** Encodes arbitrary runtime strings via UTF-8 base64 so zero untrusted interpolation enters SQL syntax. */
function sqlTextB64(value: string): string {
  const b64 = Buffer.from(value, "utf8").toString("base64");
  if (!/^[A-Za-z0-9+/=]*$/.test(b64)) {
    throw new IntegrationCredentialError(
      "VALIDATION_ERROR",
      400,
      "Invalid character encoding in integration parameter."
    );
  }
  return `convert_from(decode('${b64}', 'base64'), 'utf8')`;
}

function sqlJsonbB64(value: unknown): string {
  return `(${sqlTextB64(JSON.stringify(value))})::jsonb`;
}

function sqlUuidLiteral(uuid: string): string {
  return `(${sqlTextB64(uuid)})::uuid`;
}

function sqlTimestamptzLiteral(iso: string): string {
  return `(${sqlTextB64(iso)})::timestamptz`;
}

function mapRow(row: RawPgRow): IntegrationClientRecord {
  const normalizedScopes: readonly ChatBridgeIntegrationScope[] =
    normalizeIntegrationScopes(row.scopes);
  return Object.freeze({
    id: row.id,
    tenantId: row.tenant_id,
    clientKey: row.client_key,
    secretHash: row.secret_hash,
    secretVersion: Number(row.secret_version),
    scopes: normalizedScopes,
    status: row.status,
    rotatedAt: row.rotated_at,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  });
}

function buildPgEnv(config: PsqlConnectionConfig): NodeJS.ProcessEnv {
  const env: NodeJS.ProcessEnv = { ...process.env };
  delete env.DATABASE_URL;

  if (config.connectionUri) {
    const u = new URL(config.connectionUri);
    env.PGHOST = config.host ?? (u.hostname || "127.0.0.1");
    env.PGPORT = String(config.port ?? (u.port || "5432"));
    env.PGUSER = config.user ?? decodeURIComponent(u.username);
    env.PGPASSWORD = config.password ?? decodeURIComponent(u.password);
    env.PGDATABASE = config.database ?? u.pathname.replace(/^\//, "");
    env.PGSSLMODE = config.sslMode ?? u.searchParams.get("sslmode") ?? "disable";
    return env;
  }

  if (config.host !== undefined) env.PGHOST = config.host;
  if (config.port !== undefined) env.PGPORT = String(config.port);
  if (config.user !== undefined) env.PGUSER = config.user;
  if (config.password !== undefined) env.PGPASSWORD = config.password;
  if (config.database !== undefined) env.PGDATABASE = config.database;
  if (config.sslMode !== undefined) env.PGSSLMODE = config.sslMode;
  return env;
}

const CLIENT_JSON_PROJECTION = `json_build_object(
  'id', id::text,
  'tenant_id', tenant_id::text,
  'client_key', client_key,
  'secret_hash', secret_hash,
  'secret_version', secret_version,
  'scopes', scopes,
  'status', status,
  'rotated_at', rotated_at,
  'created_at', created_at,
  'updated_at', updated_at
)`;

export class PsqlIntegrationClientStore implements IntegrationClientStore {
  private readonly pgEnv: NodeJS.ProcessEnv;

  constructor(config: PsqlConnectionConfig) {
    this.pgEnv = buildPgEnv(config);
  }

  private async execJson<T>(sql: string): Promise<T | null> {
    try {
      const { stdout } = await execFileAsync(
        "psql",
        ["-v", "ON_ERROR_STOP=1", "-X", "-q", "-t", "-A", "-c", sql],
        {
          env: this.pgEnv,
          encoding: "utf8",
        }
      );
      const trimmed = stdout.trim();
      if (!trimmed) {
        return null;
      }
      return JSON.parse(trimmed) as T;
    } catch (err: unknown) {
      const stderr =
        err instanceof Error && "stderr" in err
          ? String((err as { stderr?: unknown }).stderr ?? "")
          : "";
      if (stderr.includes("23505")) {
        throw new IntegrationCredentialError(
          "STATE_CONFLICT",
          409,
          "Integration client key or identifier already exists for this tenant."
        );
      }
      throw err;
    }
  }

  async createClient(
    input: CreateIntegrationClientInput
  ): Promise<IntegrationClientRecord> {
    const sql = `
      INSERT INTO integration_clients (
        id, tenant_id, client_key, secret_hash, secret_version, scopes, status, rotated_at, created_at, updated_at
      )
      VALUES (
        ${sqlUuidLiteral(input.id)},
        ${sqlUuidLiteral(input.tenantId)},
        ${sqlTextB64(input.clientKey)},
        ${sqlTextB64(input.secretHash)},
        1,
        ${sqlJsonbB64(input.scopes)},
        'ACTIVE',
        NULL,
        ${sqlTimestamptzLiteral(input.nowIso)},
        ${sqlTimestamptzLiteral(input.nowIso)}
      )
      RETURNING ${CLIENT_JSON_PROJECTION};
    `;
    const row = await this.execJson<RawPgRow>(sql);
    if (!row) {
      throw new IntegrationCredentialError(
        "STATE_CONFLICT",
        409,
        "Unable to persist integration client."
      );
    }
    return mapRow(row);
  }

  async findById(
    tenantId: string,
    clientId: string
  ): Promise<IntegrationClientRecord | null> {
    const sql = `
      SELECT ${CLIENT_JSON_PROJECTION}
      FROM integration_clients
      WHERE tenant_id = ${sqlUuidLiteral(tenantId)}
        AND id = ${sqlUuidLiteral(clientId)}
      LIMIT 1;
    `;
    const row = await this.execJson<RawPgRow>(sql);
    return row ? mapRow(row) : null;
  }

  async findByClientKey(
    tenantId: string,
    clientKey: string
  ): Promise<IntegrationClientRecord | null> {
    const sql = `
      SELECT ${CLIENT_JSON_PROJECTION}
      FROM integration_clients
      WHERE tenant_id = ${sqlUuidLiteral(tenantId)}
        AND client_key = ${sqlTextB64(clientKey)}
      LIMIT 1;
    `;
    const row = await this.execJson<RawPgRow>(sql);
    return row ? mapRow(row) : null;
  }

  async rotateClientSecret(
    input: RotateIntegrationClientInput
  ): Promise<{
    readonly record: IntegrationClientRecord;
    readonly previousSecretVersion: number;
  }> {
    const versionClause =
      typeof input.expectedVersion === "number"
        ? `AND secret_version = ${Math.trunc(input.expectedVersion)}`
        : "";

    const updateSql = `
      UPDATE integration_clients
      SET
        secret_hash = ${sqlTextB64(input.nextSecretHash)},
        secret_version = secret_version + 1,
        scopes = ${sqlJsonbB64(input.nextScopes)},
        status = 'ACTIVE',
        rotated_at = ${sqlTimestamptzLiteral(input.nowIso)},
        updated_at = ${sqlTimestamptzLiteral(input.nowIso)}
      WHERE tenant_id = ${sqlUuidLiteral(input.tenantId)}
        AND id = ${sqlUuidLiteral(input.id)}
        AND status <> 'REVOKED'
        ${versionClause}
      RETURNING ${CLIENT_JSON_PROJECTION};
    `;

    const updated = await this.execJson<RawPgRow>(updateSql);
    if (updated) {
      const mapped = mapRow(updated);
      return {
        record: mapped,
        previousSecretVersion: mapped.secretVersion - 1,
      };
    }

    const current = await this.findById(input.tenantId, input.id);
    if (!current) {
      throw new IntegrationCredentialError(
        "NOT_FOUND",
        404,
        "Integration client not found for tenant."
      );
    }
    if (current.status === "REVOKED") {
      throw new IntegrationCredentialError(
        "STATE_CONFLICT",
        409,
        "Revoked integration clients cannot be rotated; provision a fresh client.",
        current.secretVersion
      );
    }
    throw new IntegrationCredentialError(
      "STALE_VERSION",
      412,
      "Secret version mismatch; refresh integration client before rotating.",
      current.secretVersion
    );
  }

  async revokeClient(
    input: RevokeIntegrationClientInput
  ): Promise<IntegrationClientRecord> {
    const sql = `
      UPDATE integration_clients
      SET
        status = 'REVOKED',
        updated_at = CASE WHEN status = 'REVOKED' THEN updated_at ELSE ${sqlTimestamptzLiteral(input.nowIso)} END
      WHERE tenant_id = ${sqlUuidLiteral(input.tenantId)}
        AND id = ${sqlUuidLiteral(input.id)}
      RETURNING ${CLIENT_JSON_PROJECTION};
    `;
    const row = await this.execJson<RawPgRow>(sql);
    if (!row) {
      throw new IntegrationCredentialError(
        "NOT_FOUND",
        404,
        "Integration client not found for tenant."
      );
    }
    return mapRow(row);
  }

  async listByTenant(
    tenantId: string,
    limit: number
  ): Promise<readonly IntegrationClientRecord[]> {
    const bounded = Math.max(1, Math.min(200, Math.floor(limit)));
    const sql = `
      SELECT COALESCE(json_agg(sub.obj), '[]'::json)
      FROM (
        SELECT ${CLIENT_JSON_PROJECTION} AS obj
        FROM integration_clients
        WHERE tenant_id = ${sqlUuidLiteral(tenantId)}
        ORDER BY created_at ASC, id ASC
        LIMIT ${bounded}
      ) sub;
    `;
    const rows = await this.execJson<readonly RawPgRow[]>(sql);
    if (!rows) {
      return Object.freeze([]);
    }
    return Object.freeze(rows.map(mapRow));
  }

  async appendAudit(record: IntegrationClientAuditRecord): Promise<void> {
    const traceClause =
      record.traceId !== null ? sqlTextB64(record.traceId) : "NULL";
    const sql = `
      INSERT INTO audit_logs (
        id, tenant_id, actor, action, result, target, trace_id, occurred_at, metadata_redacted
      )
      VALUES (
        ${sqlUuidLiteral(record.id)},
        ${sqlUuidLiteral(record.tenantId)},
        ${sqlTextB64(record.actor)},
        ${sqlTextB64(record.action)},
        ${sqlTextB64(record.result)},
        ${sqlTextB64(record.target)},
        ${traceClause},
        ${sqlTimestamptzLiteral(record.occurredAt)},
        ${sqlJsonbB64(record.metadataRedacted)}
      );
    `;
    await this.execJson<unknown>(sql);
  }
}
