import {
  IntegrationCredentialError,
  type ChatBridgeIntegrationScope,
  type IntegrationClientAuditRecord,
  type IntegrationClientRecord,
} from "./types.js";

export interface CreateIntegrationClientInput {
  readonly id: string;
  readonly tenantId: string;
  readonly clientKey: string;
  readonly secretHash: string;
  readonly scopes: readonly ChatBridgeIntegrationScope[];
  readonly nowIso: string;
}

export interface RotateIntegrationClientInput {
  readonly id: string;
  readonly tenantId: string;
  readonly expectedVersion?: number | null;
  readonly nextSecretHash: string;
  readonly nextScopes: readonly ChatBridgeIntegrationScope[];
  readonly nowIso: string;
}

export interface RevokeIntegrationClientInput {
  readonly id: string;
  readonly tenantId: string;
  readonly nowIso: string;
}

export interface IntegrationClientStore {
  createClient(input: CreateIntegrationClientInput): Promise<IntegrationClientRecord>;
  findById(tenantId: string, clientId: string): Promise<IntegrationClientRecord | null>;
  findByClientKey(
    tenantId: string,
    clientKey: string
  ): Promise<IntegrationClientRecord | null>;
  rotateClientSecret(
    input: RotateIntegrationClientInput
  ): Promise<{
    readonly record: IntegrationClientRecord;
    readonly previousSecretVersion: number;
  }>;
  revokeClient(input: RevokeIntegrationClientInput): Promise<IntegrationClientRecord>;
  listByTenant(tenantId: string, limit: number): Promise<readonly IntegrationClientRecord[]>;
  appendAudit(record: IntegrationClientAuditRecord): Promise<void>;
}

function cloneRecord(record: IntegrationClientRecord): IntegrationClientRecord {
  return Object.freeze({
    ...record,
    scopes: Object.freeze([...record.scopes]),
  });
}

export class InMemoryIntegrationClientStore implements IntegrationClientStore {
  private readonly byId = new Map<string, IntegrationClientRecord>();
  private readonly keyIndex = new Map<string, string>();
  private readonly audits: IntegrationClientAuditRecord[] = [];

  private compositeKey(tenantId: string, clientKey: string): string {
    return `${tenantId}::${clientKey}`;
  }

  async createClient(
    input: CreateIntegrationClientInput
  ): Promise<IntegrationClientRecord> {
    if (this.byId.has(input.id)) {
      throw new IntegrationCredentialError(
        "STATE_CONFLICT",
        409,
        "Integration client identifier already exists."
      );
    }
    const ck = this.compositeKey(input.tenantId, input.clientKey);
    if (this.keyIndex.has(ck)) {
      throw new IntegrationCredentialError(
        "STATE_CONFLICT",
        409,
        "Integration client key already exists for this tenant."
      );
    }

    const record: IntegrationClientRecord = {
      id: input.id,
      tenantId: input.tenantId,
      clientKey: input.clientKey,
      secretHash: input.secretHash,
      secretVersion: 1,
      scopes: Object.freeze([...input.scopes]),
      status: "ACTIVE",
      rotatedAt: null,
      createdAt: input.nowIso,
      updatedAt: input.nowIso,
    };

    this.byId.set(record.id, record);
    this.keyIndex.set(ck, record.id);
    return cloneRecord(record);
  }

  async findById(
    tenantId: string,
    clientId: string
  ): Promise<IntegrationClientRecord | null> {
    const existing = this.byId.get(clientId);
    if (!existing || existing.tenantId !== tenantId) {
      return null;
    }
    return cloneRecord(existing);
  }

  async findByClientKey(
    tenantId: string,
    clientKey: string
  ): Promise<IntegrationClientRecord | null> {
    const id = this.keyIndex.get(this.compositeKey(tenantId, clientKey));
    if (!id) {
      return null;
    }
    const existing = this.byId.get(id);
    if (!existing || existing.tenantId !== tenantId) {
      return null;
    }
    return cloneRecord(existing);
  }

  async rotateClientSecret(
    input: RotateIntegrationClientInput
  ): Promise<{
    readonly record: IntegrationClientRecord;
    readonly previousSecretVersion: number;
  }> {
    const existing = this.byId.get(input.id);
    if (!existing || existing.tenantId !== input.tenantId) {
      throw new IntegrationCredentialError(
        "NOT_FOUND",
        404,
        "Integration client not found for tenant."
      );
    }

    if (existing.status === "REVOKED") {
      throw new IntegrationCredentialError(
        "STATE_CONFLICT",
        409,
        "Revoked integration clients cannot be rotated; provision a fresh client.",
        existing.secretVersion
      );
    }

    if (
      typeof input.expectedVersion === "number" &&
      existing.secretVersion !== input.expectedVersion
    ) {
      throw new IntegrationCredentialError(
        "STALE_VERSION",
        412,
        "Secret version mismatch; refresh integration client before rotating.",
        existing.secretVersion
      );
    }

    const updated: IntegrationClientRecord = {
      ...existing,
      secretHash: input.nextSecretHash,
      secretVersion: existing.secretVersion + 1,
      scopes: Object.freeze([...input.nextScopes]),
      status: "ACTIVE",
      rotatedAt: input.nowIso,
      updatedAt: input.nowIso,
    };

    this.byId.set(updated.id, updated);
    return {
      record: cloneRecord(updated),
      previousSecretVersion: existing.secretVersion,
    };
  }

  async revokeClient(
    input: RevokeIntegrationClientInput
  ): Promise<IntegrationClientRecord> {
    const existing = this.byId.get(input.id);
    if (!existing || existing.tenantId !== input.tenantId) {
      throw new IntegrationCredentialError(
        "NOT_FOUND",
        404,
        "Integration client not found for tenant."
      );
    }

    if (existing.status === "REVOKED") {
      return cloneRecord(existing);
    }

    const updated: IntegrationClientRecord = {
      ...existing,
      status: "REVOKED",
      updatedAt: input.nowIso,
    };

    this.byId.set(updated.id, updated);
    return cloneRecord(updated);
  }

  async listByTenant(
    tenantId: string,
    limit: number
  ): Promise<readonly IntegrationClientRecord[]> {
    const bounded = Math.max(1, Math.min(200, Math.floor(limit)));
    const matches: IntegrationClientRecord[] = [];
    for (const record of this.byId.values()) {
      if (record.tenantId === tenantId) {
        matches.push(cloneRecord(record));
      }
    }
    matches.sort((a, b) => a.createdAt.localeCompare(b.createdAt) || a.id.localeCompare(b.id));
    return Object.freeze(matches.slice(0, bounded));
  }

  async appendAudit(record: IntegrationClientAuditRecord): Promise<void> {
    this.audits.push(Object.freeze({
      ...record,
      metadataRedacted: Object.freeze({ ...record.metadataRedacted }),
    }));
  }

  listAuditsForTenant(tenantId: string): readonly IntegrationClientAuditRecord[] {
    return Object.freeze(
      this.audits
        .filter((item) => item.tenantId === tenantId)
        .map((item) => ({
          ...item,
          metadataRedacted: Object.freeze({ ...item.metadataRedacted }),
        }))
    );
  }
}
