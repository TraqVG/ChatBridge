import { randomUUID } from "node:crypto";
import {
  formatIntegrationCredential,
  generateRawSecret,
  hashIntegrationSecret,
  parseIntegrationCredential,
  verifyIntegrationSecret,
} from "./crypto.js";
import type { IntegrationClientStore } from "./store.js";
import {
  assertValidClientKey,
  assertValidUuid,
  IntegrationCredentialError,
  normalizeIntegrationScopes,
  toSafeClientSummary,
  type AuthorizationDecision,
  type ChatBridgeIntegrationScope,
  type IntegrationClientSafeSummary,
  type ProvisionedIntegrationClient,
  type RotatedIntegrationClient,
} from "./types.js";

export interface ProvisionClientRequest {
  readonly tenantId: string;
  readonly clientKey: string;
  readonly scopes: readonly ChatBridgeIntegrationScope[];
  readonly actor: string;
  readonly traceId?: string | null;
}

export interface RotateClientRequest {
  readonly tenantId: string;
  readonly clientId: string;
  readonly expectedVersion?: number | null;
  readonly nextScopes?: readonly ChatBridgeIntegrationScope[] | null;
  readonly allowScopeExpansion?: boolean;
  readonly actor: string;
  readonly traceId?: string | null;
}

export interface RevokeClientRequest {
  readonly tenantId: string;
  readonly clientId: string;
  readonly reason?: string | null;
  readonly actor: string;
  readonly traceId?: string | null;
}

export interface AuthorizeRequest {
  readonly credential: string;
  readonly targetTenantId: string;
  readonly requiredScope: ChatBridgeIntegrationScope;
  readonly traceId?: string | null;
}

/**
 * Standalone ChatBridge service managing multi-tenant integration client
 * provisioning, capability scopes, secret rotation, revocation, and fail-closed
 * read/send authorization independently of any downstream client backend.
 */
export class IntegrationCredentialService {
  private readonly store: IntegrationClientStore;
  private readonly nowFn: () => Date;

  constructor(store: IntegrationClientStore, nowFn: () => Date = () => new Date()) {
    this.store = store;
    this.nowFn = nowFn;
  }

  async provisionClient(
    req: ProvisionClientRequest
  ): Promise<ProvisionedIntegrationClient> {
    const tenantId = assertValidUuid(req.tenantId, "tenantId");
    const clientKey = assertValidClientKey(req.clientKey);
    const scopes = normalizeIntegrationScopes(req.scopes);
    const actor = req.actor.trim();
    if (actor.length === 0) {
      throw new IntegrationCredentialError(
        "VALIDATION_ERROR",
        400,
        "Field 'actor' is required for audit trail."
      );
    }

    const nowIso = this.nowFn().toISOString();
    const clientId = randomUUID();
    const rawSecret = generateRawSecret();
    const secretHash = hashIntegrationSecret(rawSecret);

    const record = await this.store.createClient({
      id: clientId,
      tenantId,
      clientKey,
      secretHash,
      scopes,
      nowIso,
    });

    await this.store.appendAudit({
      id: randomUUID(),
      tenantId,
      actor,
      action: "INTEGRATION_CLIENT_PROVISIONED",
      result: "SUCCESS",
      target: `integration_client:${record.id}`,
      traceId: req.traceId ?? null,
      occurredAt: nowIso,
      metadataRedacted: {
        client_key: record.clientKey,
        secret_version: record.secretVersion,
        scopes: [...record.scopes],
        status: record.status,
      },
    });

    return {
      client: toSafeClientSummary(record),
      credential: formatIntegrationCredential(tenantId, clientKey, rawSecret),
    };
  }

  async rotateClientSecret(
    req: RotateClientRequest
  ): Promise<RotatedIntegrationClient> {
    const tenantId = assertValidUuid(req.tenantId, "tenantId");
    const clientId = assertValidUuid(req.clientId, "clientId");
    const actor = req.actor.trim();
    if (actor.length === 0) {
      throw new IntegrationCredentialError(
        "VALIDATION_ERROR",
        400,
        "Field 'actor' is required for audit trail."
      );
    }

    const existing = await this.store.findById(tenantId, clientId);
    if (!existing) {
      throw new IntegrationCredentialError(
        "NOT_FOUND",
        404,
        "Integration client not found for tenant."
      );
    }

    const nextScopes =
      req.nextScopes != null
        ? normalizeIntegrationScopes(req.nextScopes)
        : existing.scopes;

    if (!req.allowScopeExpansion) {
      const existingSet = new Set<ChatBridgeIntegrationScope>(existing.scopes);
      const unauthorized = nextScopes.filter((s) => !existingSet.has(s));
      if (unauthorized.length > 0) {
        throw new IntegrationCredentialError(
          "FORBIDDEN",
          403,
          `Secret rotation cannot expand capability scopes without explicit expansion approval: ${unauthorized.join(", ")}.`,
          existing.secretVersion
        );
      }
    }

    const nowIso = this.nowFn().toISOString();
    const rawSecret = generateRawSecret();
    const nextSecretHash = hashIntegrationSecret(rawSecret);

    const rotated = await this.store.rotateClientSecret({
      id: clientId,
      tenantId,
      expectedVersion: req.expectedVersion ?? null,
      nextSecretHash,
      nextScopes,
      nowIso,
    });

    await this.store.appendAudit({
      id: randomUUID(),
      tenantId,
      actor,
      action: "INTEGRATION_CLIENT_ROTATED",
      result: "SUCCESS",
      target: `integration_client:${rotated.record.id}`,
      traceId: req.traceId ?? null,
      occurredAt: nowIso,
      metadataRedacted: {
        client_key: rotated.record.clientKey,
        previous_secret_version: rotated.previousSecretVersion,
        secret_version: rotated.record.secretVersion,
        scopes: [...rotated.record.scopes],
        status: rotated.record.status,
      },
    });

    return {
      client: toSafeClientSummary(rotated.record),
      credential: formatIntegrationCredential(
        rotated.record.tenantId,
        rotated.record.clientKey,
        rawSecret
      ),
      previousSecretVersion: rotated.previousSecretVersion,
    };
  }

  async revokeClient(
    req: RevokeClientRequest
  ): Promise<IntegrationClientSafeSummary> {
    const tenantId = assertValidUuid(req.tenantId, "tenantId");
    const clientId = assertValidUuid(req.clientId, "clientId");
    const actor = req.actor.trim();
    if (actor.length === 0) {
      throw new IntegrationCredentialError(
        "VALIDATION_ERROR",
        400,
        "Field 'actor' is required for audit trail."
      );
    }

    const nowIso = this.nowFn().toISOString();
    const record = await this.store.revokeClient({
      id: clientId,
      tenantId,
      nowIso,
    });

    await this.store.appendAudit({
      id: randomUUID(),
      tenantId,
      actor,
      action: "INTEGRATION_CLIENT_REVOKED",
      result: "SUCCESS",
      target: `integration_client:${record.id}`,
      traceId: req.traceId ?? null,
      occurredAt: nowIso,
      metadataRedacted: {
        client_key: record.clientKey,
        secret_version: record.secretVersion,
        status: record.status,
        reason_safe: (req.reason ?? "owner_revoked").slice(0, 120),
      },
    });

    return toSafeClientSummary(record);
  }

  async listClients(
    tenantId: string,
    limit = 50
  ): Promise<readonly IntegrationClientSafeSummary[]> {
    const validTenantId = assertValidUuid(tenantId, "tenantId");
    const rows = await this.store.listByTenant(validTenantId, limit);
    return Object.freeze(rows.map(toSafeClientSummary));
  }

  async authenticateAndAuthorize(
    req: AuthorizeRequest
  ): Promise<AuthorizationDecision> {
    const targetTenantId = assertValidUuid(req.targetTenantId, "targetTenantId");
    const [requiredScope] = normalizeIntegrationScopes([req.requiredScope]);
    if (!requiredScope) {
      return {
        ok: false,
        httpStatus: 400,
        code: "VALIDATION_ERROR",
        reason: "MALFORMED_CREDENTIAL",
        messageSafe: "Required integration scope is missing.",
      };
    }

    const parsed = parseIntegrationCredential(req.credential);
    if (!parsed) {
      return {
        ok: false,
        httpStatus: 401,
        code: "UNAUTHENTICATED",
        reason: "MALFORMED_CREDENTIAL",
        messageSafe: "Missing or malformed integration credential.",
      };
    }

    // Load using credential's bound tenantId first so a forged tenant segment fails consistently.
    const record = await this.store.findByClientKey(parsed.tenantId, parsed.clientKey);
    if (!record) {
      return {
        ok: false,
        httpStatus: 401,
        code: "UNAUTHENTICATED",
        reason: "CLIENT_NOT_FOUND",
        messageSafe: "Unknown integration client credential.",
      };
    }

    if (!verifyIntegrationSecret(parsed.rawSecret, record.secretHash)) {
      await this.recordDeniedAudit(
        record.tenantId,
        record.id,
        record.clientKey,
        "INVALID_SECRET",
        requiredScope,
        req.traceId ?? null
      );
      return {
        ok: false,
        httpStatus: 401,
        code: "UNAUTHENTICATED",
        reason: "INVALID_SECRET",
        messageSafe: "Invalid integration client secret.",
      };
    }

    // Fail closed immediately when revoked: revoked clients can never read, send, or act on any tenant.
    if (record.status === "REVOKED") {
      await this.recordDeniedAudit(
        record.tenantId,
        record.id,
        record.clientKey,
        "CLIENT_REVOKED",
        requiredScope,
        req.traceId ?? null
      );
      return {
        ok: false,
        httpStatus: 401,
        code: "UNAUTHENTICATED",
        reason: "CLIENT_REVOKED",
        messageSafe: "Integration client credentials have been revoked.",
      };
    }

    if (record.tenantId !== targetTenantId) {
      await this.recordDeniedAudit(
        record.tenantId,
        record.id,
        record.clientKey,
        "CROSS_TENANT_DENIED",
        requiredScope,
        req.traceId ?? null
      );
      return {
        ok: false,
        httpStatus: 403,
        code: "FORBIDDEN",
        reason: "CROSS_TENANT_DENIED",
        messageSafe: "Integration client is not authorized for the target tenant.",
      };
    }

    if (!record.scopes.includes(requiredScope)) {
      await this.recordDeniedAudit(
        record.tenantId,
        record.id,
        record.clientKey,
        "INSUFFICIENT_SCOPE",
        requiredScope,
        req.traceId ?? null
      );
      return {
        ok: false,
        httpStatus: 403,
        code: "FORBIDDEN",
        reason: "INSUFFICIENT_SCOPE",
        messageSafe: `Integration client lacks required scope '${requiredScope}'.`,
      };
    }

    return {
      ok: true,
      clientId: record.id,
      tenantId: record.tenantId,
      clientKey: record.clientKey,
      secretVersion: record.secretVersion,
      scopes: record.scopes,
      status: record.status,
    };
  }

  authorizeRead(
    credential: string,
    targetTenantId: string,
    scope: "conversations:read" | "messages:read" | "channels:read" | "media:read" = "conversations:read"
  ): Promise<AuthorizationDecision> {
    return this.authenticateAndAuthorize({
      credential,
      targetTenantId,
      requiredScope: scope,
    });
  }

  authorizeSend(
    credential: string,
    targetTenantId: string
  ): Promise<AuthorizationDecision> {
    return this.authenticateAndAuthorize({
      credential,
      targetTenantId,
      requiredScope: "messages:send",
    });
  }

  private async recordDeniedAudit(
    tenantId: string,
    clientId: string,
    clientKey: string,
    reason: string,
    requiredScope: ChatBridgeIntegrationScope,
    traceId: string | null
  ): Promise<void> {
    await this.store.appendAudit({
      id: randomUUID(),
      tenantId,
      actor: `integration_client:${clientId}`,
      action: "INTEGRATION_CLIENT_AUTH_DENIED",
      result: "DENY",
      target: `integration_client:${clientId}`,
      traceId,
      occurredAt: this.nowFn().toISOString(),
      metadataRedacted: {
        client_key: clientKey,
        denial_reason: reason,
        required_scope: requiredScope,
      },
    });
  }
}
