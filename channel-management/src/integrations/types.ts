export const CHATBRIDGE_INTEGRATION_SCOPES = [
  "conversations:read",
  "conversations:write",
  "messages:read",
  "messages:send",
  "channels:read",
  "channels:manage",
  "media:read",
  "media:write",
  "realtime:subscribe",
  "integrations:admin",
] as const;

export type ChatBridgeIntegrationScope =
  (typeof CHATBRIDGE_INTEGRATION_SCOPES)[number];

export type IntegrationClientStatus = "ACTIVE" | "ROTATING" | "REVOKED";

export type IntegrationErrorCode =
  | "VALIDATION_ERROR"
  | "UNAUTHENTICATED"
  | "FORBIDDEN"
  | "NOT_FOUND"
  | "STATE_CONFLICT"
  | "STALE_VERSION";

export type IntegrationDenialReason =
  | "MALFORMED_CREDENTIAL"
  | "CLIENT_NOT_FOUND"
  | "INVALID_SECRET"
  | "CLIENT_REVOKED"
  | "CROSS_TENANT_DENIED"
  | "INSUFFICIENT_SCOPE";

export interface IntegrationClientRecord {
  readonly id: string;
  readonly tenantId: string;
  readonly clientKey: string;
  readonly secretHash: string;
  readonly secretVersion: number;
  readonly scopes: readonly ChatBridgeIntegrationScope[];
  readonly status: IntegrationClientStatus;
  readonly rotatedAt: string | null;
  readonly createdAt: string;
  readonly updatedAt: string;
}

export interface IntegrationClientSafeSummary {
  readonly id: string;
  readonly tenantId: string;
  readonly clientKey: string;
  readonly secretVersion: number;
  readonly scopes: readonly ChatBridgeIntegrationScope[];
  readonly status: IntegrationClientStatus;
  readonly rotatedAt: string | null;
  readonly createdAt: string;
  readonly updatedAt: string;
}

export interface ProvisionedIntegrationClient {
  readonly client: IntegrationClientSafeSummary;
  readonly credential: string;
}

export interface RotatedIntegrationClient {
  readonly client: IntegrationClientSafeSummary;
  readonly credential: string;
  readonly previousSecretVersion: number;
}

export interface IntegrationClientAuditRecord {
  readonly id: string;
  readonly tenantId: string;
  readonly actor: string;
  readonly action:
    | "INTEGRATION_CLIENT_PROVISIONED"
    | "INTEGRATION_CLIENT_ROTATED"
    | "INTEGRATION_CLIENT_REVOKED"
    | "INTEGRATION_CLIENT_AUTH_DENIED";
  readonly result: "ALLOW" | "DENY" | "SUCCESS" | "FAILURE";
  readonly target: string;
  readonly traceId: string | null;
  readonly occurredAt: string;
  readonly metadataRedacted: Readonly<Record<string, string | number | boolean | readonly string[]>>;
}

export type AuthorizationDecision =
  | {
      readonly ok: true;
      readonly clientId: string;
      readonly tenantId: string;
      readonly clientKey: string;
      readonly secretVersion: number;
      readonly scopes: readonly ChatBridgeIntegrationScope[];
      readonly status: IntegrationClientStatus;
    }
  | {
      readonly ok: false;
      readonly httpStatus: 400 | 401 | 403 | 404;
      readonly code: IntegrationErrorCode;
      readonly reason: IntegrationDenialReason;
      readonly messageSafe: string;
    };

export class IntegrationCredentialError extends Error {
  readonly code: IntegrationErrorCode;
  readonly httpStatus: 400 | 401 | 403 | 404 | 409 | 412;
  readonly currentVersion: number | null;

  constructor(
    code: IntegrationErrorCode,
    httpStatus: 400 | 401 | 403 | 404 | 409 | 412,
    messageSafe: string,
    currentVersion: number | null = null
  ) {
    super(messageSafe);
    this.name = "IntegrationCredentialError";
    this.code = code;
    this.httpStatus = httpStatus;
    this.currentVersion = currentVersion;
  }
}

const VALID_SCOPE_SET = new Set<string>(CHATBRIDGE_INTEGRATION_SCOPES);

const UUID_REGEX =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const CLIENT_KEY_REGEX = /^[a-z0-9][a-z0-9_-]{2,63}$/;

export function assertValidUuid(value: string, fieldName: string): string {
  const normalized = value.trim().toLowerCase();
  if (!UUID_REGEX.test(normalized)) {
    throw new IntegrationCredentialError(
      "VALIDATION_ERROR",
      400,
      `Field '${fieldName}' must be a valid UUID.`
    );
  }
  return normalized;
}

export function assertValidClientKey(value: string): string {
  const normalized = value.trim();
  if (!CLIENT_KEY_REGEX.test(normalized)) {
    throw new IntegrationCredentialError(
      "VALIDATION_ERROR",
      400,
      "Field 'clientKey' must match ^[a-z0-9][a-z0-9_-]{2,63}$."
    );
  }
  return normalized;
}

export function normalizeIntegrationScopes(
  scopes: readonly string[]
): readonly ChatBridgeIntegrationScope[] {
  if (!Array.isArray(scopes) || scopes.length === 0) {
    throw new IntegrationCredentialError(
      "VALIDATION_ERROR",
      400,
      "Field 'scopes' must contain at least one valid ChatBridge integration scope."
    );
  }

  const deduped = new Set<ChatBridgeIntegrationScope>();
  for (const raw of scopes) {
    if (typeof raw !== "string") {
      throw new IntegrationCredentialError(
        "VALIDATION_ERROR",
        400,
        "Every integration scope must be a string."
      );
    }
    const candidate = raw.trim();
    if (!VALID_SCOPE_SET.has(candidate)) {
      throw new IntegrationCredentialError(
        "VALIDATION_ERROR",
        400,
        `Unsupported ChatBridge integration scope '${candidate}'.`
      );
    }
    deduped.add(candidate as ChatBridgeIntegrationScope);
  }

  return Object.freeze([...deduped].sort());
}

export function toSafeClientSummary(
  record: IntegrationClientRecord
): IntegrationClientSafeSummary {
  return Object.freeze({
    id: record.id,
    tenantId: record.tenantId,
    clientKey: record.clientKey,
    secretVersion: record.secretVersion,
    scopes: Object.freeze([...record.scopes]),
    status: record.status,
    rotatedAt: record.rotatedAt,
    createdAt: record.createdAt,
    updatedAt: record.updatedAt,
  });
}
