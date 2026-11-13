export type OutboxSourceKind =
  | "LIVE_WEBHOOK"
  | "LIVE_API"
  | "INCREMENTAL_SYNC"
  | "INITIAL_BACKFILL"
  | "CSV_IMPORT";

export type OutboxState =
  | "PENDING"
  | "CLAIMED"
  | "SUCCEEDED"
  | "RETRY_WAIT"
  | "QUARANTINED";

export type InboxDeliveryOutcome =
  | "APPLIED"
  | "SUPPRESSED_HISTORY"
  | "IGNORED_STALE";

export type OutboxErrorCode =
  | "VALIDATION_ERROR"
  | "STATE_CONFLICT"
  | "DEPENDENCY_UNAVAILABLE";

export class OutboxError extends Error {
  readonly code: OutboxErrorCode;
  readonly status: 400 | 409 | 503;

  constructor(code: OutboxErrorCode, message: string, status?: 400 | 409 | 503) {
    super(message);
    this.name = "OutboxError";
    this.code = code;
    this.status =
      status ??
      (code === "VALIDATION_ERROR"
        ? 400
        : code === "STATE_CONFLICT"
          ? 409
          : 503);
  }
}

export interface OutboxScope {
  readonly tenant_id: string;
  readonly channel_account_id?: string | null;
}

export interface StreamCursor {
  readonly partition_key: string;
  readonly outbox_seq: number;
  readonly cursor_token: string;
}

export interface ReplayMetadata {
  readonly is_replay: boolean;
  readonly replay_id: string | null;
  readonly attempt: number;
  readonly original_occurred_at?: string | null;
}

export interface HistoryMetadata {
  readonly historical_import: boolean;
  readonly suppress_side_effects: boolean;
  readonly source_kind: OutboxSourceKind;
}

export interface OutboxEventEnvelope {
  readonly event_id: string;
  readonly schema_version: number;
  readonly contract_revision: string;
  readonly producer: string;
  readonly owner_db: string;
  readonly type: string;
  readonly scope: OutboxScope;
  readonly aggregate_type: string;
  readonly aggregate_ref: string;
  readonly aggregate_seq: number;
  readonly stream_cursor: StreamCursor;
  readonly replay: ReplayMetadata;
  readonly history: HistoryMetadata;
  readonly occurred_at: string;
  readonly correlation_id: string;
  readonly payload: Readonly<Record<string, unknown>>;
}

export interface StoredOutboxRecord {
  readonly envelope: OutboxEventEnvelope;
  readonly state: OutboxState;
  readonly available_at: string;
  readonly locked_until: string | null;
  readonly attempts: number;
  readonly claimed_by: string | null;
  readonly published_at: string | null;
  readonly last_error_safe: string | null;
  readonly created_at: string;
}

export interface EnqueueOutboxInput {
  readonly eventId: string;
  readonly schemaVersion?: number;
  readonly contractRevision?: string;
  readonly producer?: string;
  readonly ownerDb?: string;
  readonly tenantId: string;
  readonly channelAccountId?: string | null;
  readonly type: string;
  readonly aggregateType: string;
  readonly aggregateKey: string;
  readonly aggregateVersion: number;
  readonly payload: Readonly<Record<string, unknown>>;
  readonly historicalImport?: boolean;
  readonly suppressSideEffects?: boolean;
  readonly sourceKind?: OutboxSourceKind;
  readonly correlationId: string;
  readonly occurredAt?: string;
  readonly availableAt?: string;
  readonly isReplay?: boolean;
  readonly replayId?: string | null;
  readonly originalOccurredAt?: string | null;
}

export interface ConversationMessageMutation {
  readonly kind: "CONVERSATION_MESSAGE";
  readonly conversationId: string;
  readonly channelAccountId: string;
  readonly identityId: string;
  readonly messageId: string;
  readonly messageSeq: number;
  readonly direction: "IN" | "OUT";
  readonly senderKind: "CUSTOMER" | "WEB_GUEST" | "STAFF" | "ADMIN" | "OWNER" | "AI" | "SYSTEM";
  readonly messageKind?: string;
  readonly body: string | null;
  readonly receivedAt?: string;
}

export interface AuditLogMutation {
  readonly kind: "AUDIT_LOG";
  readonly auditId: string;
  readonly actor: string;
  readonly action: string;
  readonly result: "ALLOW" | "DENY" | "SUCCESS" | "FAILURE";
  readonly target: string;
  readonly metadataRedacted?: Readonly<Record<string, unknown>>;
}

export type LocalOwningMutation = ConversationMessageMutation | AuditLogMutation;

export interface CommitWithOutboxResult {
  readonly record: StoredOutboxRecord;
  readonly deduplicated: boolean;
}

export interface InboxReceiptRecord {
  readonly tenantId: string;
  readonly consumer: string;
  readonly externalEventId: string;
  readonly producer: string;
  readonly eventType: string;
  readonly schemaVersion: number;
  readonly contractRevision: string;
  readonly aggregateType: string;
  readonly aggregateKey: string;
  readonly aggregateSeq: number;
  readonly historicalImport: boolean;
  readonly suppressSideEffects: boolean;
  readonly sourceKind: OutboxSourceKind;
  readonly correlationId: string | null;
  readonly deliveryOutcome: InboxDeliveryOutcome;
  readonly effectCount: number;
  readonly processedAt: string;
}

export interface ConsumeInboxResult {
  readonly status: "APPLIED" | "DUPLICATE_NOOP" | "SUPPRESSED_HISTORY" | "IGNORED_STALE";
  readonly effectExecuted: boolean;
  readonly receipt: InboxReceiptRecord;
}

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const SEMVER_RE = /^[0-9]+\.[0-9]+\.[0-9]+$/;
const EVENT_TYPE_RE = /^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$/;
const AGGREGATE_TYPE_RE = /^[a-z][a-z0-9_]*$/;
const FORBIDDEN_PAYLOAD_KEY_RE =
  /(otp|secret|password|token|presigned_?url|signed_?url|raw_?body|raw_?message|bank_?account|cookie|authorization)/i;

const VALID_SOURCE_KINDS: ReadonlySet<string> = new Set([
  "LIVE_WEBHOOK",
  "LIVE_API",
  "INCREMENTAL_SYNC",
  "INITIAL_BACKFILL",
  "CSV_IMPORT",
]);

export function assertUuid(value: string, field: string): void {
  if (!UUID_RE.test(value)) {
    throw new OutboxError("VALIDATION_ERROR", `Field ${field} must be a valid UUID`);
  }
}

export function validatePayloadSafety(
  value: unknown,
  pathPrefix = "payload"
): void {
  if (value === null || value === undefined || typeof value !== "object") {
    return;
  }
  if (Array.isArray(value)) {
    for (let i = 0; i < value.length; i++) {
      validatePayloadSafety(value[i], `${pathPrefix}[${i}]`);
    }
    return;
  }

  for (const [key, entry] of Object.entries(value as Record<string, unknown>)) {
    if (FORBIDDEN_PAYLOAD_KEY_RE.test(key)) {
      throw new OutboxError(
        "VALIDATION_ERROR",
        `Forbidden sensitive key '${key}' at ${pathPrefix}.${key}`
      );
    }
    if (key === "storage_object_key") {
      if (
        typeof entry !== "string" ||
        entry.length === 0 ||
        entry.includes("?") ||
        /^https?:\/\//i.test(entry)
      ) {
        throw new OutboxError(
          "VALIDATION_ERROR",
          "storage_object_key must be an opaque non-URL key without query parameters"
        );
      }
    }
    validatePayloadSafety(entry, `${pathPrefix}.${key}`);
  }
}

export interface NormalizedOutboxInput {
  readonly eventId: string;
  readonly schemaVersion: number;
  readonly contractRevision: string;
  readonly producer: string;
  readonly ownerDb: string;
  readonly tenantId: string;
  readonly channelAccountId: string | null;
  readonly type: string;
  readonly aggregateType: string;
  readonly aggregateKey: string;
  readonly aggregateVersion: number;
  readonly payload: Readonly<Record<string, unknown>>;
  readonly historicalImport: boolean;
  readonly suppressSideEffects: boolean;
  readonly sourceKind: OutboxSourceKind;
  readonly correlationId: string;
  readonly occurredAt: string;
  readonly availableAt: string;
  readonly isReplay: boolean;
  readonly replayId: string | null;
  readonly originalOccurredAt: string | null;
}

export function normalizeAndValidateOutboxInput(
  input: EnqueueOutboxInput
): NormalizedOutboxInput {
  assertUuid(input.eventId, "eventId");
  assertUuid(input.tenantId, "tenantId");

  const channelAccountId = input.channelAccountId ?? null;
  if (channelAccountId !== null) {
    assertUuid(channelAccountId, "channelAccountId");
  }

  const schemaVersion = input.schemaVersion ?? 1;
  if (!Number.isInteger(schemaVersion) || schemaVersion < 1) {
    throw new OutboxError("VALIDATION_ERROR", "schemaVersion must be an integer >= 1");
  }

  const contractRevision = input.contractRevision ?? "1.0.0";
  if (!SEMVER_RE.test(contractRevision)) {
    throw new OutboxError("VALIDATION_ERROR", "contractRevision must match X.Y.Z semver");
  }

  const producer = input.producer ?? "chatbridge";
  const ownerDb = input.ownerDb ?? "chatbridge_db";
  if (producer === "chatbridge" && ownerDb !== "chatbridge_db") {
    throw new OutboxError(
      "VALIDATION_ERROR",
      "producer 'chatbridge' must own ownerDb 'chatbridge_db'"
    );
  }

  if (!EVENT_TYPE_RE.test(input.type)) {
    throw new OutboxError("VALIDATION_ERROR", "type must be a dotted domain event name");
  }
  if (!AGGREGATE_TYPE_RE.test(input.aggregateType)) {
    throw new OutboxError("VALIDATION_ERROR", "aggregateType must be snake_case");
  }
  if (input.aggregateKey.trim().length === 0) {
    throw new OutboxError("VALIDATION_ERROR", "aggregateKey must be non-empty");
  }
  if (!Number.isInteger(input.aggregateVersion) || input.aggregateVersion < 1) {
    throw new OutboxError("VALIDATION_ERROR", "aggregateVersion must be an integer >= 1");
  }
  if (input.correlationId.trim().length < 8) {
    throw new OutboxError("VALIDATION_ERROR", "correlationId must be at least 8 characters");
  }

  const historicalImport = Boolean(input.historicalImport ?? false);
  const suppressSideEffects = Boolean(input.suppressSideEffects ?? false);
  if (historicalImport && !suppressSideEffects) {
    throw new OutboxError(
      "VALIDATION_ERROR",
      "historicalImport=true requires suppressSideEffects=true"
    );
  }

  const sourceKind: OutboxSourceKind =
    input.sourceKind ?? (historicalImport ? "INITIAL_BACKFILL" : "LIVE_WEBHOOK");
  if (!VALID_SOURCE_KINDS.has(sourceKind)) {
    throw new OutboxError("VALIDATION_ERROR", `Invalid sourceKind '${sourceKind}'`);
  }

  const isReplay = Boolean(input.isReplay ?? false);
  const replayId = input.replayId ?? null;
  const nowIso = new Date().toISOString();
  const occurredAt = input.occurredAt ?? nowIso;
  const availableAt = input.availableAt ?? occurredAt;
  const originalOccurredAt = input.originalOccurredAt ?? null;

  if (isReplay) {
    if (replayId === null) {
      throw new OutboxError("VALIDATION_ERROR", "isReplay=true requires replayId");
    }
    assertUuid(replayId, "replayId");
    if (!originalOccurredAt) {
      throw new OutboxError(
        "VALIDATION_ERROR",
        "isReplay=true requires originalOccurredAt"
      );
    }
  }

  validatePayloadSafety(input.payload);

  return Object.freeze({
    eventId: input.eventId,
    schemaVersion,
    contractRevision,
    producer,
    ownerDb,
    tenantId: input.tenantId,
    channelAccountId,
    type: input.type,
    aggregateType: input.aggregateType,
    aggregateKey: input.aggregateKey,
    aggregateVersion: input.aggregateVersion,
    payload: Object.freeze({ ...input.payload }),
    historicalImport,
    suppressSideEffects,
    sourceKind,
    correlationId: input.correlationId,
    occurredAt,
    availableAt,
    isReplay,
    replayId,
    originalOccurredAt,
  });
}

export function validateOutboxEnvelope(envelope: OutboxEventEnvelope): void {
  assertUuid(envelope.event_id, "event_id");
  assertUuid(envelope.scope.tenant_id, "scope.tenant_id");
  if (
    envelope.scope.channel_account_id !== undefined &&
    envelope.scope.channel_account_id !== null
  ) {
    assertUuid(envelope.scope.channel_account_id, "scope.channel_account_id");
  }
  if (!Number.isInteger(envelope.schema_version) || envelope.schema_version < 1) {
    throw new OutboxError("VALIDATION_ERROR", "schema_version must be integer >= 1");
  }
  if (!SEMVER_RE.test(envelope.contract_revision)) {
    throw new OutboxError("VALIDATION_ERROR", "Invalid contract_revision");
  }
  if (envelope.producer === "chatbridge" && envelope.owner_db !== "chatbridge_db") {
    throw new OutboxError(
      "VALIDATION_ERROR",
      "Cross-database owner mismatch for chatbridge producer"
    );
  }
  if (!EVENT_TYPE_RE.test(envelope.type)) {
    throw new OutboxError("VALIDATION_ERROR", "Invalid dotted event type");
  }
  if (!Number.isInteger(envelope.aggregate_seq) || envelope.aggregate_seq < 1) {
    throw new OutboxError("VALIDATION_ERROR", "aggregate_seq must be integer >= 1");
  }
  if (
    !Number.isInteger(envelope.stream_cursor.outbox_seq) ||
    envelope.stream_cursor.outbox_seq < 1 ||
    envelope.stream_cursor.partition_key.length === 0 ||
    !/^[A-Za-z0-9_=-]{8,}$/.test(envelope.stream_cursor.cursor_token)
  ) {
    throw new OutboxError("VALIDATION_ERROR", "Invalid stream_cursor");
  }
  if (envelope.replay.attempt < 1) {
    throw new OutboxError("VALIDATION_ERROR", "replay.attempt must be >= 1");
  }
  if (envelope.replay.is_replay) {
    if (!envelope.replay.replay_id) {
      throw new OutboxError("VALIDATION_ERROR", "Replayed event requires replay_id");
    }
    assertUuid(envelope.replay.replay_id, "replay.replay_id");
    if (!envelope.replay.original_occurred_at) {
      throw new OutboxError(
        "VALIDATION_ERROR",
        "Replayed event requires original_occurred_at"
      );
    }
  }
  if (!VALID_SOURCE_KINDS.has(envelope.history.source_kind)) {
    throw new OutboxError("VALIDATION_ERROR", "Invalid history.source_kind");
  }
  if (envelope.history.historical_import && !envelope.history.suppress_side_effects) {
    throw new OutboxError(
      "VALIDATION_ERROR",
      "Historical event must set suppress_side_effects=true"
    );
  }
  if (envelope.correlation_id.trim().length < 8) {
    throw new OutboxError("VALIDATION_ERROR", "correlation_id must be >= 8 characters");
  }
  validatePayloadSafety(envelope.payload);
}

export function encodeCursorToken(outboxSeq: number): string {
  return Buffer.from(`cb_seq:${outboxSeq}`, "utf8").toString("base64");
}

export function sanitizeSafeErrorMessage(error: unknown): string {
  const raw = error instanceof Error ? error.message : String(error);
  return raw
    .replace(/(bearer|basic)\s+\S+/gi, "$1 [REDACTED]")
    .replace(
      /(authorization|token|secret|password|cookie|x-api-key|signature)\s*[:=]\s*([^\s,;]+)/gi,
      "$1=[REDACTED]"
    )
    .slice(0, 240);
}
