import {
  buildPgEnv,
  runPsqlStdin,
  sqlJsonbB64,
  sqlNullableTextB64,
  sqlTextB64,
  sqlTimestamptzLiteral,
  sqlUuidLiteral,
} from "../../chat-core/src/receipt-store.js";
import {
  type CommitWithOutboxResult,
  type ConsumeInboxResult,
  encodeCursorToken,
  type EnqueueOutboxInput,
  type InboxDeliveryOutcome,
  type InboxReceiptRecord,
  type LocalOwningMutation,
  normalizeAndValidateOutboxInput,
  OutboxError,
  type OutboxEventEnvelope,
  type OutboxSourceKind,
  type OutboxState,
  type ReplayMetadata,
  sanitizeSafeErrorMessage,
  type StoredOutboxRecord,
  validateOutboxEnvelope,
} from "./outbox-types.js";

export interface ClaimBatchOptions {
  readonly workerId: string;
  readonly limit: number;
  readonly leaseMs: number;
  readonly nowIso?: string;
  readonly tenantId?: string;
}

export interface SqlAuditEffect {
  readonly auditId: string;
  readonly actor: string;
  readonly action: string;
  readonly target: string;
}

export interface ConsumeEventOptions {
  readonly consumer: string;
  readonly envelope: OutboxEventEnvelope;
  readonly nowIso?: string;
  readonly sqlAuditEffect?: SqlAuditEffect;
  readonly onBeforeCommitEffect?: () => Promise<void> | void;
  readonly simulateCrashBeforeCommit?: boolean;
}

export interface OutboxInboxStore {
  commitWithOutbox(
    input: EnqueueOutboxInput,
    localMutation?: LocalOwningMutation,
    options?: { readonly simulateCrashBeforeCommit?: boolean }
  ): Promise<CommitWithOutboxResult>;
  claimDueBatch(options: ClaimBatchOptions): Promise<readonly StoredOutboxRecord[]>;
  markSucceeded(
    tenantId: string,
    eventId: string,
    publishedAtIso?: string
  ): Promise<StoredOutboxRecord>;
  scheduleRetry(
    tenantId: string,
    eventId: string,
    safeError: string,
    nextAvailableAtIso: string,
    maxAttempts: number
  ): Promise<StoredOutboxRecord>;
  prepareReplay(
    tenantId: string,
    eventId: string,
    replayId: string,
    nowIso?: string
  ): Promise<StoredOutboxRecord>;
  listStreamSince(
    tenantId: string,
    afterOutboxSeq: number,
    limit: number
  ): Promise<readonly StoredOutboxRecord[]>;
  getOutboxRecord(
    tenantId: string,
    eventId: string
  ): Promise<StoredOutboxRecord | null>;
  consumeEvent(options: ConsumeEventOptions): Promise<ConsumeInboxResult>;
  getInboxReceipt(
    tenantId: string,
    consumer: string,
    externalEventId: string
  ): Promise<InboxReceiptRecord | null>;
}

interface RawOutboxRow {
  readonly id: string;
  readonly tenant_id: string;
  readonly channel_account_id: string | null;
  readonly event_schema_version: number | string;
  readonly contract_revision: string;
  readonly producer: string;
  readonly owner_db: string;
  readonly type: string;
  readonly aggregate_type: string;
  readonly aggregate_key: string;
  readonly aggregate_version: number | string;
  readonly outbox_seq: number | string;
  readonly cursor_token: string;
  readonly is_replay: boolean;
  readonly replay_id: string | null;
  readonly original_occurred_at: string | null;
  readonly attempts: number | string;
  readonly historical_import: boolean;
  readonly suppress_side_effects: boolean;
  readonly source_kind: OutboxSourceKind;
  readonly occurred_at: string;
  readonly correlation_id: string;
  readonly payload_minimal: Record<string, unknown>;
  readonly state: OutboxState;
  readonly available_at: string;
  readonly locked_until: string | null;
  readonly claimed_by: string | null;
  readonly published_at: string | null;
  readonly last_error_safe: string | null;
  readonly created_at: string;
  readonly deduplicated?: boolean;
}

interface RawInboxRow {
  readonly tenant_id: string;
  readonly consumer: string;
  readonly external_event_id: string;
  readonly producer: string;
  readonly event_type: string;
  readonly event_schema_version: number | string;
  readonly contract_revision: string;
  readonly aggregate_type: string;
  readonly aggregate_key: string;
  readonly aggregate_seq: number | string;
  readonly historical_import: boolean;
  readonly suppress_side_effects: boolean;
  readonly source_kind: OutboxSourceKind;
  readonly correlation_id: string | null;
  readonly delivery_outcome: InboxDeliveryOutcome;
  readonly effect_count: number | string;
  readonly processed_at: string;
  readonly status: "APPLIED" | "DUPLICATE_NOOP" | "SUPPRESSED_HISTORY" | "IGNORED_STALE";
}

function mapRawOutboxRow(row: RawOutboxRow): StoredOutboxRecord {
  const outboxSeq = Number(row.outbox_seq);
  const attempts = Math.max(1, Number(row.attempts) === 0 ? 1 : Number(row.attempts));
  const replay: ReplayMetadata = row.is_replay
    ? Object.freeze({
        is_replay: true,
        replay_id: row.replay_id,
        attempt: attempts,
        original_occurred_at: row.original_occurred_at,
      })
    : Object.freeze({
        is_replay: false,
        replay_id: null,
        attempt: attempts,
      });

  const envelope: OutboxEventEnvelope = Object.freeze({
    event_id: row.id,
    schema_version: Number(row.event_schema_version),
    contract_revision: row.contract_revision,
    producer: row.producer,
    owner_db: row.owner_db,
    type: row.type,
    scope:
      row.channel_account_id !== null
        ? Object.freeze({
            tenant_id: row.tenant_id,
            channel_account_id: row.channel_account_id,
          })
        : Object.freeze({
            tenant_id: row.tenant_id,
          }),
    aggregate_type: row.aggregate_type,
    aggregate_ref: row.aggregate_key,
    aggregate_seq: Number(row.aggregate_version),
    stream_cursor: Object.freeze({
      partition_key: row.tenant_id,
      outbox_seq: outboxSeq,
      cursor_token: row.cursor_token,
    }),
    replay,
    history: Object.freeze({
      historical_import: Boolean(row.historical_import),
      suppress_side_effects: Boolean(row.suppress_side_effects),
      source_kind: row.source_kind,
    }),
    occurred_at: row.occurred_at,
    correlation_id: row.correlation_id,
    payload: Object.freeze({ ...row.payload_minimal }),
  });

  return Object.freeze({
    envelope,
    state: row.state,
    available_at: row.available_at,
    locked_until: row.locked_until,
    attempts: Number(row.attempts),
    claimed_by: row.claimed_by,
    published_at: row.published_at,
    last_error_safe: row.last_error_safe,
    created_at: row.created_at,
  });
}

function mapRawInboxReceipt(row: Omit<RawInboxRow, "status">): InboxReceiptRecord {
  return Object.freeze({
    tenantId: row.tenant_id,
    consumer: row.consumer,
    externalEventId: row.external_event_id,
    producer: row.producer,
    eventType: row.event_type,
    schemaVersion: Number(row.event_schema_version),
    contractRevision: row.contract_revision,
    aggregateType: row.aggregate_type,
    aggregateKey: row.aggregate_key,
    aggregateSeq: Number(row.aggregate_seq),
    historicalImport: Boolean(row.historical_import),
    suppressSideEffects: Boolean(row.suppress_side_effects),
    sourceKind: row.source_kind,
    correlationId: row.correlation_id,
    deliveryOutcome: row.delivery_outcome,
    effectCount: Number(row.effect_count),
    processedAt: row.processed_at,
  });
}

export class InMemoryOutboxInboxStore implements OutboxInboxStore {
  private nextOutboxSeq = 1;
  private readonly outbox: StoredOutboxRecord[] = [];
  private readonly inbox: InboxReceiptRecord[] = [];
  private readonly aggregateCursors = new Map<string, number>();
  private readonly committedMessages = new Map<string, { conversationId: string; seq: number }>();
  private readonly committedAudits = new Map<string, SqlAuditEffect>();

  private cloneStored(record: StoredOutboxRecord): StoredOutboxRecord {
    return Object.freeze({
      ...record,
      envelope: Object.freeze({
        ...record.envelope,
        scope: Object.freeze({ ...record.envelope.scope }),
        stream_cursor: Object.freeze({ ...record.envelope.stream_cursor }),
        replay: Object.freeze({ ...record.envelope.replay }),
        history: Object.freeze({ ...record.envelope.history }),
        payload: Object.freeze({ ...record.envelope.payload }),
      }),
    });
  }

  async commitWithOutbox(
    input: EnqueueOutboxInput,
    localMutation?: LocalOwningMutation,
    options?: { readonly simulateCrashBeforeCommit?: boolean }
  ): Promise<CommitWithOutboxResult> {
    const norm = normalizeAndValidateOutboxInput(input);
    const existing = this.outbox.find(
      (r) =>
        r.envelope.scope.tenant_id === norm.tenantId &&
        r.envelope.aggregate_ref === norm.aggregateKey &&
        r.envelope.aggregate_seq === norm.aggregateVersion &&
        r.envelope.type === norm.type
    );
    if (existing) {
      return { record: this.cloneStored(existing), deduplicated: true };
    }

    if (options?.simulateCrashBeforeCommit) {
      throw new OutboxError(
        "DEPENDENCY_UNAVAILABLE",
        "Simulated crash before local commit"
      );
    }

    if (localMutation?.kind === "CONVERSATION_MESSAGE") {
      this.committedMessages.set(localMutation.messageId, {
        conversationId: localMutation.conversationId,
        seq: localMutation.messageSeq,
      });
    } else if (localMutation?.kind === "AUDIT_LOG") {
      this.committedAudits.set(localMutation.auditId, {
        auditId: localMutation.auditId,
        actor: localMutation.actor,
        action: localMutation.action,
        target: localMutation.target,
      });
    }

    const seq = this.nextOutboxSeq++;
    const envelope: OutboxEventEnvelope = Object.freeze({
      event_id: norm.eventId,
      schema_version: norm.schemaVersion,
      contract_revision: norm.contractRevision,
      producer: norm.producer,
      owner_db: norm.ownerDb,
      type: norm.type,
      scope:
        norm.channelAccountId !== null
          ? { tenant_id: norm.tenantId, channel_account_id: norm.channelAccountId }
          : { tenant_id: norm.tenantId },
      aggregate_type: norm.aggregateType,
      aggregate_ref: norm.aggregateKey,
      aggregate_seq: norm.aggregateVersion,
      stream_cursor: {
        partition_key: norm.tenantId,
        outbox_seq: seq,
        cursor_token: encodeCursorToken(seq),
      },
      replay: norm.isReplay
        ? {
            is_replay: true,
            replay_id: norm.replayId,
            attempt: 1,
            original_occurred_at: norm.originalOccurredAt,
          }
        : {
            is_replay: false,
            replay_id: null,
            attempt: 1,
          },
      history: {
        historical_import: norm.historicalImport,
        suppress_side_effects: norm.suppressSideEffects,
        source_kind: norm.sourceKind,
      },
      occurred_at: norm.occurredAt,
      correlation_id: norm.correlationId,
      payload: norm.payload,
    });

    validateOutboxEnvelope(envelope);

    const stored: StoredOutboxRecord = {
      envelope,
      state: "PENDING",
      available_at: norm.availableAt,
      locked_until: null,
      attempts: 0,
      claimed_by: null,
      published_at: null,
      last_error_safe: null,
      created_at: norm.occurredAt,
    };
    this.outbox.push(stored);
    return { record: this.cloneStored(stored), deduplicated: false };
  }

  async claimDueBatch(
    options: ClaimBatchOptions
  ): Promise<readonly StoredOutboxRecord[]> {
    const nowMs = Date.parse(options.nowIso ?? new Date().toISOString());
    const leaseUntilIso = new Date(nowMs + options.leaseMs).toISOString();
    const claimed: StoredOutboxRecord[] = [];

    const ordered = [...this.outbox].sort(
      (a, b) =>
        a.envelope.stream_cursor.outbox_seq - b.envelope.stream_cursor.outbox_seq
    );

    for (const candidate of ordered) {
      if (claimed.length >= options.limit) {
        break;
      }
      if (
        options.tenantId &&
        candidate.envelope.scope.tenant_id !== options.tenantId
      ) {
        continue;
      }
      const isDuePendingOrRetry =
        (candidate.state === "PENDING" || candidate.state === "RETRY_WAIT") &&
        Date.parse(candidate.available_at) <= nowMs;
      const isExpiredLease =
        candidate.state === "CLAIMED" &&
        candidate.locked_until !== null &&
        Date.parse(candidate.locked_until) <= nowMs;

      if (!isDuePendingOrRetry && !isExpiredLease) {
        continue;
      }

      const idx = this.outbox.indexOf(candidate);
      const nextAttempts = candidate.attempts + 1;
      const updated: StoredOutboxRecord = {
        ...candidate,
        envelope: {
          ...candidate.envelope,
          replay: candidate.envelope.replay.is_replay
            ? {
                ...candidate.envelope.replay,
                attempt: nextAttempts,
              }
            : {
                is_replay: false,
                replay_id: null,
                attempt: nextAttempts,
              },
        },
        state: "CLAIMED",
        attempts: nextAttempts,
        claimed_by: options.workerId,
        locked_until: leaseUntilIso,
      };
      this.outbox[idx] = updated;
      claimed.push(this.cloneStored(updated));
    }

    return Object.freeze(claimed);
  }

  async markSucceeded(
    tenantId: string,
    eventId: string,
    publishedAtIso = new Date().toISOString()
  ): Promise<StoredOutboxRecord> {
    const idx = this.outbox.findIndex(
      (r) => r.envelope.scope.tenant_id === tenantId && r.envelope.event_id === eventId
    );
    if (idx < 0) {
      throw new OutboxError("STATE_CONFLICT", "Outbox event not found", 409);
    }
    const current = this.outbox[idx]!;
    const updated: StoredOutboxRecord = {
      ...current,
      state: "SUCCEEDED",
      locked_until: null,
      published_at: publishedAtIso,
      last_error_safe: null,
    };
    this.outbox[idx] = updated;
    return this.cloneStored(updated);
  }

  async scheduleRetry(
    tenantId: string,
    eventId: string,
    safeError: string,
    nextAvailableAtIso: string,
    maxAttempts: number
  ): Promise<StoredOutboxRecord> {
    const idx = this.outbox.findIndex(
      (r) => r.envelope.scope.tenant_id === tenantId && r.envelope.event_id === eventId
    );
    if (idx < 0) {
      throw new OutboxError("STATE_CONFLICT", "Outbox event not found", 409);
    }
    const current = this.outbox[idx]!;
    const nextState: OutboxState =
      current.attempts >= maxAttempts ? "QUARANTINED" : "RETRY_WAIT";
    const updated: StoredOutboxRecord = {
      ...current,
      state: nextState,
      available_at: nextAvailableAtIso,
      locked_until: null,
      last_error_safe: sanitizeSafeErrorMessage(safeError),
    };
    this.outbox[idx] = updated;
    return this.cloneStored(updated);
  }

  async prepareReplay(
    tenantId: string,
    eventId: string,
    replayId: string,
    nowIso = new Date().toISOString()
  ): Promise<StoredOutboxRecord> {
    const idx = this.outbox.findIndex(
      (r) => r.envelope.scope.tenant_id === tenantId && r.envelope.event_id === eventId
    );
    if (idx < 0) {
      throw new OutboxError("STATE_CONFLICT", "Outbox event not found", 409);
    }
    const current = this.outbox[idx]!;
    const nextAttempt = current.attempts + 1;
    const updated: StoredOutboxRecord = {
      ...current,
      envelope: {
        ...current.envelope,
        replay: {
          is_replay: true,
          replay_id: replayId,
          attempt: nextAttempt,
          original_occurred_at: current.envelope.occurred_at,
        },
      },
      state: "PENDING",
      available_at: nowIso,
      locked_until: null,
    };
    this.outbox[idx] = updated;
    return this.cloneStored(updated);
  }

  async listStreamSince(
    tenantId: string,
    afterOutboxSeq: number,
    limit: number
  ): Promise<readonly StoredOutboxRecord[]> {
    return Object.freeze(
      this.outbox
        .filter(
          (r) =>
            r.envelope.scope.tenant_id === tenantId &&
            r.envelope.stream_cursor.outbox_seq > afterOutboxSeq
        )
        .sort(
          (a, b) =>
            a.envelope.stream_cursor.outbox_seq - b.envelope.stream_cursor.outbox_seq
        )
        .slice(0, limit)
        .map((r) => this.cloneStored(r))
    );
  }

  async getOutboxRecord(
    tenantId: string,
    eventId: string
  ): Promise<StoredOutboxRecord | null> {
    const found = this.outbox.find(
      (r) => r.envelope.scope.tenant_id === tenantId && r.envelope.event_id === eventId
    );
    return found ? this.cloneStored(found) : null;
  }

  async consumeEvent(options: ConsumeEventOptions): Promise<ConsumeInboxResult> {
    validateOutboxEnvelope(options.envelope);
    const tenantId = options.envelope.scope.tenant_id;
    const nowIso = options.nowIso ?? new Date().toISOString();

    const existing = this.inbox.find(
      (r) =>
        r.tenantId === tenantId &&
        r.consumer === options.consumer &&
        r.externalEventId === options.envelope.event_id
    );
    if (existing) {
      return {
        status: "DUPLICATE_NOOP",
        effectExecuted: false,
        receipt: Object.freeze({ ...existing }),
      };
    }

    const cursorKey = `${tenantId}:${options.consumer}:${options.envelope.aggregate_type}:${options.envelope.aggregate_ref}`;
    const lastSeq = this.aggregateCursors.get(cursorKey) ?? 0;

    let deliveryOutcome: InboxDeliveryOutcome = "APPLIED";
    let status: ConsumeInboxResult["status"] = "APPLIED";
    let effectExecuted = false;

    if (options.envelope.aggregate_seq <= lastSeq) {
      deliveryOutcome = "IGNORED_STALE";
      status = "IGNORED_STALE";
    } else if (
      options.envelope.history.historical_import ||
      options.envelope.history.suppress_side_effects
    ) {
      deliveryOutcome = "SUPPRESSED_HISTORY";
      status = "SUPPRESSED_HISTORY";
    } else {
      if (options.onBeforeCommitEffect) {
        await options.onBeforeCommitEffect();
      }
      if (options.simulateCrashBeforeCommit) {
        throw new OutboxError(
          "DEPENDENCY_UNAVAILABLE",
          "Simulated consumer crash before commit"
        );
      }
      effectExecuted = true;
    }

    if (options.simulateCrashBeforeCommit && !effectExecuted) {
      throw new OutboxError(
        "DEPENDENCY_UNAVAILABLE",
        "Simulated consumer crash before commit"
      );
    }

    if (deliveryOutcome !== "IGNORED_STALE") {
      this.aggregateCursors.set(cursorKey, options.envelope.aggregate_seq);
    }
    if (effectExecuted && options.sqlAuditEffect) {
      this.committedAudits.set(options.sqlAuditEffect.auditId, options.sqlAuditEffect);
    }

    const receipt: InboxReceiptRecord = Object.freeze({
      tenantId,
      consumer: options.consumer,
      externalEventId: options.envelope.event_id,
      producer: options.envelope.producer,
      eventType: options.envelope.type,
      schemaVersion: options.envelope.schema_version,
      contractRevision: options.envelope.contract_revision,
      aggregateType: options.envelope.aggregate_type,
      aggregateKey: options.envelope.aggregate_ref,
      aggregateSeq: options.envelope.aggregate_seq,
      historicalImport: options.envelope.history.historical_import,
      suppressSideEffects: options.envelope.history.suppress_side_effects,
      sourceKind: options.envelope.history.source_kind,
      correlationId: options.envelope.correlation_id,
      deliveryOutcome,
      effectCount: effectExecuted ? 1 : 0,
      processedAt: nowIso,
    });
    this.inbox.push(receipt);

    return {
      status,
      effectExecuted,
      receipt,
    };
  }

  async getInboxReceipt(
    tenantId: string,
    consumer: string,
    externalEventId: string
  ): Promise<InboxReceiptRecord | null> {
    const found = this.inbox.find(
      (r) =>
        r.tenantId === tenantId &&
        r.consumer === consumer &&
        r.externalEventId === externalEventId
    );
    return found ? Object.freeze({ ...found }) : null;
  }

  getCommittedMessageCount(): number {
    return this.committedMessages.size;
  }

  getCommittedAuditCount(): number {
    return this.committedAudits.size;
  }
}

export class PsqlOutboxInboxStore implements OutboxInboxStore {
  private readonly pgEnv: NodeJS.ProcessEnv;

  constructor(config: { readonly connectionUri: string }) {
    this.pgEnv = buildPgEnv(config.connectionUri);
  }

  private async execJson<T>(sql: string): Promise<T> {
    let raw: string;
    try {
      raw = await runPsqlStdin(this.pgEnv, sql);
    } catch (error) {
      throw new OutboxError(
        "DEPENDENCY_UNAVAILABLE",
        `PostgreSQL outbox/inbox operation failed: ${sanitizeSafeErrorMessage(error)}`,
        503
      );
    }
    const line = raw
      .split(/\r?\n/)
      .map((l) => l.trim())
      .filter((l) => l.length > 0)
      .at(-1);
    if (!line) {
      throw new OutboxError(
        "DEPENDENCY_UNAVAILABLE",
        "PostgreSQL outbox/inbox query returned empty JSON payload",
        503
      );
    }
    return JSON.parse(line) as T;
  }

  async commitWithOutbox(
    input: EnqueueOutboxInput,
    localMutation?: LocalOwningMutation,
    options?: { readonly simulateCrashBeforeCommit?: boolean }
  ): Promise<CommitWithOutboxResult> {
    const norm = normalizeAndValidateOutboxInput(input);

    const mutationCtes: string[] = [];
    if (localMutation?.kind === "CONVERSATION_MESSAGE") {
      const receivedAtSql = sqlTimestamptzLiteral(
        localMutation.receivedAt ?? norm.occurredAt
      );
      mutationCtes.push(
        `upsert_conv AS (
          INSERT INTO conversations (
            id, tenant_id, channel_account_id, identity_id, status, last_seq, last_message_at, created_at, updated_at, version
          )
          SELECT
            ${sqlUuidLiteral(localMutation.conversationId)},
            ${sqlUuidLiteral(norm.tenantId)},
            ${sqlUuidLiteral(localMutation.channelAccountId)},
            ${sqlUuidLiteral(localMutation.identityId)},
            'OPEN',
            ${localMutation.messageSeq},
            ${receivedAtSql},
            ${sqlTimestamptzLiteral(norm.occurredAt)},
            ${sqlTimestamptzLiteral(norm.occurredAt)},
            1
          WHERE NOT EXISTS (SELECT 1 FROM existing_outbox)
          ON CONFLICT (tenant_id, id) DO UPDATE SET
            last_seq = GREATEST(conversations.last_seq, EXCLUDED.last_seq),
            last_message_at = COALESCE(EXCLUDED.last_message_at, conversations.last_message_at),
            updated_at = NOW(),
            version = conversations.version + 1
          RETURNING id
        )`,
        `ins_msg AS (
          INSERT INTO messages (
            id, tenant_id, conversation_id, seq, direction, sender_kind, kind, body, received_at, delivery_state, historical_import, created_at
          )
          SELECT
            ${sqlUuidLiteral(localMutation.messageId)},
            ${sqlUuidLiteral(norm.tenantId)},
            ${sqlUuidLiteral(localMutation.conversationId)},
            ${localMutation.messageSeq},
            ${sqlTextB64(localMutation.direction)},
            ${sqlTextB64(localMutation.senderKind)},
            ${sqlTextB64(localMutation.messageKind ?? "TEXT")},
            ${sqlNullableTextB64(localMutation.body)},
            ${receivedAtSql},
            'RECEIVED',
            ${norm.historicalImport ? "TRUE" : "FALSE"},
            ${sqlTimestamptzLiteral(norm.occurredAt)}
          WHERE NOT EXISTS (SELECT 1 FROM existing_outbox)
          ON CONFLICT (tenant_id, conversation_id, seq) DO NOTHING
          RETURNING id
        )`
      );
    } else if (localMutation?.kind === "AUDIT_LOG") {
      mutationCtes.push(
        `ins_audit AS (
          INSERT INTO audit_logs (
            id, tenant_id, actor, action, result, target, trace_id, occurred_at, metadata_redacted
          )
          SELECT
            ${sqlUuidLiteral(localMutation.auditId)},
            ${sqlUuidLiteral(norm.tenantId)},
            ${sqlTextB64(localMutation.actor)},
            ${sqlTextB64(localMutation.action)},
            ${sqlTextB64(localMutation.result)},
            ${sqlTextB64(localMutation.target)},
            ${sqlTextB64(norm.correlationId)},
            ${sqlTimestamptzLiteral(norm.occurredAt)},
            ${sqlJsonbB64(localMutation.metadataRedacted ?? {})}
          WHERE NOT EXISTS (SELECT 1 FROM existing_outbox)
          ON CONFLICT (id) DO NOTHING
          RETURNING id
        )`
      );
    }

    const crashStatement = options?.simulateCrashBeforeCommit
      ? "SELECT 1 / 0;"
      : "";

    const sql = `
BEGIN;
WITH existing_outbox AS (
  SELECT *, TRUE AS deduplicated
  FROM outbox_events
  WHERE tenant_id = ${sqlUuidLiteral(norm.tenantId)}
    AND aggregate_key = ${sqlTextB64(norm.aggregateKey)}
    AND aggregate_version = ${norm.aggregateVersion}
    AND type = ${sqlTextB64(norm.type)}
  LIMIT 1
),
seq_alloc AS (
  SELECT nextval('outbox_stream_seq') AS seq
  WHERE NOT EXISTS (SELECT 1 FROM existing_outbox)
),
${mutationCtes.length > 0 ? `${mutationCtes.join(",\n")},\n` : ""}ins_outbox AS (
  INSERT INTO outbox_events (
    id,
    tenant_id,
    channel_account_id,
    aggregate_key,
    aggregate_version,
    type,
    schema_version,
    event_schema_version,
    contract_revision,
    producer,
    owner_db,
    aggregate_type,
    outbox_seq,
    cursor_token,
    is_replay,
    replay_id,
    original_occurred_at,
    historical_import,
    suppress_side_effects,
    source_kind,
    occurred_at,
    correlation_id,
    payload_minimal,
    state,
    available_at,
    attempts,
    created_at
  )
  SELECT
    ${sqlUuidLiteral(norm.eventId)},
    ${sqlUuidLiteral(norm.tenantId)},
    ${norm.channelAccountId === null ? "NULL" : sqlUuidLiteral(norm.channelAccountId)},
    ${sqlTextB64(norm.aggregateKey)},
    ${norm.aggregateVersion},
    ${sqlTextB64(norm.type)},
    ${sqlTextB64(norm.contractRevision)},
    ${norm.schemaVersion},
    ${sqlTextB64(norm.contractRevision)},
    ${sqlTextB64(norm.producer)},
    ${sqlTextB64(norm.ownerDb)},
    ${sqlTextB64(norm.aggregateType)},
    sa.seq,
    replace(encode(convert_to('cb_seq:' || sa.seq::text, 'utf8'), 'base64'), E'\\n', ''),
    ${norm.isReplay ? "TRUE" : "FALSE"},
    ${norm.replayId === null ? "NULL" : sqlUuidLiteral(norm.replayId)},
    ${norm.originalOccurredAt === null ? "NULL" : sqlTimestamptzLiteral(norm.originalOccurredAt)},
    ${norm.historicalImport ? "TRUE" : "FALSE"},
    ${norm.suppressSideEffects ? "TRUE" : "FALSE"},
    ${sqlTextB64(norm.sourceKind)},
    ${sqlTimestamptzLiteral(norm.occurredAt)},
    ${sqlTextB64(norm.correlationId)},
    ${sqlJsonbB64(norm.payload)},
    'PENDING',
    ${sqlTimestamptzLiteral(norm.availableAt)},
    0,
    ${sqlTimestamptzLiteral(norm.occurredAt)}
  FROM seq_alloc sa
  ON CONFLICT (tenant_id, aggregate_key, aggregate_version, type) DO NOTHING
  RETURNING *, FALSE AS deduplicated
),
fallback_existing AS (
  SELECT *, TRUE AS deduplicated
  FROM outbox_events
  WHERE tenant_id = ${sqlUuidLiteral(norm.tenantId)}
    AND aggregate_key = ${sqlTextB64(norm.aggregateKey)}
    AND aggregate_version = ${norm.aggregateVersion}
    AND type = ${sqlTextB64(norm.type)}
    AND NOT EXISTS (SELECT 1 FROM existing_outbox)
    AND NOT EXISTS (SELECT 1 FROM ins_outbox)
  LIMIT 1
),
combined AS (
  SELECT * FROM existing_outbox
  UNION ALL
  SELECT * FROM ins_outbox
  UNION ALL
  SELECT * FROM fallback_existing
)
SELECT COALESCE((SELECT row_to_json(c)::text FROM combined c LIMIT 1), 'null');
${crashStatement}
COMMIT;`;

    const row = await this.execJson<RawOutboxRow | null>(sql);
    if (!row) {
      const existing = await this.getOutboxRecord(norm.tenantId, norm.eventId);
      if (existing) {
        return { record: existing, deduplicated: true };
      }
      throw new OutboxError(
        "DEPENDENCY_UNAVAILABLE",
        "Outbox commit did not return inserted or existing record",
        503
      );
    }
    return {
      record: mapRawOutboxRow(row),
      deduplicated: Boolean(row.deduplicated),
    };
  }

  async claimDueBatch(
    options: ClaimBatchOptions
  ): Promise<readonly StoredOutboxRecord[]> {
    const nowIso = options.nowIso ?? new Date().toISOString();
    const leaseUntilIso = new Date(
      Date.parse(nowIso) + options.leaseMs
    ).toISOString();
    const tenantClause = options.tenantId
      ? `AND tenant_id = ${sqlUuidLiteral(options.tenantId)}`
      : "";

    const sql = `
BEGIN;
WITH picked AS (
  SELECT id
  FROM outbox_events
  WHERE (
      (state IN ('PENDING', 'RETRY_WAIT') AND available_at <= ${sqlTimestamptzLiteral(nowIso)})
      OR (state = 'CLAIMED' AND locked_until IS NOT NULL AND locked_until <= ${sqlTimestamptzLiteral(nowIso)})
    )
    ${tenantClause}
  ORDER BY outbox_seq ASC
  LIMIT ${Math.max(1, Math.floor(options.limit))}
  FOR UPDATE SKIP LOCKED
),
updated AS (
  UPDATE outbox_events o
  SET
    state = 'CLAIMED',
    attempts = o.attempts + 1,
    claimed_by = ${sqlTextB64(options.workerId)},
    locked_until = ${sqlTimestamptzLiteral(leaseUntilIso)}
  FROM picked
  WHERE o.id = picked.id
  RETURNING o.*
)
SELECT COALESCE(json_agg(row_to_json(u) ORDER BY u.outbox_seq ASC), '[]'::json)::text
FROM updated u;
COMMIT;`;

    const rows = await this.execJson<RawOutboxRow[]>(sql);
    return Object.freeze(rows.map(mapRawOutboxRow));
  }

  async markSucceeded(
    tenantId: string,
    eventId: string,
    publishedAtIso = new Date().toISOString()
  ): Promise<StoredOutboxRecord> {
    const sql = `
BEGIN;
WITH updated AS (
  UPDATE outbox_events
  SET
    state = 'SUCCEEDED',
    locked_until = NULL,
    published_at = ${sqlTimestamptzLiteral(publishedAtIso)},
    last_error_safe = NULL
  WHERE tenant_id = ${sqlUuidLiteral(tenantId)}
    AND id = ${sqlUuidLiteral(eventId)}
  RETURNING *
)
SELECT COALESCE((SELECT row_to_json(u)::text FROM updated u LIMIT 1), 'null');
COMMIT;`;

    const row = await this.execJson<RawOutboxRow | null>(sql);
    if (!row) {
      throw new OutboxError("STATE_CONFLICT", "Outbox event not found", 409);
    }
    return mapRawOutboxRow(row);
  }

  async scheduleRetry(
    tenantId: string,
    eventId: string,
    safeError: string,
    nextAvailableAtIso: string,
    maxAttempts: number
  ): Promise<StoredOutboxRecord> {
    const redacted = sanitizeSafeErrorMessage(safeError);
    const sql = `
BEGIN;
WITH updated AS (
  UPDATE outbox_events
  SET
    state = CASE WHEN attempts >= ${Math.max(1, Math.floor(maxAttempts))} THEN 'QUARANTINED' ELSE 'RETRY_WAIT' END,
    available_at = ${sqlTimestamptzLiteral(nextAvailableAtIso)},
    locked_until = NULL,
    last_error_safe = ${sqlTextB64(redacted)}
  WHERE tenant_id = ${sqlUuidLiteral(tenantId)}
    AND id = ${sqlUuidLiteral(eventId)}
  RETURNING *
)
SELECT COALESCE((SELECT row_to_json(u)::text FROM updated u LIMIT 1), 'null');
COMMIT;`;

    const row = await this.execJson<RawOutboxRow | null>(sql);
    if (!row) {
      throw new OutboxError("STATE_CONFLICT", "Outbox event not found", 409);
    }
    return mapRawOutboxRow(row);
  }

  async prepareReplay(
    tenantId: string,
    eventId: string,
    replayId: string,
    nowIso = new Date().toISOString()
  ): Promise<StoredOutboxRecord> {
    const sql = `
BEGIN;
WITH updated AS (
  UPDATE outbox_events
  SET
    state = 'PENDING',
    is_replay = TRUE,
    replay_id = ${sqlUuidLiteral(replayId)},
    original_occurred_at = COALESCE(original_occurred_at, occurred_at),
    available_at = ${sqlTimestamptzLiteral(nowIso)},
    locked_until = NULL
  WHERE tenant_id = ${sqlUuidLiteral(tenantId)}
    AND id = ${sqlUuidLiteral(eventId)}
  RETURNING *
)
SELECT COALESCE((SELECT row_to_json(u)::text FROM updated u LIMIT 1), 'null');
COMMIT;`;

    const row = await this.execJson<RawOutboxRow | null>(sql);
    if (!row) {
      throw new OutboxError("STATE_CONFLICT", "Outbox event not found", 409);
    }
    return mapRawOutboxRow(row);
  }

  async listStreamSince(
    tenantId: string,
    afterOutboxSeq: number,
    limit: number
  ): Promise<readonly StoredOutboxRecord[]> {
    const sql = `
SELECT COALESCE(json_agg(row_to_json(o) ORDER BY o.outbox_seq ASC), '[]'::json)::text
FROM (
  SELECT *
  FROM outbox_events
  WHERE tenant_id = ${sqlUuidLiteral(tenantId)}
    AND outbox_seq > ${Math.max(0, Math.floor(afterOutboxSeq))}
  ORDER BY outbox_seq ASC
  LIMIT ${Math.max(1, Math.floor(limit))}
) o;`;

    const rows = await this.execJson<RawOutboxRow[]>(sql);
    return Object.freeze(rows.map(mapRawOutboxRow));
  }

  async getOutboxRecord(
    tenantId: string,
    eventId: string
  ): Promise<StoredOutboxRecord | null> {
    const sql = `
SELECT COALESCE((
  SELECT row_to_json(o)::text
  FROM outbox_events o
  WHERE o.tenant_id = ${sqlUuidLiteral(tenantId)}
    AND o.id = ${sqlUuidLiteral(eventId)}
  LIMIT 1
), 'null');`;

    const row = await this.execJson<RawOutboxRow | null>(sql);
    return row ? mapRawOutboxRow(row) : null;
  }

  async consumeEvent(options: ConsumeEventOptions): Promise<ConsumeInboxResult> {
    validateOutboxEnvelope(options.envelope);

    const env = options.envelope;
    const tenantId = env.scope.tenant_id;
    const nowIso = options.nowIso ?? new Date().toISOString();
    const auditEffect = options.sqlAuditEffect;
    const crashStatement = options.simulateCrashBeforeCommit
      ? "SELECT 1 / 0;"
      : "";

    const auditCte = auditEffect
      ? `,
ins_effect AS (
  INSERT INTO audit_logs (
    id, tenant_id, actor, action, result, target, trace_id, occurred_at, metadata_redacted
  )
  SELECT
    ${sqlUuidLiteral(auditEffect.auditId)},
    ${sqlUuidLiteral(tenantId)},
    ${sqlTextB64(auditEffect.actor)},
    ${sqlTextB64(auditEffect.action)},
    'SUCCESS',
    ${sqlTextB64(auditEffect.target)},
    ${sqlTextB64(env.correlation_id)},
    ${sqlTimestamptzLiteral(nowIso)},
    ${sqlJsonbB64({
      event_id: env.event_id,
      aggregate_type: env.aggregate_type,
      aggregate_ref: env.aggregate_ref,
      aggregate_seq: env.aggregate_seq,
      historical_import: env.history.historical_import,
    })}
  FROM inserted_receipt ir
  WHERE ir.delivery_outcome = 'APPLIED'
  ON CONFLICT (id) DO NOTHING
  RETURNING id
)`
      : "";

    const sql = `
BEGIN;
WITH existing_receipt AS (
  SELECT r.*
  FROM inbox_receipts r
  WHERE r.tenant_id = ${sqlUuidLiteral(tenantId)}
    AND r.consumer = ${sqlTextB64(options.consumer)}
    AND r.external_event_id = ${sqlTextB64(env.event_id)}
  LIMIT 1
),
cursor_row AS (
  SELECT last_applied_aggregate_seq
  FROM consumer_aggregate_cursors
  WHERE tenant_id = ${sqlUuidLiteral(tenantId)}
    AND consumer = ${sqlTextB64(options.consumer)}
    AND aggregate_type = ${sqlTextB64(env.aggregate_type)}
    AND aggregate_key = ${sqlTextB64(env.aggregate_ref)}
  FOR UPDATE
),
decision AS (
  SELECT
    EXISTS (SELECT 1 FROM existing_receipt) AS is_duplicate,
    CASE
      WHEN EXISTS (SELECT 1 FROM existing_receipt) THEN 'DUPLICATE_NOOP'
      WHEN COALESCE((SELECT last_applied_aggregate_seq FROM cursor_row), 0) >= ${env.aggregate_seq} THEN 'IGNORED_STALE'
      WHEN ${env.history.historical_import ? "TRUE" : "FALSE"} OR ${env.history.suppress_side_effects ? "TRUE" : "FALSE"} THEN 'SUPPRESSED_HISTORY'
      ELSE 'APPLIED'
    END AS status_value,
    CASE
      WHEN COALESCE((SELECT last_applied_aggregate_seq FROM cursor_row), 0) >= ${env.aggregate_seq} THEN 'IGNORED_STALE'
      WHEN ${env.history.historical_import ? "TRUE" : "FALSE"} OR ${env.history.suppress_side_effects ? "TRUE" : "FALSE"} THEN 'SUPPRESSED_HISTORY'
      ELSE 'APPLIED'
    END AS outcome_value
),
inserted_receipt AS (
  INSERT INTO inbox_receipts (
    tenant_id,
    consumer,
    external_event_id,
    processed_at,
    producer,
    event_type,
    event_schema_version,
    contract_revision,
    aggregate_type,
    aggregate_key,
    aggregate_seq,
    historical_import,
    suppress_side_effects,
    source_kind,
    correlation_id,
    delivery_outcome,
    effect_count
  )
  SELECT
    ${sqlUuidLiteral(tenantId)},
    ${sqlTextB64(options.consumer)},
    ${sqlTextB64(env.event_id)},
    ${sqlTimestamptzLiteral(nowIso)},
    ${sqlTextB64(env.producer)},
    ${sqlTextB64(env.type)},
    ${env.schema_version},
    ${sqlTextB64(env.contract_revision)},
    ${sqlTextB64(env.aggregate_type)},
    ${sqlTextB64(env.aggregate_ref)},
    ${env.aggregate_seq},
    ${env.history.historical_import ? "TRUE" : "FALSE"},
    ${env.history.suppress_side_effects ? "TRUE" : "FALSE"},
    ${sqlTextB64(env.history.source_kind)},
    ${sqlTextB64(env.correlation_id)},
    d.outcome_value,
    CASE WHEN d.outcome_value = 'APPLIED' THEN 1 ELSE 0 END
  FROM decision d
  WHERE NOT d.is_duplicate
  ON CONFLICT (tenant_id, consumer, external_event_id) DO NOTHING
  RETURNING *
),
upserted_cursor AS (
  INSERT INTO consumer_aggregate_cursors (
    tenant_id,
    consumer,
    aggregate_type,
    aggregate_key,
    last_applied_aggregate_seq,
    last_event_id,
    updated_at
  )
  SELECT
    ir.tenant_id,
    ir.consumer,
    ir.aggregate_type,
    ir.aggregate_key,
    ir.aggregate_seq,
    (ir.external_event_id)::uuid,
    ir.processed_at
  FROM inserted_receipt ir
  WHERE ir.delivery_outcome IN ('APPLIED', 'SUPPRESSED_HISTORY')
  ON CONFLICT (tenant_id, consumer, aggregate_type, aggregate_key) DO UPDATE SET
    last_applied_aggregate_seq = GREATEST(
      consumer_aggregate_cursors.last_applied_aggregate_seq,
      EXCLUDED.last_applied_aggregate_seq
    ),
    last_event_id = CASE
      WHEN EXCLUDED.last_applied_aggregate_seq > consumer_aggregate_cursors.last_applied_aggregate_seq
        THEN EXCLUDED.last_event_id
      ELSE consumer_aggregate_cursors.last_event_id
    END,
    updated_at = NOW()
  RETURNING tenant_id
)${auditCte},
final_row AS (
  SELECT
    ir.*,
    ir.delivery_outcome AS status
  FROM inserted_receipt ir
  UNION ALL
  SELECT
    r.*,
    'DUPLICATE_NOOP' AS status
  FROM inbox_receipts r
  WHERE NOT EXISTS (SELECT 1 FROM inserted_receipt)
    AND r.tenant_id = ${sqlUuidLiteral(tenantId)}
    AND r.consumer = ${sqlTextB64(options.consumer)}
    AND r.external_event_id = ${sqlTextB64(env.event_id)}
  LIMIT 1
)
SELECT COALESCE((SELECT row_to_json(f)::text FROM final_row f LIMIT 1), 'null');
${crashStatement}
COMMIT;`;

    const row = await this.execJson<RawInboxRow | null>(sql);
    if (!row) {
      const existing = await this.getInboxReceipt(
        tenantId,
        options.consumer,
        env.event_id
      );
      if (existing) {
        return {
          status: "DUPLICATE_NOOP",
          effectExecuted: false,
          receipt: existing,
        };
      }
      throw new OutboxError(
        "DEPENDENCY_UNAVAILABLE",
        "Inbox consume transaction returned no receipt record",
        503
      );
    }
    return {
      status: row.status,
      effectExecuted: row.status === "APPLIED",
      receipt: mapRawInboxReceipt(row),
    };
  }

  async getInboxReceipt(
    tenantId: string,
    consumer: string,
    externalEventId: string
  ): Promise<InboxReceiptRecord | null> {
    const sql = `
SELECT COALESCE((
  SELECT row_to_json(r)::text
  FROM inbox_receipts r
  WHERE r.tenant_id = ${sqlUuidLiteral(tenantId)}
    AND r.consumer = ${sqlTextB64(consumer)}
    AND r.external_event_id = ${sqlTextB64(externalEventId)}
  LIMIT 1
), 'null');`;

    const row = await this.execJson<Omit<RawInboxRow, "status"> | null>(sql);
    return row ? mapRawInboxReceipt(row) : null;
  }
}
