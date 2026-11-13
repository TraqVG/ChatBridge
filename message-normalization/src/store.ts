import { randomUUID } from "node:crypto";
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
  encodeCursorToken,
  sanitizeSafeErrorMessage,
  type OutboxEventEnvelope,
  type OutboxSourceKind,
  type OutboxState,
  type StoredOutboxRecord,
} from "../../queue-and-retry/src/outbox-types.js";
import {
  NormalizationError,
  type MessagePartKind,
  type NormalizedMessageEnvelope,
  type PersistedMessageRecord,
  type PersistNormalizedResult,
} from "./types.js";

export interface NormalizedMessageStore {
  persistNormalizedMessage(
    envelope: NormalizedMessageEnvelope,
    options?: { readonly simulateCrashBeforeCommit?: boolean }
  ): Promise<PersistNormalizedResult>;
  getConversationState(
    tenantId: string,
    conversationId: string
  ): Promise<{
    readonly status: "OPEN" | "RESOLVED";
    readonly lastSeq: number;
    readonly version: number;
  } | null>;
}

interface StoredConvRecord {
  id: string;
  tenantId: string;
  channelAccountId: string;
  identityId: string | null;
  webParticipantId: string | null;
  status: "OPEN" | "RESOLVED";
  lastSeq: number;
  version: number;
  lastMessageAt: string | null;
  createdAt: string;
}

export class InMemoryNormalizedMessageStore implements NormalizedMessageStore {
  private nextOutboxSeq = 1;
  private readonly identities = new Map<string, string>();
  private readonly conversations: StoredConvRecord[] = [];
  private readonly activeClaimants = new Map<string, string | null>();
  private readonly dedupBySourceRef = new Map<string, PersistedMessageRecord>();
  private readonly mediaAssets = new Map<string, NormalizedMessageEnvelope["parts"][number]["mediaAsset"]>();
  private readonly webhookProcessed = new Set<string>();

  seedResolvedConversationWithClaimant(params: {
    readonly conversationId: string;
    readonly tenantId: string;
    readonly channelAccountId: string;
    readonly identityId: string | null;
    readonly webParticipantId: string | null;
    readonly lastSeq: number;
    readonly claimantPrincipalId: string;
  }): void {
    this.conversations.push({
      id: params.conversationId,
      tenantId: params.tenantId,
      channelAccountId: params.channelAccountId,
      identityId: params.identityId,
      webParticipantId: params.webParticipantId,
      status: "RESOLVED",
      lastSeq: params.lastSeq,
      version: 1,
      lastMessageAt: null,
      createdAt: new Date().toISOString(),
    });
    this.activeClaimants.set(params.conversationId, params.claimantPrincipalId);
  }

  getActiveClaimant(conversationId: string): string | null {
    return this.activeClaimants.get(conversationId) ?? null;
  }

  getStoredMediaAssetCount(): number {
    return this.mediaAssets.size;
  }

  async getConversationState(
    tenantId: string,
    conversationId: string
  ): Promise<{
    readonly status: "OPEN" | "RESOLVED";
    readonly lastSeq: number;
    readonly version: number;
  } | null> {
    const found = this.conversations.find(
      (c) => c.tenantId === tenantId && c.id === conversationId
    );
    return found
      ? { status: found.status, lastSeq: found.lastSeq, version: found.version }
      : null;
  }

  async persistNormalizedMessage(
    env: NormalizedMessageEnvelope,
    options?: { readonly simulateCrashBeforeCommit?: boolean }
  ): Promise<PersistNormalizedResult> {
    const dedupKey = `${env.tenantId}|${env.channelAccountId}|${env.sourceKind}|${env.sourceFingerprint}`;
    const existing = this.dedupBySourceRef.get(dedupKey);
    if (existing) {
      return { deduplicated: true, message: existing };
    }

    if (options?.simulateCrashBeforeCommit) {
      throw new NormalizationError(
        "DEPENDENCY_UNAVAILABLE",
        "Simulated crash before message persistence commit",
        503
      );
    }

    let resolvedIdentityId: string | null = env.explicitIdentityId;
    if (!env.webParticipantId && resolvedIdentityId === null) {
      const identKey = `${env.tenantId}|${env.channelAccountId}|${env.senderScopeKey}`;
      const existingIdent = this.identities.get(identKey);
      if (existingIdent) {
        resolvedIdentityId = existingIdent;
      } else {
        resolvedIdentityId = randomUUID();
        this.identities.set(identKey, resolvedIdentityId);
      }
    }

    let conv = this.conversations.find((c) => {
      if (c.tenantId !== env.tenantId || c.channelAccountId !== env.channelAccountId) {
        return false;
      }
      if (env.conversationIdOverride) {
        return c.id === env.conversationIdOverride;
      }
      return (
        (resolvedIdentityId !== null && c.identityId === resolvedIdentityId) ||
        (env.webParticipantId !== null && c.webParticipantId === env.webParticipantId)
      );
    });

    let reopened = false;
    if (!conv) {
      conv = {
        id: env.conversationIdOverride ?? randomUUID(),
        tenantId: env.tenantId,
        channelAccountId: env.channelAccountId,
        identityId: resolvedIdentityId,
        webParticipantId: env.webParticipantId,
        status: "OPEN",
        lastSeq: 1,
        version: 1,
        lastMessageAt: env.receivedAtIso,
        createdAt: env.receivedAtIso,
      };
      this.conversations.push(conv);
    } else {
      conv.lastSeq += 1;
      conv.lastMessageAt = env.receivedAtIso;
      conv.version += 1;
      if (conv.status === "RESOLVED" && !env.historicalImport) {
        conv.status = "OPEN";
        reopened = true;
        this.activeClaimants.set(conv.id, null);
      }
    }

    const messageId = randomUUID();
    const messageSeq = conv.lastSeq;
    const outboxSeq = this.nextOutboxSeq++;
    const eventId = randomUUID();

    for (const part of env.parts) {
      if (part.mediaAsset) {
        this.mediaAssets.set(part.mediaAsset.id, part.mediaAsset);
      }
    }

    if (env.webhookReceiptId) {
      this.webhookProcessed.add(`${env.tenantId}:${env.webhookReceiptId}`);
    }

    const outboxEnvelope: OutboxEventEnvelope = Object.freeze({
      event_id: eventId,
      schema_version: 1,
      contract_revision: "1.0.0",
      producer: "chatbridge",
      owner_db: "chatbridge_db",
      type: "message.created",
      scope: Object.freeze({
        tenant_id: env.tenantId,
        channel_account_id: env.channelAccountId,
      }),
      aggregate_type: "conversation",
      aggregate_ref: `conv:${conv.id}`,
      aggregate_seq: messageSeq,
      stream_cursor: Object.freeze({
        partition_key: env.tenantId,
        outbox_seq: outboxSeq,
        cursor_token: encodeCursorToken(outboxSeq),
      }),
      replay: Object.freeze({
        is_replay: false,
        replay_id: null,
        attempt: 1,
      }),
      history: Object.freeze({
        historical_import: env.historicalImport,
        suppress_side_effects: env.suppressSideEffects,
        source_kind: env.outboxSourceKind,
      }),
      occurred_at: env.receivedAtIso,
      correlation_id: env.correlationId,
      payload: Object.freeze({
        ...env.outboxPayloadMinimal,
        conversation_id: conv.id,
        message_id: messageId,
        message_seq: messageSeq,
      }),
    });

    const outboxRecord: StoredOutboxRecord = Object.freeze({
      envelope: outboxEnvelope,
      state: "PENDING",
      available_at: env.receivedAtIso,
      locked_until: null,
      attempts: 0,
      claimed_by: null,
      published_at: null,
      last_error_safe: null,
      created_at: env.receivedAtIso,
    });

    const persisted: PersistedMessageRecord = Object.freeze({
      conversationId: conv.id,
      identityId: resolvedIdentityId,
      webParticipantId: env.webParticipantId,
      conversationStatus: conv.status,
      messageId,
      seq: messageSeq,
      kind: env.messageKind,
      body: env.body,
      historicalImport: env.historicalImport,
      parts: Object.freeze(
        env.parts.map((p) =>
          Object.freeze({
            id: p.id,
            partIndex: p.partIndex,
            kind: p.kind,
            payload: p.payload,
            mediaAssetId: p.mediaAsset ? p.mediaAsset.id : null,
          })
        )
      ),
      outboxRecord,
      reopenedConversation: reopened,
    });

    this.dedupBySourceRef.set(dedupKey, persisted);
    return { deduplicated: false, message: persisted };
  }
}

interface RawPersistResponse {
  readonly deduplicated: boolean;
  readonly reopened_conversation: boolean;
  readonly conversation_id: string;
  readonly identity_id: string | null;
  readonly web_participant_id: string | null;
  readonly conversation_status: "OPEN" | "RESOLVED";
  readonly message_id: string;
  readonly message_seq: number | string;
  readonly message_kind: MessagePartKind;
  readonly message_body: string | null;
  readonly historical_import: boolean;
  readonly parts: ReadonlyArray<{
    readonly id: string;
    readonly part_index: number;
    readonly kind: MessagePartKind;
    readonly payload: Record<string, unknown>;
    readonly media_asset_id: string | null;
  }>;
  readonly outbox: {
    readonly id: string;
    readonly tenant_id: string;
    readonly channel_account_id: string | null;
    readonly event_schema_version: number | string;
    readonly contract_revision: string;
    readonly producer: string;
    readonly owner_db: string;
    readonly aggregate_type: string;
    readonly aggregate_key: string;
    readonly aggregate_version: number | string;
    readonly outbox_seq: number | string;
    readonly cursor_token: string;
    readonly is_replay: boolean;
    readonly replay_id: string | null;
    readonly original_occurred_at: string | null;
    readonly historical_import: boolean;
    readonly suppress_side_effects: boolean;
    readonly source_kind: OutboxSourceKind;
    readonly occurred_at: string;
    readonly correlation_id: string;
    readonly payload_minimal: Record<string, unknown>;
    readonly state: OutboxState;
    readonly available_at: string;
    readonly locked_until: string | null;
    readonly attempts: number | string;
    readonly claimed_by: string | null;
    readonly published_at: string | null;
    readonly last_error_safe: string | null;
    readonly created_at: string;
  };
}

function mapPersistResponse(raw: RawPersistResponse): PersistNormalizedResult {
  const ob = raw.outbox;
  const outboxRecord: StoredOutboxRecord = Object.freeze({
    envelope: Object.freeze({
      event_id: ob.id,
      schema_version: Number(ob.event_schema_version),
      contract_revision: ob.contract_revision,
      producer: ob.producer,
      owner_db: ob.owner_db,
      type: "message.created",
      scope:
        ob.channel_account_id !== null
          ? Object.freeze({
              tenant_id: ob.tenant_id,
              channel_account_id: ob.channel_account_id,
            })
          : Object.freeze({ tenant_id: ob.tenant_id }),
      aggregate_type: ob.aggregate_type,
      aggregate_ref: ob.aggregate_key,
      aggregate_seq: Number(ob.aggregate_version),
      stream_cursor: Object.freeze({
        partition_key: ob.tenant_id,
        outbox_seq: Number(ob.outbox_seq),
        cursor_token: ob.cursor_token,
      }),
      replay: ob.is_replay
        ? Object.freeze({
            is_replay: true,
            replay_id: ob.replay_id,
            attempt: Math.max(1, Number(ob.attempts) || 1),
            original_occurred_at: ob.original_occurred_at,
          })
        : Object.freeze({
            is_replay: false,
            replay_id: null,
            attempt: Math.max(1, Number(ob.attempts) || 1),
          }),
      history: Object.freeze({
        historical_import: Boolean(ob.historical_import),
        suppress_side_effects: Boolean(ob.suppress_side_effects),
        source_kind: ob.source_kind,
      }),
      occurred_at: ob.occurred_at,
      correlation_id: ob.correlation_id,
      payload: Object.freeze({ ...ob.payload_minimal }),
    }),
    state: ob.state,
    available_at: ob.available_at,
    locked_until: ob.locked_until,
    attempts: Number(ob.attempts),
    claimed_by: ob.claimed_by,
    published_at: ob.published_at,
    last_error_safe: ob.last_error_safe,
    created_at: ob.created_at,
  });

  return Object.freeze({
    deduplicated: Boolean(raw.deduplicated),
    message: Object.freeze({
      conversationId: raw.conversation_id,
      identityId: raw.identity_id,
      webParticipantId: raw.web_participant_id,
      conversationStatus: raw.conversation_status,
      messageId: raw.message_id,
      seq: Number(raw.message_seq),
      kind: raw.message_kind,
      body: raw.message_body,
      historicalImport: Boolean(raw.historical_import),
      parts: Object.freeze(
        raw.parts.map((p) =>
          Object.freeze({
            id: p.id,
            partIndex: Number(p.part_index),
            kind: p.kind,
            payload: Object.freeze({ ...p.payload }),
            mediaAssetId: p.media_asset_id,
          })
        )
      ),
      outboxRecord,
      reopenedConversation: Boolean(raw.reopened_conversation),
    }),
  });
}

export class PsqlNormalizedMessageStore implements NormalizedMessageStore {
  private readonly pgEnv: NodeJS.ProcessEnv;

  constructor(config: { readonly connectionUri: string }) {
    this.pgEnv = buildPgEnv(config.connectionUri);
  }

  async getConversationState(
    tenantId: string,
    conversationId: string
  ): Promise<{
    readonly status: "OPEN" | "RESOLVED";
    readonly lastSeq: number;
    readonly version: number;
  } | null> {
    let raw: string;
    try {
      raw = await runPsqlStdin(
        this.pgEnv,
        `SELECT COALESCE((
          SELECT json_build_object('status', status, 'last_seq', last_seq, 'version', version)::text
          FROM conversations
          WHERE tenant_id = ${sqlUuidLiteral(tenantId)} AND id = ${sqlUuidLiteral(conversationId)}
          LIMIT 1
        ), 'null');`
      );
    } catch (error) {
      throw new NormalizationError(
        "DEPENDENCY_UNAVAILABLE",
        `Failed querying conversation state: ${sanitizeSafeErrorMessage(error)}`,
        503
      );
    }
    const line = raw
      .split(/\r?\n/)
      .map((l) => l.trim())
      .filter((l) => l.length > 0)
      .at(-1);
    if (!line || line === "null") {
      return null;
    }
    const parsed = JSON.parse(line) as {
      status: "OPEN" | "RESOLVED";
      last_seq: number | string;
      version: number | string;
    };
    return {
      status: parsed.status,
      lastSeq: Number(parsed.last_seq),
      version: Number(parsed.version),
    };
  }

  async persistNormalizedMessage(
    env: NormalizedMessageEnvelope,
    options?: { readonly simulateCrashBeforeCommit?: boolean }
  ): Promise<PersistNormalizedResult> {
    const newIdentityId = randomUUID();
    const newConvId = randomUUID();
    const newMessageId = randomUUID();
    const newRefId = randomUUID();
    const newEventId = randomUUID();

    const partsJson = env.parts.map((p) => ({
      part_id: p.id,
      part_index: p.partIndex,
      kind: p.kind,
      payload: p.payload,
      media_asset_id: p.mediaAsset?.id ?? null,
      content_type: p.mediaAsset?.contentType ?? null,
      bytes: p.mediaAsset?.bytes ?? null,
      sha256_hex: p.mediaAsset?.sha256Hex ?? null,
      provider_ref: p.mediaAsset?.providerRef ?? null,
      storage_provider: p.mediaAsset?.storageProvider ?? null,
      storage_object_key: p.mediaAsset?.storageObjectKey ?? null,
      thumbnail_key: p.mediaAsset?.thumbnailKey ?? null,
      ingest_state: p.mediaAsset?.ingestState ?? null,
      quarantine_state: p.mediaAsset?.quarantineState ?? null,
    }));

    const outboxBasePayloadJson = JSON.stringify(env.outboxPayloadMinimal);
    const crashStmt = options?.simulateCrashBeforeCommit ? "SELECT 1 / 0;" : "";
    const receiptCte = env.webhookReceiptId
      ? `,
mark_receipt AS (
  UPDATE webhook_receipts wr
  SET state = 'PROCESSED', processed_at = COALESCE(wr.processed_at, NOW())
  FROM ins_msg im
  WHERE wr.tenant_id = im.tenant_id
    AND wr.id = ${sqlUuidLiteral(env.webhookReceiptId)}
  RETURNING wr.id
)`
      : "";
    const receiptJoin = env.webhookReceiptId
      ? "LEFT JOIN mark_receipt mr ON TRUE"
      : "";

    const sql = `
BEGIN;
SELECT 1 FROM (
  SELECT pg_advisory_xact_lock(
    hashtext(${sqlTextB64(env.tenantId)}),
    hashtext(${sqlTextB64(env.senderScopeKey)})
  )
) _lock WHERE FALSE;
WITH existing_ref AS (
  SELECT msr.message_id, msr.tenant_id
  FROM message_source_refs msr
  WHERE msr.tenant_id = ${sqlUuidLiteral(env.tenantId)}
    AND msr.channel_account_id = ${sqlUuidLiteral(env.channelAccountId)}
    AND msr.source_kind = ${sqlTextB64(env.sourceKind)}
    AND msr.source_fingerprint = ${sqlTextB64(env.sourceFingerprint)}
  LIMIT 1
),
existing_ident AS (
  SELECT ci.id
  FROM channel_identities ci
  WHERE NOT EXISTS (SELECT 1 FROM existing_ref)
    AND ${env.webParticipantId === null && env.explicitIdentityId === null ? "TRUE" : "FALSE"}
    AND ci.tenant_id = ${sqlUuidLiteral(env.tenantId)}
    AND ci.channel_account_id = ${sqlUuidLiteral(env.channelAccountId)}
    AND (
      (${sqlNullableTextB64(env.providerSenderRef)} IS NOT NULL AND ci.provider_sender_ref = ${sqlNullableTextB64(env.providerSenderRef)})
      OR ci.scope_key = ${sqlTextB64(env.senderScopeKey)}
    )
  LIMIT 1
),
ins_ident AS (
  INSERT INTO channel_identities (
    id, tenant_id, channel_account_id, provider_sender_ref, scope_key, display_name, created_at, updated_at
  )
  SELECT
    ${sqlUuidLiteral(newIdentityId)},
    ${sqlUuidLiteral(env.tenantId)},
    ${sqlUuidLiteral(env.channelAccountId)},
    ${sqlNullableTextB64(env.providerSenderRef)},
    ${sqlTextB64(env.senderScopeKey)},
    ${sqlNullableTextB64(env.senderDisplayName)},
    ${sqlTimestamptzLiteral(env.receivedAtIso)},
    ${sqlTimestamptzLiteral(env.receivedAtIso)}
  WHERE NOT EXISTS (SELECT 1 FROM existing_ref)
    AND ${env.webParticipantId === null && env.explicitIdentityId === null ? "TRUE" : "FALSE"}
    AND NOT EXISTS (SELECT 1 FROM existing_ident)
  ON CONFLICT (tenant_id, channel_account_id, provider_sender_ref)
    WHERE provider_sender_ref IS NOT NULL
    DO NOTHING
  RETURNING id
),
resolved_ident AS (
  SELECT ${env.explicitIdentityId ? sqlUuidLiteral(env.explicitIdentityId) : "NULL::uuid"} AS id
  WHERE ${env.explicitIdentityId !== null ? "TRUE" : "FALSE"}
  UNION ALL
  SELECT id FROM existing_ident
  UNION ALL
  SELECT id FROM ins_ident
),
existing_conv AS (
  SELECT c.id, c.tenant_id, c.identity_id, c.web_participant_id, c.status, c.last_seq
  FROM conversations c
  WHERE NOT EXISTS (SELECT 1 FROM existing_ref)
    AND c.tenant_id = ${sqlUuidLiteral(env.tenantId)}
    AND c.channel_account_id = ${sqlUuidLiteral(env.channelAccountId)}
    AND (
      (${env.conversationIdOverride ? sqlUuidLiteral(env.conversationIdOverride) : "NULL::uuid"} IS NOT NULL
        AND c.id = ${env.conversationIdOverride ? sqlUuidLiteral(env.conversationIdOverride) : "NULL::uuid"})
      OR (
        ${env.conversationIdOverride ? "FALSE" : "TRUE"}
        AND (
          (c.identity_id IS NOT NULL AND c.identity_id = (SELECT id FROM resolved_ident LIMIT 1))
          OR (c.web_participant_id IS NOT NULL AND c.web_participant_id = ${env.webParticipantId ? sqlUuidLiteral(env.webParticipantId) : "NULL::uuid"})
        )
      )
    )
  ORDER BY c.created_at DESC, c.id DESC
  LIMIT 1
  FOR UPDATE
),
upd_conv AS (
  UPDATE conversations c
  SET
    last_seq = c.last_seq + 1,
    last_message_at = GREATEST(COALESCE(c.last_message_at, ${sqlTimestamptzLiteral(env.receivedAtIso)}), ${sqlTimestamptzLiteral(env.receivedAtIso)}),
    status = CASE WHEN c.status = 'RESOLVED' AND NOT ${env.historicalImport ? "TRUE" : "FALSE"} THEN 'OPEN' ELSE c.status END,
    updated_at = NOW(),
    version = c.version + 1
  FROM existing_conv ec
  WHERE c.id = ec.id
  RETURNING
    c.id,
    c.tenant_id,
    c.identity_id,
    c.web_participant_id,
    c.status,
    c.last_seq AS assigned_seq,
    (ec.status = 'RESOLVED' AND NOT ${env.historicalImport ? "TRUE" : "FALSE"}) AS reopened_conversation
),
clear_claim AS (
  UPDATE conversation_assignments ca
  SET
    claimant_principal_id = NULL,
    lease_id = NULL,
    lease_until = NULL,
    released_at = NOW()
  FROM upd_conv uc
  WHERE ca.tenant_id = uc.tenant_id
    AND ca.conversation_id = uc.id
    AND uc.reopened_conversation
  RETURNING ca.conversation_id
),
ins_conv AS (
  INSERT INTO conversations (
    id, tenant_id, channel_account_id, identity_id, web_participant_id, status, last_seq, last_message_at, created_at, updated_at, version
  )
  SELECT
    ${env.conversationIdOverride ? sqlUuidLiteral(env.conversationIdOverride) : sqlUuidLiteral(newConvId)},
    ${sqlUuidLiteral(env.tenantId)},
    ${sqlUuidLiteral(env.channelAccountId)},
    (SELECT id FROM resolved_ident LIMIT 1),
    ${env.webParticipantId ? sqlUuidLiteral(env.webParticipantId) : "NULL::uuid"},
    'OPEN',
    1,
    ${sqlTimestamptzLiteral(env.receivedAtIso)},
    ${sqlTimestamptzLiteral(env.receivedAtIso)},
    ${sqlTimestamptzLiteral(env.receivedAtIso)},
    1
  WHERE NOT EXISTS (SELECT 1 FROM existing_ref)
    AND NOT EXISTS (SELECT 1 FROM existing_conv)
  RETURNING
    id,
    tenant_id,
    identity_id,
    web_participant_id,
    status,
    last_seq AS assigned_seq,
    FALSE AS reopened_conversation
),
resolved_conv AS (
  SELECT * FROM upd_conv
  UNION ALL
  SELECT * FROM ins_conv
),
ins_msg AS (
  INSERT INTO messages (
    id, tenant_id, conversation_id, seq, direction, sender_kind, sender_ref, kind, body, received_at, provider_at, delivery_state, historical_import, created_at
  )
  SELECT
    ${sqlUuidLiteral(newMessageId)},
    rc.tenant_id,
    rc.id,
    rc.assigned_seq,
    ${sqlTextB64(env.direction)},
    ${sqlTextB64(env.senderKind)},
    ${sqlNullableTextB64(env.senderRef)},
    ${sqlTextB64(env.messageKind)},
    ${sqlNullableTextB64(env.body)},
    ${sqlTimestamptzLiteral(env.receivedAtIso)},
    ${env.providerAtIso ? sqlTimestamptzLiteral(env.providerAtIso) : "NULL::timestamptz"},
    CASE WHEN ${sqlTextB64(env.direction)} = 'IN' THEN 'RECEIVED' ELSE 'QUEUED' END,
    ${env.historicalImport ? "TRUE" : "FALSE"},
    ${sqlTimestamptzLiteral(env.receivedAtIso)}
  FROM resolved_conv rc
  ON CONFLICT (tenant_id, conversation_id, seq) DO NOTHING
  RETURNING *
),
input_parts AS (
  SELECT *
  FROM jsonb_to_recordset(${sqlJsonbB64(partsJson)}) AS x(
    part_id uuid,
    part_index integer,
    kind text,
    payload jsonb,
    media_asset_id uuid,
    content_type text,
    bytes bigint,
    sha256_hex text,
    provider_ref text,
    storage_provider text,
    storage_object_key text,
    thumbnail_key text,
    ingest_state text,
    quarantine_state text
  )
),
ins_media AS (
  INSERT INTO media_assets (
    id, tenant_id, content_type, bytes, sha256_hex, provider_ref, storage_provider, storage_object_key, thumbnail_key, ingest_state, quarantine_state, created_at, updated_at
  )
  SELECT DISTINCT ON (p.media_asset_id)
    p.media_asset_id,
    im.tenant_id,
    p.content_type,
    p.bytes,
    p.sha256_hex,
    p.provider_ref,
    p.storage_provider,
    p.storage_object_key,
    p.thumbnail_key,
    p.ingest_state,
    p.quarantine_state,
    im.created_at,
    im.created_at
  FROM ins_msg im
  JOIN input_parts p ON TRUE
  WHERE p.media_asset_id IS NOT NULL
  ON CONFLICT (id) DO NOTHING
  RETURNING id
),
ins_parts AS (
  INSERT INTO message_parts (
    id, tenant_id, message_id, part_index, kind, payload, media_asset_id, created_at
  )
  SELECT
    p.part_id,
    im.tenant_id,
    im.id,
    p.part_index,
    p.kind,
    p.payload,
    COALESCE(ma.id, p.media_asset_id),
    im.created_at
  FROM ins_msg im
  JOIN input_parts p ON TRUE
  LEFT JOIN ins_media ma ON ma.id = p.media_asset_id
  ON CONFLICT (tenant_id, message_id, part_index) DO NOTHING
  RETURNING id, part_index, kind, payload, media_asset_id
),
ins_ref AS (
  INSERT INTO message_source_refs (
    id, tenant_id, message_id, channel_account_id, source_kind, source_fingerprint, created_at
  )
  SELECT
    ${sqlUuidLiteral(newRefId)},
    im.tenant_id,
    im.id,
    ${sqlUuidLiteral(env.channelAccountId)},
    ${sqlTextB64(env.sourceKind)},
    ${sqlTextB64(env.sourceFingerprint)},
    im.created_at
  FROM ins_msg im
  ON CONFLICT (tenant_id, channel_account_id, source_kind, source_fingerprint) DO NOTHING
  RETURNING id
),
seq_alloc AS (
  SELECT nextval('outbox_stream_seq') AS seq
  FROM ins_msg
),
ins_outbox AS (
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
    ${sqlUuidLiteral(newEventId)},
    im.tenant_id,
    ${sqlUuidLiteral(env.channelAccountId)},
    'conv:' || im.conversation_id::text,
    im.seq,
    'message.created',
    '1.0.0',
    1,
    '1.0.0',
    'chatbridge',
    'chatbridge_db',
    'conversation',
    sa.seq,
    replace(encode(convert_to('cb_seq:' || sa.seq::text, 'utf8'), 'base64'), E'\\n', ''),
    FALSE,
    NULL::uuid,
    NULL::timestamptz,
    ${env.historicalImport ? "TRUE" : "FALSE"},
    ${env.suppressSideEffects ? "TRUE" : "FALSE"},
    ${sqlTextB64(env.outboxSourceKind)},
    ${sqlTimestamptzLiteral(env.receivedAtIso)},
    ${sqlTextB64(env.correlationId)},
    (${sqlTextB64(outboxBasePayloadJson)}::jsonb || jsonb_build_object(
      'conversation_id', im.conversation_id,
      'message_id', im.id,
      'message_seq', im.seq
    )),
    'PENDING',
    ${sqlTimestamptzLiteral(env.receivedAtIso)},
    0,
    ${sqlTimestamptzLiteral(env.receivedAtIso)}
  FROM ins_msg im
  JOIN seq_alloc sa ON TRUE
  ON CONFLICT (tenant_id, aggregate_key, aggregate_version, type) DO NOTHING
  RETURNING *
)${receiptCte},
new_result AS (
  SELECT json_build_object(
    'deduplicated', FALSE,
    'reopened_conversation', rc.reopened_conversation,
    'conversation_id', rc.id,
    'identity_id', rc.identity_id,
    'web_participant_id', rc.web_participant_id,
    'conversation_status', rc.status,
    'message_id', im.id,
    'message_seq', im.seq,
    'message_kind', im.kind,
    'message_body', im.body,
    'historical_import', im.historical_import,
    'parts', COALESCE((
      SELECT json_agg(json_build_object(
        'id', ip.id,
        'part_index', ip.part_index,
        'kind', ip.kind,
        'payload', ip.payload,
        'media_asset_id', ip.media_asset_id
      ) ORDER BY ip.part_index ASC)
      FROM ins_parts ip
    ), '[]'::json),
    'outbox', row_to_json(io)
  ) AS payload
  FROM resolved_conv rc
  JOIN ins_msg im ON im.conversation_id = rc.id
  JOIN ins_ref ir ON TRUE
  JOIN ins_outbox io ON TRUE
  LEFT JOIN clear_claim cc ON TRUE
  ${receiptJoin}
  LIMIT 1
),
dup_result AS (
  SELECT json_build_object(
    'deduplicated', TRUE,
    'reopened_conversation', FALSE,
    'conversation_id', c.id,
    'identity_id', c.identity_id,
    'web_participant_id', c.web_participant_id,
    'conversation_status', c.status,
    'message_id', m.id,
    'message_seq', m.seq,
    'message_kind', m.kind,
    'message_body', m.body,
    'historical_import', m.historical_import,
    'parts', COALESCE((
      SELECT json_agg(json_build_object(
        'id', mp.id,
        'part_index', mp.part_index,
        'kind', mp.kind,
        'payload', mp.payload,
        'media_asset_id', mp.media_asset_id
      ) ORDER BY mp.part_index ASC)
      FROM message_parts mp
      WHERE mp.tenant_id = m.tenant_id AND mp.message_id = m.id
    ), '[]'::json),
    'outbox', (
      SELECT row_to_json(o)
      FROM outbox_events o
      WHERE o.tenant_id = m.tenant_id
        AND o.aggregate_key = ('conv:' || m.conversation_id::text)
        AND o.aggregate_version = m.seq
        AND o.type = 'message.created'
      LIMIT 1
    )
  ) AS payload
  FROM existing_ref er
  JOIN messages m ON m.tenant_id = er.tenant_id AND m.id = er.message_id
  JOIN conversations c ON c.tenant_id = m.tenant_id AND c.id = m.conversation_id
  LIMIT 1
)
SELECT COALESCE((SELECT payload::text FROM new_result), (SELECT payload::text FROM dup_result), 'null');
${crashStmt}
COMMIT;`;

    let rawOut: string;
    try {
      rawOut = await runPsqlStdin(this.pgEnv, sql);
    } catch (error) {
      throw new NormalizationError(
        "DEPENDENCY_UNAVAILABLE",
        `PostgreSQL normalization commit failed: ${sanitizeSafeErrorMessage(error)}`,
        503
      );
    }

    const line = rawOut
      .split(/\r?\n/)
      .map((l) => l.trim())
      .filter((l) => l.length > 0)
      .at(-1);
    if (!line || line === "null") {
      throw new NormalizationError(
        "DEPENDENCY_UNAVAILABLE",
        "Normalization transaction returned empty persistence payload",
        503
      );
    }

    return mapPersistResponse(JSON.parse(line) as RawPersistResponse);
  }
}
