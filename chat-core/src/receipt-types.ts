import { createHash, createHmac, timingSafeEqual } from "node:crypto";

export type IngressProvider = "line_oa" | "tiktok" | "web_chat";

export type ReceiptDedupStrategy =
  | "PROVIDER_EVENT_ID"
  | "PROVIDER_MESSAGE_ID"
  | "WEB_CHAT_CLIENT_MESSAGE_ID"
  | "CANONICAL_BODY_FALLBACK";

export type ReceiptState =
  | "RECEIVED"
  | "VERIFIED"
  | "PROCESSED"
  | "REJECTED"
  | "FAILED";

export type IngressErrorCode =
  | "VALIDATION_ERROR"
  | "UNAUTHENTICATED"
  | "FORBIDDEN"
  | "DEPENDENCY_UNAVAILABLE";

export interface ChannelAccountBinding {
  readonly tenantId: string;
  readonly channelAccountId: string;
  readonly provider: IngressProvider;
  readonly connectionStatus:
    | "PENDING"
    | "CONNECTED"
    | "DEGRADED"
    | "DISCONNECTED"
    | "REVOKED";
  readonly signingSecret: string;
  readonly allowedWebOrigins?: readonly string[];
}

export interface NormalizedInboundEventInput {
  readonly eventKind: string;
  readonly providerEventId?: string | null;
  readonly providerMessageId?: string | null;
  readonly clientMessageId?: string | null;
  readonly payload: Readonly<Record<string, unknown>>;
}

export interface PersistReceiptRecordInput {
  readonly id: string;
  readonly tenantId: string;
  readonly channelAccountId: string;
  readonly provider: IngressProvider;
  readonly eventKind: string;
  readonly scopedProviderKey: string | null;
  readonly providerEventId: string | null;
  readonly providerMessageId: string | null;
  readonly canonicalDedupKey: string;
  readonly dedupStrategy: ReceiptDedupStrategy;
  readonly bodyHash: string;
  readonly signatureVerified: boolean;
  readonly correlationId: string | null;
  readonly payloadRedacted: Readonly<Record<string, unknown>>;
  readonly receivedAtIso: string;
}

export interface StoredWebhookReceipt {
  readonly id: string;
  readonly tenantId: string;
  readonly channelAccountId: string;
  readonly provider: IngressProvider;
  readonly eventKind: string;
  readonly scopedProviderKey: string | null;
  readonly providerEventId: string | null;
  readonly providerMessageId: string | null;
  readonly canonicalDedupKey: string;
  readonly dedupStrategy: ReceiptDedupStrategy;
  readonly bodyHash: string;
  readonly state: ReceiptState;
  readonly signatureVerified: boolean;
  readonly correlationId: string | null;
  readonly receivedAt: string;
}

export interface PersistReceiptOutcome {
  readonly receipt: StoredWebhookReceipt;
  readonly deduplicated: boolean;
}

export interface WebhookReceiptAckEnvelope {
  readonly httpStatus: 202;
  readonly data: {
    readonly receipt_id: string;
    readonly receipt_ids: readonly string[];
    readonly receipts: ReadonlyArray<{
      readonly receipt_id: string;
      readonly deduplicated: boolean;
      readonly dedup_strategy: ReceiptDedupStrategy;
    }>;
    readonly provider: IngressProvider;
    readonly persisted: true;
    readonly deduplicated: boolean;
    readonly history_sync_satisfied: false;
  };
  readonly meta: {
    readonly request_id: string;
    readonly server_time: string;
    readonly contract_version: "1.0.0";
  };
}

export interface WebhookErrorEnvelope {
  readonly httpStatus: 400 | 401 | 403 | 503;
  readonly persisted: false;
  readonly error: {
    readonly code: IngressErrorCode;
    readonly message_safe: string;
    readonly retryable: boolean;
    readonly details_safe?: Readonly<Record<string, unknown>>;
  };
  readonly meta: {
    readonly request_id: string;
    readonly server_time: string;
    readonly contract_version: "1.0.0";
  };
}

export type WebhookIngressResult = WebhookReceiptAckEnvelope | WebhookErrorEnvelope;

const VOLATILE_DELIVERY_KEYS = new Set([
  "deliveryContext",
  "isRedelivery",
  "retryCount",
  "attempt",
  "redelivered",
  "webhookDeliveryAttempt",
]);

function normalizeId(value: string | null | undefined): string | null {
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

/** Strips delivery-retry noise and sorts keys deeply so reordered redeliveries hash identically. */
export function canonicalizeForHash(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(canonicalizeForHash);
  }
  if (value !== null && typeof value === "object") {
    const out: Record<string, unknown> = {};
    const entries = Object.entries(value as Record<string, unknown>)
      .filter(([k]) => !VOLATILE_DELIVERY_KEYS.has(k))
      .sort(([a], [b]) => a.localeCompare(b));
    for (const [key, entryVal] of entries) {
      out[key] = canonicalizeForHash(entryVal);
    }
    return out;
  }
  return value;
}

export function sha256Hex(value: string | Buffer): string {
  return createHash("sha256").update(value).digest("hex");
}

export function computeScopedDedupIdentity(
  event: NormalizedInboundEventInput
): {
  readonly canonicalDedupKey: string;
  readonly dedupStrategy: ReceiptDedupStrategy;
  readonly scopedProviderKey: string | null;
  readonly providerEventId: string | null;
  readonly providerMessageId: string | null;
  readonly eventBodyHash: string;
} {
  const eventKind = normalizeId(event.eventKind) ?? "message.inbound";
  const providerEventId = normalizeId(event.providerEventId);
  const providerMessageId = normalizeId(event.providerMessageId);
  const clientMessageId = normalizeId(event.clientMessageId);

  const canonicalPayloadJson = JSON.stringify(canonicalizeForHash({
    eventKind,
    payload: event.payload,
  }));
  const eventBodyHash = sha256Hex(canonicalPayloadJson);

  if (providerEventId !== null) {
    const scopedProviderKey = `evt:${eventKind}:${providerEventId}`;
    return {
      canonicalDedupKey: scopedProviderKey,
      dedupStrategy: "PROVIDER_EVENT_ID",
      scopedProviderKey,
      providerEventId,
      providerMessageId,
      eventBodyHash,
    };
  }

  if (providerMessageId !== null) {
    const scopedProviderKey = `msg:${eventKind}:${providerMessageId}`;
    return {
      canonicalDedupKey: scopedProviderKey,
      dedupStrategy: "PROVIDER_MESSAGE_ID",
      scopedProviderKey,
      providerEventId: null,
      providerMessageId,
      eventBodyHash,
    };
  }

  if (clientMessageId !== null) {
    const scopedProviderKey = `wc:${eventKind}:${clientMessageId}`;
    return {
      canonicalDedupKey: scopedProviderKey,
      dedupStrategy: "WEB_CHAT_CLIENT_MESSAGE_ID",
      scopedProviderKey,
      providerEventId: null,
      providerMessageId: null,
      eventBodyHash,
    };
  }

  return {
    canonicalDedupKey: `body:${eventKind}:${eventBodyHash}`,
    dedupStrategy: "CANONICAL_BODY_FALLBACK",
    scopedProviderKey: null,
    providerEventId: null,
    providerMessageId: null,
    eventBodyHash,
  };
}

function safeEqualBuffers(a: Buffer, b: Buffer): boolean {
  if (a.length === 0 || a.length !== b.length) {
    return false;
  }
  return timingSafeEqual(a, b);
}

export function verifyHmacWebhookSignature(
  provider: "line_oa" | "tiktok",
  rawBody: Buffer | string,
  signatureHeader: string | undefined,
  signingSecret: string
): boolean {
  if (typeof signatureHeader !== "string") {
    return false;
  }
  const trimmed = signatureHeader.trim();
  if (!trimmed || !signingSecret) {
    return false;
  }

  const mac = createHmac("sha256", signingSecret).update(rawBody);
  if (provider === "line_oa") {
    const expected = mac.digest();
    const provided = Buffer.from(trimmed, "base64");
    return safeEqualBuffers(expected, provided);
  }

  const normalizedHex = trimmed.startsWith("sha256=")
    ? trimmed.slice("sha256=".length)
    : trimmed;
  if (!/^[0-9a-f]{64}$/i.test(normalizedHex)) {
    return false;
  }
  const expected = mac.digest();
  const provided = Buffer.from(normalizedHex, "hex");
  return safeEqualBuffers(expected, provided);
}

export function buildRedactedReceiptMetadata(
  event: NormalizedInboundEventInput,
  dedupStrategy: ReceiptDedupStrategy
): Readonly<Record<string, unknown>> {
  return Object.freeze({
    event_kind: event.eventKind,
    dedup_strategy: dedupStrategy,
    has_provider_event_id: normalizeId(event.providerEventId) !== null,
    has_provider_message_id: normalizeId(event.providerMessageId) !== null,
    payload_keys: Object.keys(event.payload).sort(),
    redacted: true,
  });
}
