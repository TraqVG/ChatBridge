import { createHash, randomUUID } from "node:crypto";
import {
  assertUuid,
  validatePayloadSafety,
  type OutboxSourceKind,
} from "../../queue-and-retry/src/outbox-types.js";
import {
  NormalizationError,
  type CanonicalInboundInput,
  type MessagePartKind,
  type NormalizedMediaAsset,
  type NormalizedMessageEnvelope,
  type NormalizedMessagePart,
  type RawInboundPartInput,
} from "./types.js";

function assertSafeStorageKey(
  value: string | null | undefined,
  field: string
): string | null {
  if (value === undefined || value === null || value === "") {
    return null;
  }
  if (/^https?:\/\//i.test(value) || value.includes("?") || value.includes("X-Amz-Signature")) {
    throw new NormalizationError(
      "VALIDATION_ERROR",
      `${field} must be an opaque storage key, never a URL or presigned query`
    );
  }
  return value;
}

function buildUnsupportedPlaceholder(
  partIndex: number,
  originalKind: string,
  reasonCode: string,
  labelSafe: string,
  extraMeta: Readonly<Record<string, unknown>> = {}
): NormalizedMessagePart {
  return Object.freeze({
    id: randomUUID(),
    partIndex,
    kind: "UNSUPPORTED",
    payload: Object.freeze({
      placeholder: true,
      availability: "UNAVAILABLE",
      original_kind: originalKind,
      reason_code: reasonCode,
      label_safe: labelSafe,
      ...extraMeta,
    }),
    mediaAsset: null,
  });
}

function isSafeHttpUrl(raw: string): boolean {
  try {
    const u = new URL(raw);
    return u.protocol === "https:" || u.protocol === "http:";
  } catch {
    return false;
  }
}

function normalizeSinglePart(
  raw: RawInboundPartInput,
  partIndex: number
): NormalizedMessagePart {
  switch (raw.kind) {
    case "TEXT": {
      return Object.freeze({
        id: randomUUID(),
        partIndex,
        kind: "TEXT",
        payload: Object.freeze({ text: raw.text }),
        mediaAsset: null,
      });
    }

    case "LINK": {
      if (raw.unavailable || !isSafeHttpUrl(raw.url)) {
        return buildUnsupportedPlaceholder(
          partIndex,
          "LINK",
          raw.unavailable ? (raw.unavailableReason ?? "LINK_UNAVAILABLE") : "UNSAFE_OR_INVALID_LINK",
          "Link preview unavailable"
        );
      }
      return Object.freeze({
        id: randomUUID(),
        partIndex,
        kind: "LINK",
        payload: Object.freeze({
          url: raw.url,
          title: raw.title ?? null,
          preview_state: "PENDING_ASYNC",
        }),
        mediaAsset: null,
      });
    }

    case "STICKER": {
      const storageKey = assertSafeStorageKey(raw.storageObjectKey, "storageObjectKey");
      if (raw.unavailable) {
        return buildUnsupportedPlaceholder(
          partIndex,
          "STICKER",
          raw.unavailableReason ?? "STICKER_MEDIA_UNAVAILABLE",
          "Sticker unavailable",
          {
            package_id: raw.packageId ?? null,
            sticker_id: raw.stickerId ?? null,
          }
        );
      }
      let mediaAsset: NormalizedMediaAsset | null = null;
      if (storageKey) {
        mediaAsset = Object.freeze({
          id: randomUUID(),
          contentType: "image/png",
          bytes: 0,
          sha256Hex: null,
          providerRef: raw.stickerId ?? null,
          storageProvider: "OMEGA_SEAWEEDFS",
          storageObjectKey: storageKey,
          thumbnailKey: null,
          ingestState: "STORED",
          quarantineState: "PENDING",
        });
      }
      return Object.freeze({
        id: randomUUID(),
        partIndex,
        kind: "STICKER",
        payload: Object.freeze({
          package_id: raw.packageId ?? null,
          sticker_id: raw.stickerId ?? null,
          resource_type: raw.resourceType ?? "STATIC",
          ...(storageKey !== null ? { storage_object_key: storageKey } : {}),
        }),
        mediaAsset,
      });
    }

    case "IMAGE":
    case "VIDEO":
    case "AUDIO":
    case "FILE": {
      const storageKey = assertSafeStorageKey(raw.storageObjectKey, "storageObjectKey");
      const thumbKey = assertSafeStorageKey(raw.thumbnailKey, "thumbnailKey");
      const providerRef = raw.providerRef ?? null;

      if (
        raw.unavailable ||
        (storageKey === null && (providerRef === null || providerRef.trim().length === 0))
      ) {
        return buildUnsupportedPlaceholder(
          partIndex,
          raw.kind,
          raw.unavailableReason ?? "MEDIA_UNAVAILABLE",
          `${raw.kind.toLowerCase()} unavailable`,
          raw.filename ? { filename_safe: raw.filename.slice(0, 120) } : {}
        );
      }

      const bytes = raw.bytes ?? 0;
      if (!Number.isInteger(bytes) || bytes < 0) {
        return buildUnsupportedPlaceholder(
          partIndex,
          raw.kind,
          "INVALID_MEDIA_METADATA",
          `${raw.kind.toLowerCase()} metadata unavailable`
        );
      }

      const assetId = randomUUID();
      const mediaAsset: NormalizedMediaAsset = Object.freeze({
        id: assetId,
        contentType: raw.contentType ?? "application/octet-stream",
        bytes,
        sha256Hex: raw.sha256Hex ?? null,
        providerRef,
        storageProvider: "OMEGA_SEAWEEDFS",
        storageObjectKey: storageKey,
        thumbnailKey: thumbKey,
        ingestState: storageKey ? "STORED" : "PENDING",
        quarantineState: "PENDING",
      });

      return Object.freeze({
        id: randomUUID(),
        partIndex,
        kind: raw.kind,
        payload: Object.freeze({
          content_type: mediaAsset.contentType,
          bytes: mediaAsset.bytes,
          availability: storageKey ? "AVAILABLE" : "PENDING_INGEST",
          ...(raw.filename ? { filename_safe: raw.filename.slice(0, 120) } : {}),
          ...(storageKey !== null ? { storage_object_key: storageKey } : {}),
          ...(thumbKey !== null ? { thumbnail_key: thumbKey } : {}),
        }),
        mediaAsset,
      });
    }

    case "UNSUPPORTED":
      return buildUnsupportedPlaceholder(
        partIndex,
        raw.originalKind ?? "UNSUPPORTED",
        raw.reasonCode ?? "UNSUPPORTED_CONTENT",
        raw.labelSafe ?? "Unsupported media unavailable"
      );

    case "RAW_PROVIDER_UNSUPPORTED":
      return buildUnsupportedPlaceholder(
        partIndex,
        raw.rawType,
        raw.reasonCode ?? "PROVIDER_TYPE_UNSUPPORTED",
        raw.labelSafe ?? `Unsupported ${raw.rawType} content unavailable`
      );
  }
}

function defaultOutboxSourceKind(sourceKind: CanonicalInboundInput["sourceKind"]): OutboxSourceKind {
  switch (sourceKind) {
    case "WEBHOOK":
      return "LIVE_WEBHOOK";
    case "OUTBOUND":
      return "LIVE_API";
    case "HISTORY_SYNC":
      return "INITIAL_BACKFILL";
    case "MANUAL_IMPORT":
      return "CSV_IMPORT";
  }
}

function deriveSummaryBody(parts: readonly NormalizedMessagePart[]): string {
  const texts: string[] = [];
  for (const p of parts) {
    if (p.kind === "TEXT" && typeof p.payload.text === "string" && p.payload.text.length > 0) {
      texts.push(p.payload.text);
    } else if (p.kind === "LINK" && typeof p.payload.url === "string") {
      texts.push(p.payload.url);
    } else if (p.kind === "UNSUPPORTED") {
      texts.push(
        typeof p.payload.label_safe === "string"
          ? `[${p.payload.label_safe}]`
          : "[Unsupported media unavailable]"
      );
    } else {
      texts.push(`[${p.kind.toLowerCase()}]`);
    }
  }
  return texts.join(" ").slice(0, 1000);
}

function computeSourceFingerprint(input: CanonicalInboundInput, senderScopeKey: string, parts: readonly NormalizedMessagePart[]): string {
  if (input.providerMessageId && input.providerMessageId.trim().length > 0) {
    return `pmsg:${input.providerMessageId.trim()}`;
  }
  if (input.clientMessageId && input.clientMessageId.trim().length > 0) {
    return `cmsg:${input.clientMessageId.trim()}`;
  }
  if (input.providerEventId && input.providerEventId.trim().length > 0) {
    return `pevt:${input.providerEventId.trim()}`;
  }
  const canonicalFallback = JSON.stringify({
    provider: input.provider,
    senderScopeKey,
    receivedAtIso: input.receivedAtIso ?? "",
    parts: parts.map((p) => ({ kind: p.kind, payload: p.payload })),
  });
  return `body:${createHash("sha256").update(canonicalFallback).digest("hex")}`;
}

export function normalizeInboundMessage(
  input: CanonicalInboundInput
): NormalizedMessageEnvelope {
  assertUuid(input.tenantId, "tenantId");
  assertUuid(input.channelAccountId, "channelAccountId");
  if (input.sender.identityId) {
    assertUuid(input.sender.identityId, "sender.identityId");
  }
  if (input.sender.webParticipantId) {
    assertUuid(input.sender.webParticipantId, "sender.webParticipantId");
  }
  if (input.conversationId) {
    assertUuid(input.conversationId, "conversationId");
  }
  if (input.webhookReceiptId) {
    assertUuid(input.webhookReceiptId, "webhookReceiptId");
  }
  if (input.correlationId.trim().length < 8) {
    throw new NormalizationError(
      "VALIDATION_ERROR",
      "correlationId must be at least 8 characters"
    );
  }

  const providerSenderRef = input.sender.providerSenderRef?.trim() || null;
  const webParticipantId = input.sender.webParticipantId ?? null;
  const explicitIdentityId = input.sender.identityId ?? null;

  if (!explicitIdentityId && !providerSenderRef && !webParticipantId) {
    throw new NormalizationError(
      "VALIDATION_ERROR",
      "At least one scoped sender identifier (identityId, providerSenderRef, or webParticipantId) is required"
    );
  }

  const senderScopeKey = explicitIdentityId
    ? `ident:${explicitIdentityId}`
    : webParticipantId
      ? `web:${webParticipantId}`
      : `${input.channelAccountId}:${providerSenderRef}`;

  const historicalImport =
    input.historicalImport ??
    (input.sourceKind === "HISTORY_SYNC" || input.sourceKind === "MANUAL_IMPORT");
  const suppressSideEffects = input.suppressSideEffects ?? historicalImport;
  if (historicalImport && !suppressSideEffects) {
    throw new NormalizationError(
      "VALIDATION_ERROR",
      "historicalImport=true requires suppressSideEffects=true"
    );
  }

  const rawParts: readonly RawInboundPartInput[] =
    input.parts.length > 0
      ? input.parts
      : [
          {
            kind: "UNSUPPORTED",
            originalKind: "EMPTY",
            reasonCode: "EMPTY_MESSAGE_PARTS",
            labelSafe: "Empty message unavailable",
          },
        ];

  const parts = Object.freeze(
    rawParts.map((part, idx) => normalizeSinglePart(part, idx))
  );

  const firstKind: MessagePartKind = parts[0]?.kind ?? "UNSUPPORTED";
  const allUnsupported = parts.every((p) => p.kind === "UNSUPPORTED");
  const hasUnsupportedPlaceholder = parts.some((p) => p.kind === "UNSUPPORTED");
  const messageKind: MessagePartKind = allUnsupported ? "UNSUPPORTED" : firstKind;
  const body = deriveSummaryBody(parts);
  const receivedAtIso = input.receivedAtIso ?? new Date().toISOString();
  const outboxSourceKind =
    input.outboxSourceKind ?? defaultOutboxSourceKind(input.sourceKind);
  const direction = input.direction ?? "IN";
  const senderKind =
    input.senderKind ??
    (direction === "IN"
      ? input.provider === "web_chat"
        ? "WEB_GUEST"
        : "CUSTOMER"
      : "STAFF");

  const primaryStorageKey =
    parts.find((p) => p.mediaAsset?.storageObjectKey)?.mediaAsset?.storageObjectKey ?? null;

  const outboxPayloadMinimal: Record<string, unknown> = {
    provider: input.provider,
    direction,
    sender_kind: senderKind,
    message_kind: messageKind,
    part_kinds: parts.map((p) => p.kind),
    part_count: parts.length,
    has_unsupported_placeholder: hasUnsupportedPlaceholder,
    ...(primaryStorageKey !== null ? { storage_object_key: primaryStorageKey } : {}),
  };
  validatePayloadSafety(outboxPayloadMinimal);

  const sourceFingerprint = computeSourceFingerprint(input, senderScopeKey, parts);

  return Object.freeze({
    tenantId: input.tenantId,
    channelAccountId: input.channelAccountId,
    provider: input.provider,
    sourceKind: input.sourceKind,
    outboxSourceKind,
    historicalImport,
    suppressSideEffects,
    direction,
    senderKind,
    senderRef: providerSenderRef ?? webParticipantId ?? explicitIdentityId,
    explicitIdentityId,
    providerSenderRef,
    webParticipantId,
    senderScopeKey,
    senderDisplayName: input.sender.displayName ?? null,
    conversationIdOverride: input.conversationId ?? null,
    webhookReceiptId: input.webhookReceiptId ?? null,
    messageKind,
    body,
    parts,
    sourceFingerprint,
    receivedAtIso,
    providerAtIso: input.providerAtIso ?? null,
    correlationId: input.correlationId,
    hasUnsupportedPlaceholder,
    outboxPayloadMinimal: Object.freeze(outboxPayloadMinimal),
  });
}
