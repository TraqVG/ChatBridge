import type { IngressProvider } from "../../chat-core/src/receipt-types.js";
import type { OutboxSourceKind, StoredOutboxRecord } from "../../queue-and-retry/src/outbox-types.js";

export type MessageDirection = "IN" | "OUT";

export type MessageSenderKind =
  | "CUSTOMER"
  | "WEB_GUEST"
  | "STAFF"
  | "ADMIN"
  | "OWNER"
  | "AI"
  | "SYSTEM";

export type MessageSourceKind =
  | "WEBHOOK"
  | "HISTORY_SYNC"
  | "OUTBOUND"
  | "MANUAL_IMPORT";

export type MessagePartKind =
  | "TEXT"
  | "STICKER"
  | "IMAGE"
  | "FILE"
  | "VIDEO"
  | "AUDIO"
  | "LINK"
  | "UNSUPPORTED";

export class NormalizationError extends Error {
  readonly code: "VALIDATION_ERROR" | "STATE_CONFLICT" | "DEPENDENCY_UNAVAILABLE";
  readonly status: 400 | 409 | 503;

  constructor(
    code: "VALIDATION_ERROR" | "STATE_CONFLICT" | "DEPENDENCY_UNAVAILABLE",
    message: string,
    status?: 400 | 409 | 503
  ) {
    super(message);
    this.name = "NormalizationError";
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

export type RawInboundPartInput =
  | { readonly kind: "TEXT"; readonly text: string }
  | {
      readonly kind: "IMAGE" | "VIDEO" | "AUDIO" | "FILE";
      readonly contentType?: string;
      readonly bytes?: number;
      readonly sha256Hex?: string | null;
      readonly providerRef?: string | null;
      readonly storageObjectKey?: string | null;
      readonly thumbnailKey?: string | null;
      readonly filename?: string | null;
      readonly unavailable?: boolean;
      readonly unavailableReason?: string;
    }
  | {
      readonly kind: "STICKER";
      readonly packageId?: string | null;
      readonly stickerId?: string | null;
      readonly resourceType?: string | null;
      readonly storageObjectKey?: string | null;
      readonly unavailable?: boolean;
      readonly unavailableReason?: string;
    }
  | {
      readonly kind: "LINK";
      readonly url: string;
      readonly title?: string | null;
      readonly unavailable?: boolean;
      readonly unavailableReason?: string;
    }
  | {
      readonly kind: "UNSUPPORTED";
      readonly originalKind?: string;
      readonly reasonCode?: string;
      readonly labelSafe?: string;
    }
  | {
      readonly kind: "RAW_PROVIDER_UNSUPPORTED";
      readonly rawType: string;
      readonly reasonCode?: string;
      readonly labelSafe?: string;
    };

export interface SenderIdentityInput {
  readonly identityId?: string | null;
  readonly providerSenderRef?: string | null;
  readonly webParticipantId?: string | null;
  readonly displayName?: string | null;
}

export interface CanonicalInboundInput {
  readonly tenantId: string;
  readonly channelAccountId: string;
  readonly provider: IngressProvider;
  readonly sourceKind: MessageSourceKind;
  readonly outboxSourceKind?: OutboxSourceKind;
  readonly historicalImport?: boolean;
  readonly suppressSideEffects?: boolean;
  readonly direction?: MessageDirection;
  readonly senderKind?: MessageSenderKind;
  readonly sender: SenderIdentityInput;
  readonly providerEventId?: string | null;
  readonly providerMessageId?: string | null;
  readonly clientMessageId?: string | null;
  readonly webhookReceiptId?: string | null;
  readonly conversationId?: string | null;
  readonly receivedAtIso?: string;
  readonly providerAtIso?: string | null;
  readonly correlationId: string;
  readonly parts: readonly RawInboundPartInput[];
}

export interface NormalizedMediaAsset {
  readonly id: string;
  readonly contentType: string;
  readonly bytes: number;
  readonly sha256Hex: string | null;
  readonly providerRef: string | null;
  readonly storageProvider: "OMEGA_SEAWEEDFS";
  readonly storageObjectKey: string | null;
  readonly thumbnailKey: string | null;
  readonly ingestState: "PENDING" | "STORED" | "FAILED" | "DELETED";
  readonly quarantineState: "PENDING" | "CLEAN" | "QUARANTINED" | "BLOCKED";
}

export interface NormalizedMessagePart {
  readonly id: string;
  readonly partIndex: number;
  readonly kind: MessagePartKind;
  readonly payload: Readonly<Record<string, unknown>>;
  readonly mediaAsset: NormalizedMediaAsset | null;
}

export interface NormalizedMessageEnvelope {
  readonly tenantId: string;
  readonly channelAccountId: string;
  readonly provider: IngressProvider;
  readonly sourceKind: MessageSourceKind;
  readonly outboxSourceKind: OutboxSourceKind;
  readonly historicalImport: boolean;
  readonly suppressSideEffects: boolean;
  readonly direction: MessageDirection;
  readonly senderKind: MessageSenderKind;
  readonly senderRef: string | null;
  readonly explicitIdentityId: string | null;
  readonly providerSenderRef: string | null;
  readonly webParticipantId: string | null;
  readonly senderScopeKey: string;
  readonly senderDisplayName: string | null;
  readonly conversationIdOverride: string | null;
  readonly webhookReceiptId: string | null;
  readonly messageKind: MessagePartKind;
  readonly body: string;
  readonly parts: readonly NormalizedMessagePart[];
  readonly sourceFingerprint: string;
  readonly receivedAtIso: string;
  readonly providerAtIso: string | null;
  readonly correlationId: string;
  readonly hasUnsupportedPlaceholder: boolean;
  readonly outboxPayloadMinimal: Readonly<Record<string, unknown>>;
}

export interface PersistedMessageRecord {
  readonly conversationId: string;
  readonly identityId: string | null;
  readonly webParticipantId: string | null;
  readonly conversationStatus: "OPEN" | "RESOLVED";
  readonly messageId: string;
  readonly seq: number;
  readonly kind: MessagePartKind;
  readonly body: string | null;
  readonly historicalImport: boolean;
  readonly parts: readonly {
    readonly id: string;
    readonly partIndex: number;
    readonly kind: MessagePartKind;
    readonly payload: Readonly<Record<string, unknown>>;
    readonly mediaAssetId: string | null;
  }[];
  readonly outboxRecord: StoredOutboxRecord;
  readonly reopenedConversation: boolean;
}

export interface PersistNormalizedResult {
  readonly deduplicated: boolean;
  readonly message: PersistedMessageRecord;
}
