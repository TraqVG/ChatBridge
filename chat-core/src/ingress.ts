import { randomUUID } from "node:crypto";
import {
  buildRedactedReceiptMetadata,
  computeScopedDedupIdentity,
  verifyHmacWebhookSignature,
  type ChannelAccountBinding,
  type IngressProvider,
  type NormalizedInboundEventInput,
  type PersistReceiptRecordInput,
  type WebhookErrorEnvelope,
  type WebhookIngressResult,
} from "./receipt-types.js";
import type { ReceiptStore } from "./receipt-store.js";

export * from "./receipt-types.js";
export * from "./receipt-store.js";

export interface ProviderWebhookRequest {
  readonly provider: "line_oa" | "tiktok";
  readonly tenantId: string;
  readonly channelAccountId: string;
  readonly rawBody: string | Buffer;
  readonly signatureHeader: string | undefined;
  readonly events: readonly NormalizedInboundEventInput[];
  readonly requestId?: string;
  readonly correlationId?: string | null;
}

export interface WebChatGuestInboundRequest {
  readonly tenantId: string;
  readonly channelAccountId: string;
  readonly siteId: string;
  readonly origin: string | undefined;
  readonly guestSessionToken: string | undefined;
  readonly verifiedGuestParticipantId: string | null;
  readonly rawBody: string | Buffer;
  readonly event: NormalizedInboundEventInput;
  readonly requestId?: string;
  readonly correlationId?: string | null;
}

export interface AccountBindingResolver {
  resolveAccountBinding(
    tenantId: string,
    channelAccountId: string,
    provider: IngressProvider
  ): Promise<ChannelAccountBinding | null>;
}

export interface GuestSessionVerifier {
  verifyGuestSession(params: {
    readonly tenantId: string;
    readonly channelAccountId: string;
    readonly siteId: string;
    readonly origin: string | undefined;
    readonly guestSessionToken: string | undefined;
  }): Promise<{ readonly valid: boolean; readonly reason?: string }>;
}

function buildErrorEnvelope(
  httpStatus: 400 | 401 | 403 | 503,
  code: WebhookErrorEnvelope["error"]["code"],
  messageSafe: string,
  retryable: boolean,
  requestId: string,
  nowIso: string,
  detailsSafe?: Readonly<Record<string, unknown>>
): WebhookErrorEnvelope {
  return {
    httpStatus,
    persisted: false,
    error: detailsSafe
      ? {
          code,
          message_safe: messageSafe,
          retryable,
          details_safe: detailsSafe,
        }
      : {
          code,
          message_safe: messageSafe,
          retryable,
        },
    meta: {
      request_id: requestId,
      server_time: nowIso,
      contract_version: "1.0.0",
    },
  };
}

/**
 * Authenticated ingress boundary persisting durable, scoped webhook/guest
 * receipts before returning HTTP 202. DB failures always reject with 503 and
 * never emit durable success.
 */
export class WebhookIngressService {
  private readonly store: ReceiptStore;
  private readonly resolver: AccountBindingResolver;
  private readonly guestVerifier?: GuestSessionVerifier;
  private readonly nowFn: () => Date;

  constructor(options: {
    readonly store: ReceiptStore;
    readonly resolver: AccountBindingResolver;
    readonly guestVerifier?: GuestSessionVerifier;
    readonly nowFn?: () => Date;
  }) {
    this.store = options.store;
    this.resolver = options.resolver;
    if (options.guestVerifier) {
      this.guestVerifier = options.guestVerifier;
    }
    this.nowFn = options.nowFn ?? (() => new Date());
  }

  async ingestProviderWebhook(
    req: ProviderWebhookRequest
  ): Promise<WebhookIngressResult> {
    const requestId = req.requestId?.trim() || `req_${randomUUID()}`;
    const nowIso = this.nowFn().toISOString();

    if (!Array.isArray(req.events) || req.events.length === 0) {
      return buildErrorEnvelope(
        400,
        "VALIDATION_ERROR",
        "Webhook payload must include at least one inbound event.",
        false,
        requestId,
        nowIso
      );
    }

    const binding = await this.resolver.resolveAccountBinding(
      req.tenantId,
      req.channelAccountId,
      req.provider
    );
    if (
      !binding ||
      binding.provider !== req.provider ||
      binding.connectionStatus === "REVOKED" ||
      binding.connectionStatus === "DISCONNECTED"
    ) {
      return buildErrorEnvelope(
        403,
        "FORBIDDEN",
        "Channel account binding is missing, mismatched, or revoked.",
        false,
        requestId,
        nowIso
      );
    }

    const sigValid = verifyHmacWebhookSignature(
      req.provider,
      req.rawBody,
      req.signatureHeader,
      binding.signingSecret
    );
    if (!sigValid) {
      return buildErrorEnvelope(
        401,
        "UNAUTHENTICATED",
        "Provider webhook signature verification failed.",
        false,
        requestId,
        nowIso
      );
    }

    return this.persistAndAck({
      provider: req.provider,
      tenantId: binding.tenantId,
      channelAccountId: binding.channelAccountId,
      events: req.events,
      correlationId: req.correlationId ?? requestId,
      requestId,
      nowIso,
    });
  }

  async ingestWebChatGuestInbound(
    req: WebChatGuestInboundRequest
  ): Promise<WebhookIngressResult> {
    const requestId = req.requestId?.trim() || `req_${randomUUID()}`;
    const nowIso = this.nowFn().toISOString();

    const binding = await this.resolver.resolveAccountBinding(
      req.tenantId,
      req.channelAccountId,
      "web_chat"
    );
    if (
      !binding ||
      binding.provider !== "web_chat" ||
      binding.connectionStatus === "REVOKED" ||
      binding.connectionStatus === "DISCONNECTED"
    ) {
      return buildErrorEnvelope(
        403,
        "FORBIDDEN",
        "Web Chat channel account is missing or revoked.",
        false,
        requestId,
        nowIso
      );
    }

    if (
      binding.allowedWebOrigins &&
      binding.allowedWebOrigins.length > 0 &&
      (!req.origin || !binding.allowedWebOrigins.includes(req.origin))
    ) {
      return buildErrorEnvelope(
        403,
        "FORBIDDEN",
        "Request origin is not allowlisted for this Web Chat site.",
        false,
        requestId,
        nowIso
      );
    }

    if (!this.guestVerifier || !req.verifiedGuestParticipantId) {
      return buildErrorEnvelope(
        401,
        "UNAUTHENTICATED",
        "Verified Web Chat guest session is required.",
        false,
        requestId,
        nowIso
      );
    }

    const guestAuth = await this.guestVerifier.verifyGuestSession({
      tenantId: req.tenantId,
      channelAccountId: req.channelAccountId,
      siteId: req.siteId,
      origin: req.origin,
      guestSessionToken: req.guestSessionToken,
    });
    if (!guestAuth.valid) {
      return buildErrorEnvelope(
        401,
        "UNAUTHENTICATED",
        "Guest session token is missing, expired, or revoked.",
        false,
        requestId,
        nowIso
      );
    }

    return this.persistAndAck({
      provider: "web_chat",
      tenantId: binding.tenantId,
      channelAccountId: binding.channelAccountId,
      events: [req.event],
      correlationId: req.correlationId ?? requestId,
      requestId,
      nowIso,
    });
  }

  private async persistAndAck(params: {
    readonly provider: IngressProvider;
    readonly tenantId: string;
    readonly channelAccountId: string;
    readonly events: readonly NormalizedInboundEventInput[];
    readonly correlationId: string;
    readonly requestId: string;
    readonly nowIso: string;
  }): Promise<WebhookIngressResult> {
    const inputs: PersistReceiptRecordInput[] = params.events.map((event) => {
      const identity = computeScopedDedupIdentity(event);
      return {
        id: randomUUID(),
        tenantId: params.tenantId,
        channelAccountId: params.channelAccountId,
        provider: params.provider,
        eventKind: event.eventKind.trim() || "message.inbound",
        scopedProviderKey: identity.scopedProviderKey,
        providerEventId: identity.providerEventId,
        providerMessageId: identity.providerMessageId,
        canonicalDedupKey: identity.canonicalDedupKey,
        dedupStrategy: identity.dedupStrategy,
        bodyHash: identity.eventBodyHash,
        signatureVerified: true,
        correlationId: params.correlationId,
        payloadRedacted: buildRedactedReceiptMetadata(event, identity.dedupStrategy),
        receivedAtIso: params.nowIso,
      };
    });

    let outcomes;
    try {
      outcomes = await this.store.persistVerifiedBatch(inputs);
    } catch {
      return buildErrorEnvelope(
        503,
        "DEPENDENCY_UNAVAILABLE",
        "Durable webhook receipt persistence failed before commit; retry later.",
        true,
        params.requestId,
        params.nowIso
      );
    }

    if (outcomes.length !== inputs.length) {
      return buildErrorEnvelope(
        503,
        "DEPENDENCY_UNAVAILABLE",
        "Durable receipt commit verification mismatch; retry later.",
        true,
        params.requestId,
        params.nowIso
      );
    }

    const primary = outcomes[0]!;
    return {
      httpStatus: 202,
      data: {
        receipt_id: primary.receipt.id,
        receipt_ids: outcomes.map((o) => o.receipt.id),
        receipts: outcomes.map((o) => ({
          receipt_id: o.receipt.id,
          deduplicated: o.deduplicated,
          dedup_strategy: o.receipt.dedupStrategy,
        })),
        provider: params.provider,
        persisted: true,
        deduplicated: outcomes.every((o) => o.deduplicated),
        history_sync_satisfied: false,
      },
      meta: {
        request_id: params.requestId,
        server_time: params.nowIso,
        contract_version: "1.0.0",
      },
    };
  }
}
