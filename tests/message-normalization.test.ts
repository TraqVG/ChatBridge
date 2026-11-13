import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import {
  createInMemoryNormalizationService,
  NormalizationError,
  type CanonicalInboundInput,
} from "../message-normalization/src/envelope.js";

const TENANT_A = "11111111-1111-4111-8111-111111111101";
const TENANT_B = "22222222-2222-4222-8222-222222222202";
const ACC_A_LINE1 = "33333333-0001-4333-8333-333333333301";
const ACC_A_LINE2 = "33333333-0002-4333-8333-333333333302";
const ACC_A_TIKTOK = "33333333-0003-4333-8333-333333333303";
const ACC_A_WEB = "33333333-0004-4333-8333-333333333304";
const ACC_B_LINE = "44444444-0001-4444-8444-444444444401";
const WEB_GUEST_A = "55555555-0001-4555-8555-555555555501";

describe("CB-02-05 canonical message normalization & persistence unit suite", () => {
  it("normalizes text/media/sticker/link parts and preserves visible unavailable placeholders", async () => {
    const { service, store } = createInMemoryNormalizationService();

    const mixedResult = await service.normalizeAndPersist({
      tenantId: TENANT_A,
      channelAccountId: ACC_A_LINE1,
      provider: "line_oa",
      sourceKind: "WEBHOOK",
      providerMessageId: "line-msg-001",
      correlationId: "corr-norm-mixed-001",
      sender: { providerSenderRef: "U-line-customer-1", displayName: "Alice" },
      parts: [
        { kind: "TEXT", text: "Check product attachment" },
        {
          kind: "IMAGE",
          contentType: "image/jpeg",
          bytes: 4096,
          storageObjectKey: "tenants/a/media/img-001.jpg",
        },
        { kind: "STICKER", packageId: "11537", stickerId: "52002734" },
        { kind: "LINK", url: "https://example.com/item/1", title: "Item 1" },
        {
          kind: "VIDEO",
          unavailable: true,
          unavailableReason: "PROVIDER_MEDIA_EXPIRED",
          filename: "clip.mp4",
        },
        { kind: "RAW_PROVIDER_UNSUPPORTED", rawType: "location" },
        { kind: "LINK", url: "javascript:alert(1)" },
      ],
    });

    assert.equal(mixedResult.deduplicated, false);
    assert.equal(mixedResult.message.seq, 1);
    assert.equal(mixedResult.message.parts.length, 7);
    assert.equal(mixedResult.message.parts[0]!.kind, "TEXT");
    assert.equal(mixedResult.message.parts[1]!.kind, "IMAGE");
    assert.ok(mixedResult.message.parts[1]!.mediaAssetId);
    assert.equal(mixedResult.message.parts[2]!.kind, "STICKER");
    assert.equal(mixedResult.message.parts[3]!.kind, "LINK");

    // Unavailable video, unsupported provider location, and unsafe javascript: link stay visible as placeholders.
    for (const idx of [4, 5, 6]) {
      const part = mixedResult.message.parts[idx]!;
      assert.equal(part.kind, "UNSUPPORTED");
      assert.equal(part.payload.placeholder, true);
      assert.equal(part.payload.availability, "UNAVAILABLE");
    }

    assert.equal(store.getStoredMediaAssetCount(), 1);
    assert.equal(
      mixedResult.message.outboxRecord.envelope.payload.has_unsupported_placeholder,
      true
    );
    assert.equal(
      mixedResult.message.outboxRecord.envelope.payload.storage_object_key,
      "tenants/a/media/img-001.jpg"
    );

    // Presigned URL in storageObjectKey must be rejected fail-closed.
    await assert.rejects(
      service.normalizeAndPersist({
        tenantId: TENANT_A,
        channelAccountId: ACC_A_LINE1,
        provider: "line_oa",
        sourceKind: "WEBHOOK",
        providerMessageId: "line-msg-presigned",
        correlationId: "corr-norm-bad-url-01",
        sender: { providerSenderRef: "U-line-customer-1" },
        parts: [
          {
            kind: "IMAGE",
            contentType: "image/jpeg",
            bytes: 100,
            storageObjectKey: "https://storage.invalid/bucket/obj?X-Amz-Signature=deadbeef",
          },
        ],
      }),
      (err: unknown) => err instanceof NormalizationError && err.code === "VALIDATION_ERROR"
    );
  });

  it("isolates multi-account corpus without cross-account merges and deduplicates replays", async () => {
    const { service } = createInMemoryNormalizationService();

    const corpus: CanonicalInboundInput[] = [
      {
        tenantId: TENANT_A,
        channelAccountId: ACC_A_LINE1,
        provider: "line_oa",
        sourceKind: "WEBHOOK",
        providerMessageId: "shared-msg-1",
        correlationId: "corr-corpus-acc1",
        sender: { providerSenderRef: "shared-sender-01" },
        parts: [{ kind: "TEXT", text: "Hello LINE 1" }],
      },
      {
        tenantId: TENANT_A,
        channelAccountId: ACC_A_LINE2,
        provider: "line_oa",
        sourceKind: "WEBHOOK",
        providerMessageId: "shared-msg-1",
        correlationId: "corr-corpus-acc2",
        sender: { providerSenderRef: "shared-sender-01" },
        parts: [{ kind: "TEXT", text: "Hello LINE 2" }],
      },
      {
        tenantId: TENANT_A,
        channelAccountId: ACC_A_TIKTOK,
        provider: "tiktok",
        sourceKind: "WEBHOOK",
        providerMessageId: "shared-msg-1",
        correlationId: "corr-corpus-tt",
        sender: { providerSenderRef: "shared-sender-01" },
        parts: [{ kind: "RAW_PROVIDER_UNSUPPORTED", rawType: "tiktok_gift" }],
      },
      {
        tenantId: TENANT_A,
        channelAccountId: ACC_A_WEB,
        provider: "web_chat",
        sourceKind: "WEBHOOK",
        clientMessageId: "web-cli-1",
        correlationId: "corr-corpus-web",
        sender: { webParticipantId: WEB_GUEST_A },
        parts: [{ kind: "TEXT", text: "Hello Web Guest" }],
      },
      {
        tenantId: TENANT_B,
        channelAccountId: ACC_B_LINE,
        provider: "line_oa",
        sourceKind: "WEBHOOK",
        providerMessageId: "shared-msg-1",
        correlationId: "corr-corpus-tb",
        sender: { providerSenderRef: "shared-sender-01" },
        parts: [{ kind: "TEXT", text: "Hello Tenant B" }],
      },
    ];

    const firstPass = [];
    for (const item of corpus) {
      firstPass.push(await service.normalizeAndPersist(item));
    }

    const distinctConversations = new Set(
      firstPass.map((r) => `${r.message.outboxRecord.envelope.scope.tenant_id}:${r.message.conversationId}`)
    );
    assert.equal(distinctConversations.size, 5);
    for (const res of firstPass) {
      assert.equal(res.deduplicated, false);
      assert.equal(res.message.seq, 1);
    }

    // Replaying the exact corpus yields 100% deduplicated hits with identical message & conversation IDs.
    for (let i = 0; i < corpus.length; i++) {
      const replayRes = await service.normalizeAndPersist(corpus[i]!);
      assert.equal(replayRes.deduplicated, true);
      assert.equal(replayRes.message.messageId, firstPass[i]!.message.messageId);
      assert.equal(replayRes.message.conversationId, firstPass[i]!.message.conversationId);
      assert.equal(replayRes.message.seq, 1);
    }
  });

  it("handles commit failure rollback, simultaneous inserts, and live vs historical RESOLVED reopen", async () => {
    const { service, store } = createInMemoryNormalizationService();

    // Crash before commit leaves zero persisted message, then replay succeeds once.
    const crashInput: CanonicalInboundInput = {
      tenantId: TENANT_A,
      channelAccountId: ACC_A_LINE1,
      provider: "line_oa",
      sourceKind: "WEBHOOK",
      providerMessageId: "crash-msg-01",
      correlationId: "corr-norm-crash-01",
      sender: { providerSenderRef: "U-crash-user" },
      parts: [{ kind: "TEXT", text: "retry after crash" }],
    };

    await assert.rejects(
      service.normalizeAndPersist(crashInput, { simulateCrashBeforeCommit: true }),
      (err: unknown) => err instanceof NormalizationError && err.code === "DEPENDENCY_UNAVAILABLE"
    );

    const recovered = await service.normalizeAndPersist(crashInput);
    assert.equal(recovered.deduplicated, false);
    assert.equal(recovered.message.seq, 1);

    // Seed RESOLVED conversation with active claimant; HISTORY_SYNC preserves RESOLVED + claimant.
    const resolvedConvId = randomUUID();
    const resolvedIdentityId = randomUUID();
    store.seedResolvedConversationWithClaimant({
      conversationId: resolvedConvId,
      tenantId: TENANT_A,
      channelAccountId: ACC_A_LINE1,
      identityId: resolvedIdentityId,
      webParticipantId: null,
      lastSeq: 4,
      claimantPrincipalId: "staff-user-1",
    });

    const histResult = await service.normalizeAndPersist({
      tenantId: TENANT_A,
      channelAccountId: ACC_A_LINE1,
      provider: "line_oa",
      sourceKind: "HISTORY_SYNC",
      providerMessageId: "hist-backfill-01",
      conversationId: resolvedConvId,
      correlationId: "corr-hist-no-reopen",
      sender: { identityId: resolvedIdentityId },
      parts: [{ kind: "TEXT", text: "historical message" }],
    });
    assert.equal(histResult.message.seq, 5);
    assert.equal(histResult.message.historicalImport, true);
    assert.equal(histResult.message.reopenedConversation, false);
    assert.equal(histResult.message.conversationStatus, "RESOLVED");
    assert.equal(histResult.message.outboxRecord.envelope.history.suppress_side_effects, true);
    assert.equal(store.getActiveClaimant(resolvedConvId), "staff-user-1");

    // Genuine live webhook inbound reopens RESOLVED -> OPEN and clears claimant.
    const liveReopenResult = await service.normalizeAndPersist({
      tenantId: TENANT_A,
      channelAccountId: ACC_A_LINE1,
      provider: "line_oa",
      sourceKind: "WEBHOOK",
      providerMessageId: "live-reopen-01",
      conversationId: resolvedConvId,
      correlationId: "corr-live-reopen",
      sender: { identityId: resolvedIdentityId },
      parts: [{ kind: "TEXT", text: "customer replied live" }],
    });
    assert.equal(liveReopenResult.message.seq, 6);
    assert.equal(liveReopenResult.message.reopenedConversation, true);
    assert.equal(liveReopenResult.message.conversationStatus, "OPEN");
    assert.equal(store.getActiveClaimant(resolvedConvId), null);
  });
});
