import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { createHmac } from "node:crypto";
import {
  InMemoryReceiptStore,
  WebhookIngressService,
  type AccountBindingResolver,
  type ChannelAccountBinding,
} from "../chat-core/src/ingress.js";

const TENANT_A = "11111116-1111-4111-8111-111111111111";
const TENANT_B = "22222226-2222-4222-8222-222222222222";
const ACC_A1 = "aaaaaaaa-0001-4111-8111-111111111111";
const ACC_A2 = "aaaaaaaa-0002-4111-8111-111111111111";
const ACC_B1 = "bbbbbbbb-0001-4222-8222-222222222222";
const SITE_A = "a5170001-1111-4111-8111-111111111111";

const SECRET_A1 = "synthetic-line-secret-a1";
const SECRET_A2 = "synthetic-line-secret-a2";
const SECRET_B1 = "synthetic-tiktok-secret-b1";

function signLine(body: string, secret: string): string {
  return createHmac("sha256", secret).update(body).digest("base64");
}

function signTiktok(body: string, secret: string): string {
  return `sha256=${createHmac("sha256", secret).update(body).digest("hex")}`;
}

function createStaticResolver(bindings: readonly ChannelAccountBinding[]): AccountBindingResolver {
  return {
    async resolveAccountBinding(tenantId, channelAccountId, provider) {
      return (
        bindings.find(
          (b) =>
            b.tenantId === tenantId &&
            b.channelAccountId === channelAccountId &&
            b.provider === provider
        ) ?? null
      );
    },
  };
}

describe("CB-02-01 Verified inbound webhook receipt & dedup boundary", () => {
  const resolver = createStaticResolver([
    {
      tenantId: TENANT_A,
      channelAccountId: ACC_A1,
      provider: "line_oa",
      connectionStatus: "CONNECTED",
      signingSecret: SECRET_A1,
    },
    {
      tenantId: TENANT_A,
      channelAccountId: ACC_A2,
      provider: "line_oa",
      connectionStatus: "CONNECTED",
      signingSecret: SECRET_A2,
    },
    {
      tenantId: TENANT_B,
      channelAccountId: ACC_B1,
      provider: "tiktok",
      connectionStatus: "CONNECTED",
      signingSecret: SECRET_B1,
    },
    {
      tenantId: TENANT_A,
      channelAccountId: "aaaaaaaa-0003-4111-8111-111111111111",
      provider: "web_chat",
      connectionStatus: "CONNECTED",
      signingSecret: "unused",
      allowedWebOrigins: ["https://shop-a.example"],
    },
  ]);

  it("rejects invalid signature and unbound/revoked channels with zero receipt persistence", async () => {
    const store = new InMemoryReceiptStore();
    const service = new WebhookIngressService({ store, resolver });

    const body = JSON.stringify({ events: [{ id: "evt-bad-sig" }] });
    const badSig = await service.ingestProviderWebhook({
      provider: "line_oa",
      tenantId: TENANT_A,
      channelAccountId: ACC_A1,
      rawBody: body,
      signatureHeader: "invalid-signature",
      events: [{ eventKind: "message.inbound", providerEventId: "evt-bad-sig", payload: { text: "hi" } }],
    });

    assert.equal(badSig.httpStatus, 401);
    assert.equal(badSig.persisted, false);
    assert.equal(badSig.error.code, "UNAUTHENTICATED");
    assert.equal(store.getAllCommittedReceipts().length, 0);

    const unbound = await service.ingestProviderWebhook({
      provider: "line_oa",
      tenantId: TENANT_A,
      channelAccountId: ACC_B1,
      rawBody: body,
      signatureHeader: signLine(body, SECRET_B1),
      events: [{ eventKind: "message.inbound", providerEventId: "evt-unbound", payload: { text: "hi" } }],
    });

    assert.equal(unbound.httpStatus, 403);
    assert.equal(unbound.persisted, false);
    assert.equal(store.getAllCommittedReceipts().length, 0);
  });

  it("deduplicates duplicate, reordered, and missing-ID payloads per scoped channel account without cross-account collision", async () => {
    const store = new InMemoryReceiptStore();
    const service = new WebhookIngressService({ store, resolver });

    // 1. Reordered batch: [evt-2, evt-1] then [evt-1, evt-2] converges to 2 canonical receipts.
    const batch1Body = JSON.stringify({ batch: 1 });
    const ack1 = await service.ingestProviderWebhook({
      provider: "line_oa",
      tenantId: TENANT_A,
      channelAccountId: ACC_A1,
      rawBody: batch1Body,
      signatureHeader: signLine(batch1Body, SECRET_A1),
      events: [
        { eventKind: "message.inbound", providerEventId: "evt-2", providerMessageId: "msg-2", payload: { seqHint: 2 } },
        { eventKind: "message.inbound", providerEventId: "evt-1", providerMessageId: "msg-1", payload: { seqHint: 1 } },
      ],
    });
    assert.equal(ack1.httpStatus, 202);
    if (ack1.httpStatus === 202) {
      assert.equal(ack1.data.persisted, true);
      assert.equal(ack1.data.deduplicated, false);
      assert.equal(ack1.data.history_sync_satisfied, false);
      assert.equal(ack1.data.receipt_ids.length, 2);
    }

    // Reordered replay + missing providerEventId on msg-1 must converge to the exact existing canonical receipts.
    const batch2Body = JSON.stringify({ batch: 2 });
    const ack2 = await service.ingestProviderWebhook({
      provider: "line_oa",
      tenantId: TENANT_A,
      channelAccountId: ACC_A1,
      rawBody: batch2Body,
      signatureHeader: signLine(batch2Body, SECRET_A1),
      events: [
        { eventKind: "message.inbound", providerMessageId: "msg-1", payload: { seqHint: 1 } },
        { eventKind: "message.inbound", providerEventId: "evt-2", providerMessageId: "msg-2", payload: { seqHint: 2 } },
      ],
    });
    assert.equal(ack2.httpStatus, 202);
    if (ack1.httpStatus === 202 && ack2.httpStatus === 202) {
      assert.equal(ack2.data.deduplicated, true);
      assert.equal(ack2.data.receipt_ids[0], ack1.data.receipt_ids[1]);
      assert.equal(ack2.data.receipt_ids[1], ack1.data.receipt_ids[0]);
    }

    // 2. Missing BOTH providerEventId & providerMessageId uses canonical sorted body fallback hash.
    const noIdPayloadA = { text: "สวัสดี", sender: "cust-9", isRedelivery: false };
    const noIdPayloadB = { isRedelivery: true, sender: "cust-9", text: "สวัสดี" };
    const rawNoIdA = JSON.stringify(noIdPayloadA);
    const rawNoIdB = JSON.stringify(noIdPayloadB);

    const noIdFirst = await service.ingestProviderWebhook({
      provider: "tiktok",
      tenantId: TENANT_B,
      channelAccountId: ACC_B1,
      rawBody: rawNoIdA,
      signatureHeader: signTiktok(rawNoIdA, SECRET_B1),
      events: [{ eventKind: "message.inbound", payload: noIdPayloadA }],
    });
    const noIdReplay = await service.ingestProviderWebhook({
      provider: "tiktok",
      tenantId: TENANT_B,
      channelAccountId: ACC_B1,
      rawBody: rawNoIdB,
      signatureHeader: signTiktok(rawNoIdB, SECRET_B1),
      events: [{ eventKind: "message.inbound", payload: noIdPayloadB }],
    });

    assert.equal(noIdFirst.httpStatus, 202);
    assert.equal(noIdReplay.httpStatus, 202);
    if (noIdFirst.httpStatus === 202 && noIdReplay.httpStatus === 202) {
      assert.equal(noIdFirst.data.deduplicated, false);
      assert.equal(noIdReplay.data.deduplicated, true);
      assert.equal(noIdReplay.data.receipt_id, noIdFirst.data.receipt_id);
      assert.equal(noIdFirst.data.receipts[0]?.dedup_strategy, "CANONICAL_BODY_FALLBACK");
    }

    // 3. Cross-account same external IDs (evt-1 / msg-1) on ACC_A2 yields its own independent canonical receipt.
    const otherAccAck = await service.ingestProviderWebhook({
      provider: "line_oa",
      tenantId: TENANT_A,
      channelAccountId: ACC_A2,
      rawBody: batch1Body,
      signatureHeader: signLine(batch1Body, SECRET_A2),
      events: [{ eventKind: "message.inbound", providerEventId: "evt-1", providerMessageId: "msg-1", payload: { seqHint: 1 } }],
    });
    assert.equal(otherAccAck.httpStatus, 202);
    if (ack1.httpStatus === 202 && otherAccAck.httpStatus === 202) {
      assert.equal(otherAccAck.data.deduplicated, false);
      assert.notEqual(otherAccAck.data.receipt_id, ack1.data.receipt_ids[1]);
    }

    // 4. Web Chat guest ingress deduplicates by clientMessageId and denies unverified guest/origin.
    const wcDenied = await service.ingestWebChatGuestInbound({
      tenantId: TENANT_A,
      channelAccountId: "aaaaaaaa-0003-4111-8111-111111111111",
      siteId: SITE_A,
      origin: "https://evil.example",
      guestSessionToken: "tok",
      verifiedGuestParticipantId: "guest-1",
      rawBody: "{}",
      event: { eventKind: "message.inbound", clientMessageId: "cm-1", payload: { text: "hi" } },
    });
    assert.equal(wcDenied.httpStatus, 403);
    assert.equal(wcDenied.persisted, false);

    assert.equal(store.getAllCommittedReceipts().length, 4);
  });

  it("never returns durable success (503 DEPENDENCY_UNAVAILABLE) when DB fails before commit", async () => {
    const failingStore = new InMemoryReceiptStore({
      beforeCommit() {
        throw new Error("simulated postgres commit fault");
      },
    });
    const service = new WebhookIngressService({ store: failingStore, resolver });
    const body = JSON.stringify({ event: "e-fail" });

    const res = await service.ingestProviderWebhook({
      provider: "line_oa",
      tenantId: TENANT_A,
      channelAccountId: ACC_A1,
      rawBody: body,
      signatureHeader: signLine(body, SECRET_A1),
      events: [{ eventKind: "message.inbound", providerEventId: "e-fail", payload: { text: "hello" } }],
    });

    assert.equal(res.httpStatus, 503);
    assert.equal(res.persisted, false);
    if (res.httpStatus !== 202) {
      assert.equal(res.error.code, "DEPENDENCY_UNAVAILABLE");
      assert.equal(res.error.retryable, true);
    }
    assert.equal(failingStore.getAllCommittedReceipts().length, 0);
  });
});
