import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import {
  ConsumerInboxProcessor,
  InMemoryOutboxInboxStore,
  OutboxCoordinator,
  OutboxError,
  type OutboxEventEnvelope,
} from "../queue-and-retry/src/outbox.js";

const TENANT_A = "11111111-1111-4111-8111-111111111101";
const TENANT_B = "22222222-2222-4222-8222-222222222202";
const ACC_A1 = "33333333-3333-4333-8333-333333333301";
const ID_A1 = "44444444-4444-4444-8444-444444444401";
const CONV_A1 = "55555555-5555-4555-8555-555555555501";

describe("CB-03-03 transactional outbox & inbox unit suite", () => {
  it("enforces schema invariants, history suppression flag, and payload redaction", async () => {
    const store = new InMemoryOutboxInboxStore();

    await assert.rejects(
      store.commitWithOutbox({
        eventId: randomUUID(),
        tenantId: TENANT_A,
        type: "message.created",
        aggregateType: "conversation",
        aggregateKey: `conv:${CONV_A1}`,
        aggregateVersion: 1,
        historicalImport: true,
        suppressSideEffects: false,
        correlationId: "corr-invalid-hist",
        payload: { message_seq: 1 },
      }),
      (err: unknown) =>
        err instanceof OutboxError &&
        err.code === "VALIDATION_ERROR" &&
        err.status === 400
    );

    for (const badPayload of [
      { otp_code: "123456" },
      { nested: { secret_token: "abc" } },
      { presigned_url: "https://example.invalid/file?X-Amz-Signature=1" },
      { raw_body: "sensitive text" },
      { storage_object_key: "https://cdn.example/obj?sig=1" },
      { storage_object_key: "tenants/t1/media/asset?token=xyz" },
    ]) {
      await assert.rejects(
        store.commitWithOutbox({
          eventId: randomUUID(),
          tenantId: TENANT_A,
          type: "message.created",
          aggregateType: "conversation",
          aggregateKey: `conv:${CONV_A1}:bad`,
          aggregateVersion: 1,
          correlationId: "corr-bad-payload",
          payload: badPayload,
        }),
        (err: unknown) =>
          err instanceof OutboxError &&
          err.code === "VALIDATION_ERROR" &&
          err.status === 400
      );
    }

    await assert.rejects(
      store.commitWithOutbox({
        eventId: randomUUID(),
        tenantId: TENANT_A,
        producer: "chatbridge",
        ownerDb: "other_db",
        type: "message.created",
        aggregateType: "conversation",
        aggregateKey: `conv:${CONV_A1}:owner`,
        aggregateVersion: 1,
        correlationId: "corr-owner-mismatch",
        payload: { message_seq: 1 },
      }),
      (err: unknown) => err instanceof OutboxError && err.code === "VALIDATION_ERROR"
    );

    await assert.rejects(
      store.commitWithOutbox({
        eventId: randomUUID(),
        tenantId: TENANT_A,
        type: "message.created",
        aggregateType: "conversation",
        aggregateKey: `conv:${CONV_A1}:replay`,
        aggregateVersion: 1,
        isReplay: true,
        replayId: null,
        correlationId: "corr-replay-missing",
        payload: { message_seq: 1 },
      }),
      (err: unknown) => err instanceof OutboxError && err.code === "VALIDATION_ERROR"
    );
  });

  it("rolls back atomic local commit on crash and deduplicates idempotent retries", async () => {
    const store = new InMemoryOutboxInboxStore();
    const eventId = randomUUID();
    const messageId = randomUUID();

    await assert.rejects(
      store.commitWithOutbox(
        {
          eventId,
          tenantId: TENANT_A,
          channelAccountId: ACC_A1,
          type: "message.created",
          aggregateType: "conversation",
          aggregateKey: `conv:${CONV_A1}`,
          aggregateVersion: 1,
          correlationId: "corr-crash-before-commit",
          payload: {
            message_id: messageId,
            message_seq: 1,
            storage_object_key: "tenants/t1/media/asset_1",
          },
        },
        {
          kind: "CONVERSATION_MESSAGE",
          conversationId: CONV_A1,
          channelAccountId: ACC_A1,
          identityId: ID_A1,
          messageId,
          messageSeq: 1,
          direction: "IN",
          senderKind: "CUSTOMER",
          body: "hello",
        },
        { simulateCrashBeforeCommit: true }
      ),
      (err: unknown) =>
        err instanceof OutboxError &&
        err.code === "DEPENDENCY_UNAVAILABLE" &&
        err.status === 503
    );

    assert.equal(store.getCommittedMessageCount(), 0);
    assert.equal(await store.getOutboxRecord(TENANT_A, eventId), null);

    const first = await store.commitWithOutbox(
      {
        eventId,
        tenantId: TENANT_A,
        channelAccountId: ACC_A1,
        type: "message.created",
        aggregateType: "conversation",
        aggregateKey: `conv:${CONV_A1}`,
        aggregateVersion: 1,
        correlationId: "corr-commit-ok-01",
        payload: {
          message_id: messageId,
          message_seq: 1,
          storage_object_key: "tenants/t1/media/asset_1",
        },
      },
      {
        kind: "CONVERSATION_MESSAGE",
        conversationId: CONV_A1,
        channelAccountId: ACC_A1,
        identityId: ID_A1,
        messageId,
        messageSeq: 1,
        direction: "IN",
        senderKind: "CUSTOMER",
        body: "hello",
      }
    );

    assert.equal(first.deduplicated, false);
    assert.equal(first.record.state, "PENDING");
    assert.equal(first.record.envelope.stream_cursor.outbox_seq, 1);
    assert.equal(store.getCommittedMessageCount(), 1);

    const retry = await store.commitWithOutbox({
      eventId: randomUUID(),
      tenantId: TENANT_A,
      channelAccountId: ACC_A1,
      type: "message.created",
      aggregateType: "conversation",
      aggregateKey: `conv:${CONV_A1}`,
      aggregateVersion: 1,
      correlationId: "corr-commit-ok-01",
      payload: {
        message_id: messageId,
        message_seq: 1,
        storage_object_key: "tenants/t1/media/asset_1",
      },
    });
    assert.equal(retry.deduplicated, true);
    assert.equal(retry.record.envelope.event_id, eventId);
  });

  it("converges crash between local commit/publish and consumer replay to a single ChatBridge effect", async () => {
    let clockMs = Date.parse("2026-11-13T12:00:00.000Z");
    const store = new InMemoryOutboxInboxStore();
    const publishedEnvelopes: OutboxEventEnvelope[] = [];
    const consumer = new ConsumerInboxProcessor(store, "auto-responder-worker");

    let handlerExecutions = 0;

    // Worker 1 commits locally, then crashes before marking published.
    const worker1 = new OutboxCoordinator(
      store,
      {
        async publish(env) {
          publishedEnvelopes.push(env);
        },
      },
      { workerId: "worker-1", leaseMs: 10_000, now: () => new Date(clockMs) }
    );

    const eventId = randomUUID();
    const committed = await worker1.commitAndStage({
      eventId,
      tenantId: TENANT_A,
      type: "message.created",
      aggregateType: "conversation",
      aggregateKey: `conv:${CONV_A1}`,
      aggregateVersion: 1,
      occurredAt: new Date(clockMs).toISOString(),
      correlationId: "corr-crash-publish-01",
      payload: { message_seq: 1 },
    });
    assert.equal(committed.record.state, "PENDING");

    // Process crashed between commit and publish -> publishDueBatch runs with stopAfterPublishBeforeMark=true
    const crashedPublish = await worker1.publishDueBatch({
      stopAfterPublishBeforeMark: true,
    });
    assert.equal(crashedPublish.claimedCount, 1);
    assert.equal(crashedPublish.stoppedBeforeAckCount, 1);
    assert.equal(publishedEnvelopes.length, 1);

    // Consumer receives attempt 1, fails pre-commit first (rolled back), then commits and crashes before ack.
    await assert.rejects(
      consumer.process(
        publishedEnvelopes[0]!,
        () => {
          handlerExecutions++;
        },
        { nowIso: new Date(clockMs).toISOString(), simulateCrashBeforeCommit: true }
      ),
      /Simulated consumer crash before commit/
    );
    assert.equal(handlerExecutions, 0);

    await assert.rejects(
      consumer.process(
        publishedEnvelopes[0]!,
        () => {
          handlerExecutions++;
        },
        { nowIso: new Date(clockMs).toISOString(), crashAfterCommitBeforeAck: true }
      ),
      /Simulated consumer crash after durable commit/
    );
    assert.equal(handlerExecutions, 1);

    // Worker 2 cannot steal active lease before 10s expiry.
    const worker2 = new OutboxCoordinator(
      store,
      {
        async publish(env) {
          publishedEnvelopes.push(env);
        },
      },
      { workerId: "worker-2", leaseMs: 10_000, now: () => new Date(clockMs) }
    );
    clockMs += 3_000;
    const premature = await worker2.publishDueBatch();
    assert.equal(premature.claimedCount, 0);

    // After lease expires, Worker 2 reclaims and re-publishes (attempt 2).
    clockMs += 10_000;
    const recovered = await worker2.publishDueBatch();
    assert.equal(recovered.claimedCount, 1);
    assert.equal(recovered.succeededCount, 1);
    assert.equal(recovered.records[0]!.state, "SUCCEEDED");
    assert.equal(recovered.records[0]!.attempts, 2);
    assert.equal(publishedEnvelopes.length, 2);
    assert.equal(publishedEnvelopes[1]!.event_id, eventId);
    assert.equal(publishedEnvelopes[1]!.replay.attempt, 2);

    // Consumer receives duplicate delivery from Worker 2 AND explicit operator replay -> zero extra effects.
    const dupResult = await consumer.process(publishedEnvelopes[1]!, () => {
      handlerExecutions++;
    });
    assert.equal(dupResult.status, "DUPLICATE_NOOP");
    assert.equal(dupResult.effectExecuted, false);

    const replayId = randomUUID();
    await worker2.replayEvent(TENANT_A, eventId, replayId);
    const replayedPublish = await worker2.publishDueBatch();
    assert.equal(replayedPublish.succeededCount, 1);
    assert.equal(publishedEnvelopes.length, 3);
    assert.equal(publishedEnvelopes[2]!.replay.is_replay, true);
    assert.equal(publishedEnvelopes[2]!.replay.replay_id, replayId);

    const replayConsume = await consumer.process(publishedEnvelopes[2]!, () => {
      handlerExecutions++;
    });
    assert.equal(replayConsume.status, "DUPLICATE_NOOP");
    assert.equal(replayConsume.effectExecuted, false);
    assert.equal(handlerExecutions, 1);

    // Different consumer on the same tenant runs independently, and stale sequence is ignored.
    const analyticsConsumer = new ConsumerInboxProcessor(store, "analytics-projector");
    let analyticsEffects = 0;
    const envSeq2Commit = await worker2.commitAndStage({
      eventId: randomUUID(),
      tenantId: TENANT_A,
      type: "message.created",
      aggregateType: "conversation",
      aggregateKey: `conv:${CONV_A1}`,
      aggregateVersion: 2,
      occurredAt: new Date(clockMs).toISOString(),
      correlationId: "corr-seq-2-event-01",
      payload: { message_seq: 2 },
    });
    const batch2 = await worker2.publishDueBatch();
    assert.equal(batch2.succeededCount, 1);

    const applySeq2 = await analyticsConsumer.process(
      envSeq2Commit.record.envelope,
      () => {
        analyticsEffects++;
      }
    );
    assert.equal(applySeq2.status, "APPLIED");
    assert.equal(analyticsEffects, 1);

    // Brand new eventId with stale aggregate_seq=1 must be IGNORED_STALE.
    const staleResult = await analyticsConsumer.process(
      publishedEnvelopes[0]!,
      () => {
        analyticsEffects++;
      }
    );
    assert.equal(staleResult.status, "IGNORED_STALE");
    assert.equal(staleResult.effectExecuted, false);
    assert.equal(analyticsEffects, 1);
  });

  it("preserves historical flags on replay and suppresses consumer side effects", async () => {
    const store = new InMemoryOutboxInboxStore();
    const published: OutboxEventEnvelope[] = [];
    const coordinator = new OutboxCoordinator(
      store,
      {
        async publish(env) {
          published.push(env);
        },
      },
      { maxAttempts: 2, retryDelayMs: 0 }
    );

    const eventId = randomUUID();
    await coordinator.commitAndStage({
      eventId,
      tenantId: TENANT_B,
      type: "message.created",
      aggregateType: "conversation",
      aggregateKey: `conv:${TENANT_B}:001`,
      aggregateVersion: 1,
      historicalImport: true,
      suppressSideEffects: true,
      sourceKind: "INITIAL_BACKFILL",
      correlationId: "corr-history-backfill-01",
      payload: { message_seq: 1, sync_run_id: "sync-1" },
    });

    await coordinator.publishDueBatch();
    const replayId = randomUUID();
    await coordinator.replayEvent(TENANT_B, eventId, replayId);
    await coordinator.publishDueBatch();

    assert.equal(published.length, 2);
    for (const env of published) {
      assert.equal(env.history.historical_import, true);
      assert.equal(env.history.suppress_side_effects, true);
      assert.equal(env.history.source_kind, "INITIAL_BACKFILL");
    }
    assert.equal(published[1]!.replay.is_replay, true);
    assert.equal(published[1]!.replay.replay_id, replayId);

    const consumer = new ConsumerInboxProcessor(store, "ai-auto-responder");
    let aiEffects = 0;
    const firstConsume = await consumer.process(published[0]!, () => {
      aiEffects++;
    });
    assert.equal(firstConsume.status, "SUPPRESSED_HISTORY");
    assert.equal(firstConsume.effectExecuted, false);
    assert.equal(firstConsume.receipt.historicalImport, true);
    assert.equal(firstConsume.receipt.suppressSideEffects, true);
    assert.equal(firstConsume.receipt.sourceKind, "INITIAL_BACKFILL");

    const replayConsume = await consumer.process(published[1]!, () => {
      aiEffects++;
    });
    assert.equal(replayConsume.status, "DUPLICATE_NOOP");
    assert.equal(replayConsume.effectExecuted, false);
    assert.equal(aiEffects, 0);
  });

  it("quarantines poisoned deliveries after maxAttempts and supports manual replay recovery", async () => {
    let nowMs = Date.parse("2026-11-13T13:00:00.000Z");
    const store = new InMemoryOutboxInboxStore();
    let shouldFail = true;

    const coordinator = new OutboxCoordinator(
      store,
      {
        async publish() {
          if (shouldFail) {
            throw new Error("downstream error Authorization: Bearer secret123");
          }
        },
      },
      { maxAttempts: 2, retryDelayMs: 0, now: () => new Date(nowMs) }
    );

    const eventId = randomUUID();
    await coordinator.commitAndStage({
      eventId,
      tenantId: TENANT_A,
      type: "conversation.assignment.changed",
      aggregateType: "conversation",
      aggregateKey: `conv:${CONV_A1}`,
      aggregateVersion: 10,
      occurredAt: new Date(nowMs).toISOString(),
      correlationId: "corr-dlq-recovery-01",
      payload: { claim_state: "CLAIMED" },
    });

    const attempt1 = await coordinator.publishDueBatch();
    assert.equal(attempt1.retriedCount, 1);
    assert.equal(attempt1.records[0]!.state, "RETRY_WAIT");
    assert.ok(!attempt1.records[0]!.last_error_safe?.includes("secret123"));

    nowMs += 1_000;
    const attempt2 = await coordinator.publishDueBatch();
    assert.equal(attempt2.quarantinedCount, 1);
    assert.equal(attempt2.records[0]!.state, "QUARANTINED");

    shouldFail = false;
    await coordinator.replayEvent(TENANT_A, eventId, randomUUID());
    const recovered = await coordinator.publishDueBatch();
    assert.equal(recovered.succeededCount, 1);
    assert.equal(recovered.records[0]!.state, "SUCCEEDED");
    assert.equal(recovered.records[0]!.envelope.replay.is_replay, true);
  });
});
