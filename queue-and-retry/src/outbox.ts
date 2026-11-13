import { randomUUID } from "node:crypto";
import {
  InMemoryOutboxInboxStore,
  type OutboxInboxStore,
  PsqlOutboxInboxStore,
  type SqlAuditEffect,
} from "./outbox-store.js";
import {
  type CommitWithOutboxResult,
  type ConsumeInboxResult,
  type EnqueueOutboxInput,
  type InboxReceiptRecord,
  type LocalOwningMutation,
  type OutboxEventEnvelope,
  sanitizeSafeErrorMessage,
  type StoredOutboxRecord,
} from "./outbox-types.js";

export * from "./outbox-types.js";
export * from "./outbox-store.js";

export interface OutboxPublisher {
  publish(envelope: OutboxEventEnvelope): Promise<void>;
}

export interface OutboxCoordinatorConfig {
  readonly workerId?: string;
  readonly leaseMs?: number;
  readonly maxAttempts?: number;
  readonly retryDelayMs?: number;
  readonly now?: () => Date;
}

export interface PublishBatchReport {
  readonly claimedCount: number;
  readonly succeededCount: number;
  readonly retriedCount: number;
  readonly quarantinedCount: number;
  readonly stoppedBeforeAckCount: number;
  readonly records: readonly StoredOutboxRecord[];
}

export class OutboxCoordinator {
  private readonly store: OutboxInboxStore;
  private readonly publisher: OutboxPublisher;
  private readonly workerId: string;
  private readonly leaseMs: number;
  private readonly maxAttempts: number;
  private readonly retryDelayMs: number;
  private readonly now: () => Date;

  constructor(
    store: OutboxInboxStore,
    publisher: OutboxPublisher,
    config: OutboxCoordinatorConfig = {}
  ) {
    this.store = store;
    this.publisher = publisher;
    this.workerId = config.workerId ?? `outbox-worker-${randomUUID().slice(0, 8)}`;
    this.leaseMs = config.leaseMs ?? 30_000;
    this.maxAttempts = config.maxAttempts ?? 5;
    this.retryDelayMs = config.retryDelayMs ?? 1_000;
    this.now = config.now ?? (() => new Date());
  }

  async commitAndStage(
    input: EnqueueOutboxInput,
    localMutation?: LocalOwningMutation,
    options?: { readonly simulateCrashBeforeCommit?: boolean }
  ): Promise<CommitWithOutboxResult> {
    return this.store.commitWithOutbox(input, localMutation, options);
  }

  async publishDueBatch(options?: {
    readonly limit?: number;
    readonly tenantId?: string;
    readonly stopAfterPublishBeforeMark?: boolean;
  }): Promise<PublishBatchReport> {
    const nowDate = this.now();
    const nowIso = nowDate.toISOString();
    const claimed = await this.store.claimDueBatch({
      workerId: this.workerId,
      limit: options?.limit ?? 50,
      leaseMs: this.leaseMs,
      nowIso,
      ...(options?.tenantId !== undefined ? { tenantId: options.tenantId } : {}),
    });

    let succeededCount = 0;
    let retriedCount = 0;
    let quarantinedCount = 0;
    let stoppedBeforeAckCount = 0;
    const records: StoredOutboxRecord[] = [];

    for (const item of claimed) {
      try {
        await this.publisher.publish(item.envelope);
        if (options?.stopAfterPublishBeforeMark) {
          stoppedBeforeAckCount++;
          records.push(item);
          continue;
        }
        const done = await this.store.markSucceeded(
          item.envelope.scope.tenant_id,
          item.envelope.event_id,
          this.now().toISOString()
        );
        succeededCount++;
        records.push(done);
      } catch (err) {
        const nextAvail = new Date(
          this.now().getTime() + this.retryDelayMs * Math.max(1, item.attempts)
        ).toISOString();
        const updated = await this.store.scheduleRetry(
          item.envelope.scope.tenant_id,
          item.envelope.event_id,
          sanitizeSafeErrorMessage(err),
          nextAvail,
          this.maxAttempts
        );
        if (updated.state === "QUARANTINED") {
          quarantinedCount++;
        } else {
          retriedCount++;
        }
        records.push(updated);
      }
    }

    return Object.freeze({
      claimedCount: claimed.length,
      succeededCount,
      retriedCount,
      quarantinedCount,
      stoppedBeforeAckCount,
      records: Object.freeze(records),
    });
  }

  async replayEvent(
    tenantId: string,
    eventId: string,
    replayId: string = randomUUID()
  ): Promise<StoredOutboxRecord> {
    return this.store.prepareReplay(
      tenantId,
      eventId,
      replayId,
      this.now().toISOString()
    );
  }

  async readStreamSince(
    tenantId: string,
    afterOutboxSeq: number,
    limit = 100
  ): Promise<readonly StoredOutboxRecord[]> {
    return this.store.listStreamSince(tenantId, afterOutboxSeq, limit);
  }
}

export type ConsumerEffectHandler = (
  envelope: OutboxEventEnvelope,
  receipt: InboxReceiptRecord
) => Promise<void> | void;

export class ConsumerInboxProcessor {
  private readonly store: OutboxInboxStore;
  private readonly consumerName: string;

  constructor(store: OutboxInboxStore, consumerName: string) {
    this.store = store;
    this.consumerName = consumerName;
  }

  async process(
    envelope: OutboxEventEnvelope,
    handler: ConsumerEffectHandler,
    options?: {
      readonly nowIso?: string;
      readonly sqlAuditEffect?: SqlAuditEffect;
      readonly simulateCrashBeforeCommit?: boolean;
      readonly crashAfterCommitBeforeAck?: boolean;
    }
  ): Promise<ConsumeInboxResult> {
    const consumeOptions = {
      consumer: this.consumerName,
      envelope,
      ...(options?.nowIso !== undefined ? { nowIso: options.nowIso } : {}),
      ...(options?.sqlAuditEffect !== undefined
        ? { sqlAuditEffect: options.sqlAuditEffect }
        : {}),
      ...(options?.simulateCrashBeforeCommit !== undefined
        ? { simulateCrashBeforeCommit: options.simulateCrashBeforeCommit }
        : {}),
    };

    const result = await this.store.consumeEvent(consumeOptions);

    if (result.effectExecuted) {
      await handler(envelope, result.receipt);
      if (options?.crashAfterCommitBeforeAck) {
        throw new Error("Simulated consumer crash after durable commit before broker ack");
      }
    }

    return result;
  }
}

export function createInMemoryOutboxRuntime(): {
  readonly store: InMemoryOutboxInboxStore;
} {
  return { store: new InMemoryOutboxInboxStore() };
}

export function createPostgresOutboxStore(
  connectionUri: string
): PsqlOutboxInboxStore {
  return new PsqlOutboxInboxStore({ connectionUri });
}
