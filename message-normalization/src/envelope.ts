import { normalizeInboundMessage } from "./normalizer.js";
import {
  InMemoryNormalizedMessageStore,
  type NormalizedMessageStore,
  PsqlNormalizedMessageStore,
} from "./store.js";
import type {
  CanonicalInboundInput,
  NormalizedMessageEnvelope,
  PersistNormalizedResult,
} from "./types.js";

export * from "./types.js";
export * from "./normalizer.js";
export * from "./store.js";

/**
 * Coordinates canonical inbound message normalization (text/media/sticker/link
 * and visible unavailable placeholders for unsupported parts) with atomic
 * conversation sequencing, message parts/media persistence, source dedup, and
 * transactional outbox emission.
 */
export class MessageNormalizationService {
  private readonly store: NormalizedMessageStore;

  constructor(store: NormalizedMessageStore) {
    this.store = store;
  }

  normalize(input: CanonicalInboundInput): NormalizedMessageEnvelope {
    return normalizeInboundMessage(input);
  }

  async normalizeAndPersist(
    input: CanonicalInboundInput,
    options?: { readonly simulateCrashBeforeCommit?: boolean }
  ): Promise<PersistNormalizedResult> {
    const envelope = normalizeInboundMessage(input);
    return this.store.persistNormalizedMessage(envelope, options);
  }
}

export function createInMemoryNormalizationService(): {
  readonly store: InMemoryNormalizedMessageStore;
  readonly service: MessageNormalizationService;
} {
  const store = new InMemoryNormalizedMessageStore();
  return {
    store,
    service: new MessageNormalizationService(store),
  };
}

export function createPostgresNormalizationService(connectionUri: string): {
  readonly store: PsqlNormalizedMessageStore;
  readonly service: MessageNormalizationService;
} {
  const store = new PsqlNormalizedMessageStore({ connectionUri });
  return {
    store,
    service: new MessageNormalizationService(store),
  };
}
