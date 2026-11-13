import type {
  IngressProvider,
  PersistReceiptOutcome,
  PersistReceiptRecordInput,
  ReceiptDedupStrategy,
  ReceiptState,
  StoredWebhookReceipt,
} from "./receipt-types.js";

export interface ReceiptStore {
  persistVerifiedBatch(
    inputs: readonly PersistReceiptRecordInput[]
  ): Promise<readonly PersistReceiptOutcome[]>;
}

export interface InMemoryReceiptStoreHooks {
  readonly beforeCommit?: (
    pending: readonly PersistReceiptRecordInput[]
  ) => Promise<void> | void;
}

function cloneReceipt(r: StoredWebhookReceipt): StoredWebhookReceipt {
  return Object.freeze({ ...r });
}

export class InMemoryReceiptStore implements ReceiptStore {
  private readonly rows: StoredWebhookReceipt[] = [];
  private readonly hooks: InMemoryReceiptStoreHooks;

  constructor(hooks: InMemoryReceiptStoreHooks = {}) {
    this.hooks = hooks;
  }

  private findExistingConflict(
    input: PersistReceiptRecordInput
  ): StoredWebhookReceipt | null {
    for (const existing of this.rows) {
      if (
        existing.tenantId !== input.tenantId ||
        existing.channelAccountId !== input.channelAccountId
      ) {
        continue;
      }
      if (existing.canonicalDedupKey === input.canonicalDedupKey) {
        return existing;
      }
      if (
        input.providerEventId !== null &&
        existing.eventKind === input.eventKind &&
        existing.providerEventId === input.providerEventId
      ) {
        return existing;
      }
      if (
        input.providerMessageId !== null &&
        existing.eventKind === input.eventKind &&
        existing.providerMessageId === input.providerMessageId
      ) {
        return existing;
      }
    }
    return null;
  }

  async persistVerifiedBatch(
    inputs: readonly PersistReceiptRecordInput[]
  ): Promise<readonly PersistReceiptOutcome[]> {
    if (inputs.length === 0) {
      return [];
    }

    const stagedNew: StoredWebhookReceipt[] = [];
    const stagedOutcomes: PersistReceiptOutcome[] = [];

    const lookupCandidate = (
      input: PersistReceiptRecordInput
    ): StoredWebhookReceipt | null => {
      const committed = this.findExistingConflict(input);
      if (committed) {
        return committed;
      }
      for (const s of stagedNew) {
        if (
          s.tenantId === input.tenantId &&
          s.channelAccountId === input.channelAccountId &&
          (s.canonicalDedupKey === input.canonicalDedupKey ||
            (input.providerEventId !== null &&
              s.eventKind === input.eventKind &&
              s.providerEventId === input.providerEventId) ||
            (input.providerMessageId !== null &&
              s.eventKind === input.eventKind &&
              s.providerMessageId === input.providerMessageId))
        ) {
          return s;
        }
      }
      return null;
    };

    for (const input of inputs) {
      const existing = lookupCandidate(input);
      if (existing) {
        stagedOutcomes.push({
          receipt: cloneReceipt(existing),
          deduplicated: true,
        });
        continue;
      }

      const created: StoredWebhookReceipt = {
        id: input.id,
        tenantId: input.tenantId,
        channelAccountId: input.channelAccountId,
        provider: input.provider,
        eventKind: input.eventKind,
        scopedProviderKey: input.scopedProviderKey,
        providerEventId: input.providerEventId,
        providerMessageId: input.providerMessageId,
        canonicalDedupKey: input.canonicalDedupKey,
        dedupStrategy: input.dedupStrategy,
        bodyHash: input.bodyHash,
        state: "VERIFIED",
        signatureVerified: input.signatureVerified,
        correlationId: input.correlationId,
        receivedAt: input.receivedAtIso,
      };
      stagedNew.push(created);
      stagedOutcomes.push({
        receipt: cloneReceipt(created),
        deduplicated: false,
      });
    }

    // Fault-injection point BEFORE durable commit: throws must leave rows unchanged.
    if (this.hooks.beforeCommit) {
      await this.hooks.beforeCommit(inputs);
    }

    for (const created of stagedNew) {
      this.rows.push(created);
    }

    return Object.freeze(stagedOutcomes);
  }

  getAllCommittedReceipts(): readonly StoredWebhookReceipt[] {
    return Object.freeze(this.rows.map(cloneReceipt));
  }
}

export interface PsqlReceiptStoreConfig {
  readonly connectionUri: string;
  readonly beforeCommit?: (
    pending: readonly PersistReceiptRecordInput[]
  ) => Promise<void> | void;
}

interface RawReceiptRow {
  readonly id: string;
  readonly tenant_id: string;
  readonly channel_account_id: string;
  readonly provider: IngressProvider;
  readonly event_kind: string;
  readonly scoped_provider_key: string | null;
  readonly provider_event_id: string | null;
  readonly provider_message_id: string | null;
  readonly canonical_dedup_key: string;
  readonly dedup_strategy: ReceiptDedupStrategy;
  readonly body_hash: string;
  readonly state: ReceiptState;
  readonly signature_verified: boolean;
  readonly correlation_id: string | null;
  readonly received_at: string;
  readonly deduplicated: boolean;
}

function sqlTextB64(value: string): string {
  const b64 = Buffer.from(value, "utf8").toString("base64");
  if (!/^[A-Za-z0-9+/=]*$/.test(b64)) {
    throw new Error("Invalid UTF-8 base64 encoding for SQL parameter");
  }
  return `convert_from(decode('${b64}', 'base64'), 'utf8')`;
}

function sqlNullableTextB64(value: string | null): string {
  return value === null ? "NULL" : sqlTextB64(value);
}

function sqlUuidLiteral(value: string): string {
  return `(${sqlTextB64(value)})::uuid`;
}

function sqlJsonbB64(value: unknown): string {
  return `(${sqlTextB64(JSON.stringify(value))})::jsonb`;
}

function sqlTimestamptzLiteral(iso: string): string {
  return `(${sqlTextB64(iso)})::timestamptz`;
}

function buildPgEnv(connectionUri: string): NodeJS.ProcessEnv {
  const u = new URL(connectionUri);
  const env: NodeJS.ProcessEnv = {
    ...process.env,
    PGHOST: u.hostname || "127.0.0.1",
    PGPORT: u.port || "5432",
    PGUSER: decodeURIComponent(u.username),
    PGPASSWORD: decodeURIComponent(u.password),
    PGDATABASE: u.pathname.replace(/^\//, ""),
    PGSSLMODE: u.searchParams.get("sslmode") ?? "disable",
    PGOPTIONS: "-c client_min_messages=warning",
  };
  delete env.DATABASE_URL;
  return env;
}

function mapRawRow(row: RawReceiptRow): PersistReceiptOutcome {
  return {
    receipt: Object.freeze({
      id: row.id,
      tenantId: row.tenant_id,
      channelAccountId: row.channel_account_id,
      provider: row.provider,
      eventKind: row.event_kind,
      scopedProviderKey: row.scoped_provider_key,
      providerEventId: row.provider_event_id,
      providerMessageId: row.provider_message_id,
      canonicalDedupKey: row.canonical_dedup_key,
      dedupStrategy: row.dedup_strategy,
      bodyHash: row.body_hash,
      state: row.state,
      signatureVerified: Boolean(row.signature_verified),
      correlationId: row.correlation_id,
      receivedAt: row.received_at,
    }),
    deduplicated: Boolean(row.deduplicated),
  };
}

export class PsqlReceiptStore implements ReceiptStore {
  private readonly pgEnv: NodeJS.ProcessEnv;
  private readonly beforeCommit?: (
    pending: readonly PersistReceiptRecordInput[]
  ) => Promise<void> | void;

  constructor(config: PsqlReceiptStoreConfig) {
    this.pgEnv = buildPgEnv(config.connectionUri);
    if (config.beforeCommit) {
      this.beforeCommit = config.beforeCommit;
    }
  }

  async persistVerifiedBatch(
    inputs: readonly PersistReceiptRecordInput[]
  ): Promise<readonly PersistReceiptOutcome[]> {
    if (inputs.length === 0) {
      return [];
    }

    // Deduplicate intra-batch collisions in memory so each distinct key runs in one atomic CTE statement.
    const uniqueInputs: PersistReceiptRecordInput[] = [];
    const slotToUniqueIdx: Array<{ readonly uniqueIdx: number; readonly intraBatchDup: boolean }> = [];

    for (const input of inputs) {
      let matchedIdx = -1;
      for (let i = 0; i < uniqueInputs.length; i++) {
        const u = uniqueInputs[i]!;
        if (
          u.tenantId === input.tenantId &&
          u.channelAccountId === input.channelAccountId &&
          (u.canonicalDedupKey === input.canonicalDedupKey ||
            (input.providerEventId !== null &&
              u.eventKind === input.eventKind &&
              u.providerEventId === input.providerEventId) ||
            (input.providerMessageId !== null &&
              u.eventKind === input.eventKind &&
              u.providerMessageId === input.providerMessageId))
        ) {
          matchedIdx = i;
          break;
        }
      }
      if (matchedIdx >= 0) {
        slotToUniqueIdx.push({ uniqueIdx: matchedIdx, intraBatchDup: true });
      } else {
        const nextIdx = uniqueInputs.length;
        uniqueInputs.push(input);
        slotToUniqueIdx.push({ uniqueIdx: nextIdx, intraBatchDup: false });
      }
    }

    // Build one pure-SQL atomic CTE query (zero TEMP tables needed under least-privilege chatbridge_app).
    const ctes: string[] = [];
    const unions: string[] = [];

    uniqueInputs.forEach((input, idx) => {
      const evtCond =
        input.providerEventId !== null
          ? `OR (event_kind = ${sqlTextB64(input.eventKind)} AND provider_event_id = ${sqlTextB64(input.providerEventId)})`
          : "";
      const msgCond =
        input.providerMessageId !== null
          ? `OR (event_kind = ${sqlTextB64(input.eventKind)} AND provider_message_id = ${sqlTextB64(input.providerMessageId)})`
          : "";

      ctes.push(`
        ins_${idx} AS (
          INSERT INTO webhook_receipts (
            id, tenant_id, channel_account_id, provider, event_kind,
            scoped_provider_key, provider_event_id, provider_message_id,
            canonical_dedup_key, dedup_strategy, body_hash, state,
            signature_verified, correlation_id, payload_redacted, received_at
          )
          VALUES (
            ${sqlUuidLiteral(input.id)},
            ${sqlUuidLiteral(input.tenantId)},
            ${sqlUuidLiteral(input.channelAccountId)},
            ${sqlTextB64(input.provider)},
            ${sqlTextB64(input.eventKind)},
            ${sqlNullableTextB64(input.scopedProviderKey)},
            ${sqlNullableTextB64(input.providerEventId)},
            ${sqlNullableTextB64(input.providerMessageId)},
            ${sqlTextB64(input.canonicalDedupKey)},
            ${sqlTextB64(input.dedupStrategy)},
            ${sqlTextB64(input.bodyHash)},
            'VERIFIED',
            ${input.signatureVerified ? "TRUE" : "FALSE"},
            ${sqlNullableTextB64(input.correlationId)},
            ${sqlJsonbB64(input.payloadRedacted)},
            ${sqlTimestamptzLiteral(input.receivedAtIso)}
          )
          ON CONFLICT DO NOTHING
          RETURNING
            id::text, tenant_id::text, channel_account_id::text, provider, event_kind,
            scoped_provider_key, provider_event_id, provider_message_id,
            canonical_dedup_key, dedup_strategy, body_hash, state,
            signature_verified, correlation_id, received_at::text,
            FALSE AS deduplicated
        ),
        existing_${idx} AS (
          SELECT
            id::text, tenant_id::text, channel_account_id::text, provider, event_kind,
            scoped_provider_key, provider_event_id, provider_message_id,
            canonical_dedup_key, dedup_strategy, body_hash, state,
            signature_verified, correlation_id, received_at::text,
            TRUE AS deduplicated
          FROM webhook_receipts
          WHERE NOT EXISTS (SELECT 1 FROM ins_${idx})
            AND tenant_id = ${sqlUuidLiteral(input.tenantId)}
            AND channel_account_id = ${sqlUuidLiteral(input.channelAccountId)}
            AND (
              canonical_dedup_key = ${sqlTextB64(input.canonicalDedupKey)}
              ${evtCond}
              ${msgCond}
            )
          ORDER BY received_at ASC, id ASC
          LIMIT 1
        )
      `);

      unions.push(`
        SELECT ${idx} AS idx, row_to_json(r.*) AS payload FROM ins_${idx} r
        UNION ALL
        SELECT ${idx} AS idx, row_to_json(r.*) AS payload FROM existing_${idx} r
      `);
    });

    const script = `
      WITH
      ${ctes.join(",\n")}
      SELECT COALESCE(json_agg(payload ORDER BY idx ASC), '[]'::json)::text
      FROM (
        ${unions.join("\nUNION ALL\n")}
      ) all_rows;
    `;

    // Fault-injection point BEFORE sending/committing transaction to PostgreSQL.
    if (this.beforeCommit) {
      await this.beforeCommit(inputs);
    }

    const rawJson = await runPsqlStdin(this.pgEnv, script);
    if (!rawJson) {
      return [];
    }
    const uniqueOutcomes = (JSON.parse(rawJson) as RawReceiptRow[]).map(mapRawRow);
    if (uniqueOutcomes.length !== uniqueInputs.length) {
      throw new Error(
        `Receipt commit verification mismatch: expected ${uniqueInputs.length}, got ${uniqueOutcomes.length}`
      );
    }

    return Object.freeze(
      slotToUniqueIdx.map((slot) => {
        const outcome = uniqueOutcomes[slot.uniqueIdx]!;
        return slot.intraBatchDup
          ? { receipt: outcome.receipt, deduplicated: true }
          : outcome;
      })
    );
  }
}

export async function runPsqlStdin(
  pgEnv: NodeJS.ProcessEnv,
  sql: string
): Promise<string> {
  const { spawn } = await import("node:child_process");
  return new Promise<string>((resolve, reject) => {
    const child = spawn("psql", ["-v", "ON_ERROR_STOP=1", "-X", "-q", "-t", "-A"], {
      env: pgEnv,
      stdio: ["pipe", "pipe", "pipe"],
    });
    let out = "";
    let err = "";
    child.stdout.on("data", (chunk: Buffer) => {
      out += chunk.toString("utf8");
    });
    child.stderr.on("data", (chunk: Buffer) => {
      err += chunk.toString("utf8");
    });
    child.on("error", reject);
    child.on("close", (code) => {
      if (code !== 0) {
        reject(new Error(`psql exited with code ${code}: ${err.trim()}`));
        return;
      }
      resolve(out.trim());
    });
    child.stdin.end(`${sql}\n`);
  });
}

export { buildPgEnv, mapRawRow, sqlJsonbB64, sqlNullableTextB64, sqlTextB64, sqlTimestamptzLiteral, sqlUuidLiteral, type RawReceiptRow };
