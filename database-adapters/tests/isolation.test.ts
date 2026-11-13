import { after, before, describe, it } from "node:test";
import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";

function requireDatabaseUrl(): string {
  const value = process.env.DATABASE_URL;
  if (!value || value.trim().length === 0) {
    throw new Error("DATABASE_URL must be set to execute ID-01-01 storage/cursor integration tests");
  }
  return value;
}

const DB_URL: string = requireDatabaseUrl();

const MIGRATION_0001 = path.resolve(
  process.cwd(),
  "database-adapters/migrations/0001_tenant_and_chat.sql"
);
const MIGRATION_0002 = path.resolve(
  process.cwd(),
  "database-adapters/migrations/0002_conversation_message_indexes.sql"
);

const TENANT_A = "aaaa0101-1111-4111-8111-000000000001";
const TENANT_B = "bbbb0101-2222-4222-8222-000000000002";
const ACC_A = "aaaa0101-1111-4111-8111-aaaaaaaa0001";
const ACC_B = "bbbb0101-2222-4222-8222-bbbbbbbb0002";
const IDENT_A = "aaaa0101-1111-4111-8111-111111110001";
const IDENT_B = "bbbb0101-2222-4222-8222-222222220002";

const SYNTHETIC_APP_LOGIN = "cb_id0101_runtime_probe";
const SYNTHETIC_PROBE_PASS = "synthetic_id0101_probe_pw_01";

interface ExplainNode {
  "Node Type": string;
  "Index Name"?: string;
  Plans?: ExplainNode[];
}

interface ExplainRoot {
  Plan: ExplainNode;
}

function buildPgEnv(baseUrl: string, user?: string, password?: string): NodeJS.ProcessEnv {
  const u = new URL(baseUrl);
  const env: NodeJS.ProcessEnv = {
    ...process.env,
    PGHOST: u.hostname || "127.0.0.1",
    PGPORT: u.port || "5432",
    PGUSER: user ?? decodeURIComponent(u.username),
    PGPASSWORD: password ?? decodeURIComponent(u.password),
    PGDATABASE: u.pathname.replace(/^\//, ""),
    PGSSLMODE: u.searchParams.get("sslmode") ?? "disable",
  };
  delete env.DATABASE_URL;
  return env;
}

function runPsqlFile(pgEnv: NodeJS.ProcessEnv, filePath: string): void {
  execFileSync("psql", ["-v", "ON_ERROR_STOP=1", "-X", "-q", "-f", filePath], {
    env: { ...pgEnv, PGOPTIONS: "-c client_min_messages=warning" },
    encoding: "utf8",
  });
}

function runPsqlCommand(pgEnv: NodeJS.ProcessEnv, sql: string): string {
  return execFileSync("psql", ["-v", "ON_ERROR_STOP=1", "-X", "-q", "-t", "-A", "-c", sql], {
    env: pgEnv,
    encoding: "utf8",
  }).trim();
}

function runPsqlScript(pgEnv: NodeJS.ProcessEnv, sql: string): string {
  return execFileSync("psql", ["-v", "ON_ERROR_STOP=1", "-X", "-q", "-t", "-A"], {
    env: pgEnv,
    input: `${sql}\n`,
    encoding: "utf8",
  }).trim();
}

function runPsqlExpectFailure(pgEnv: NodeJS.ProcessEnv, sql: string): { status: number | null; stderr: string } {
  const res = spawnSync(
    "psql",
    ["-v", "ON_ERROR_STOP=1", "-v", "VERBOSITY=verbose", "-X", "-q"],
    {
      env: pgEnv,
      input: `${sql}\n`,
      encoding: "utf8",
    }
  );
  return { status: res.status, stderr: res.stderr ?? "" };
}

function cleanupFixture(pgEnv: NodeJS.ProcessEnv): void {
  runPsqlScript(
    pgEnv,
    `
    DELETE FROM messages WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}');
    DELETE FROM conversations WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}');
    DELETE FROM outbox_events WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}');
    DELETE FROM channel_identities WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}');
    DELETE FROM channel_accounts WHERE tenant_id IN ('${TENANT_A}', '${TENANT_B}');
    DELETE FROM tenants WHERE id IN ('${TENANT_A}', '${TENANT_B}');
    `
  );
}

function collectPlanNodes(node: ExplainNode): ExplainNode[] {
  const out: ExplainNode[] = [node];
  for (const child of node.Plans ?? []) {
    out.push(...collectPlanNodes(child));
  }
  return out;
}

describe("ID-01-01 ChatBridge conversation & message storage indexes and cursor tie pagination", () => {
  const ownerEnv = buildPgEnv(DB_URL);
  const appEnv = buildPgEnv(DB_URL, SYNTHETIC_APP_LOGIN, SYNTHETIC_PROBE_PASS);

  const convIdsA = Array.from({ length: 10 }, (_, idx) => {
    const hex = (idx + 1).toString(16).padStart(2, "0");
    return `c0a00101-1111-4111-8111-0000000000${hex}`;
  });
  const convIdsB = [
    "c0b00101-2222-4222-8222-000000000001",
    "c0b00101-2222-4222-8222-000000000002",
  ];

  before(() => {
    assert.ok(fs.existsSync(MIGRATION_0001), `Missing migration: ${MIGRATION_0001}`);
    assert.ok(fs.existsSync(MIGRATION_0002), `Missing migration: ${MIGRATION_0002}`);

    runPsqlFile(ownerEnv, MIGRATION_0001);
    runPsqlFile(ownerEnv, MIGRATION_0002);
    cleanupFixture(ownerEnv);

    runPsqlScript(
      ownerEnv,
      `
      DROP ROLE IF EXISTS ${SYNTHETIC_APP_LOGIN};
      CREATE ROLE ${SYNTHETIC_APP_LOGIN} LOGIN PASSWORD '${SYNTHETIC_PROBE_PASS}'
        NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS IN ROLE chatbridge_app;
      `
    );

    // Seed two tenants with 10 Tenant A conversations spanning tied timestamps and 2 Tenant B interleaved conversations.
    const t3 = "2026-11-13T10:30:00Z";
    const t2 = "2026-11-13T10:20:00Z";
    const t1 = "2026-11-13T10:10:00Z";

    // Assign 4 at t3 (including 1 with NULL last_message_at & created_at=t3), 4 at t2, 2 at t1.
    const convRowsA = convIdsA.map((cid, i) => {
      if (i === 0) {
        return `('${cid}', '${TENANT_A}', '${ACC_A}', '${IDENT_A}', 'OPEN', 0, NULL, '${t3}'::timestamptz)`;
      }
      const ts = i < 4 ? t3 : i < 8 ? t2 : t1;
      return `('${cid}', '${TENANT_A}', '${ACC_A}', '${IDENT_A}', 'OPEN', 9, '${ts}'::timestamptz, '${t1}'::timestamptz)`;
    });

    const msgRowsA: string[] = [];
    for (let seq = 1; seq <= 9; seq++) {
      const mId = `a0000101-1111-4111-8111-00000000000${seq}`;
      const ts = seq >= 7 ? t3 : seq >= 4 ? t2 : t1;
      msgRowsA.push(
        `('${mId}', '${TENANT_A}', '${convIdsA[1]}', ${seq}, 'IN', 'CUSTOMER', 'msg-${seq}', '${ts}'::timestamptz)`
      );
    }

    runPsqlScript(
      appEnv,
      `
      BEGIN;
      INSERT INTO tenants (id, slug, display_name) VALUES
        ('${TENANT_A}', 'id0101-alpha', 'Alpha Cursor Tenant'),
        ('${TENANT_B}', 'id0101-beta', 'Beta Cursor Tenant');

      INSERT INTO channel_accounts (id, tenant_id, provider, external_ref, display_name) VALUES
        ('${ACC_A}', '${TENANT_A}', 'LINE', 'line-alpha', 'Alpha LINE'),
        ('${ACC_B}', '${TENANT_B}', 'LINE', 'line-beta', 'Beta LINE');

      INSERT INTO channel_identities (id, tenant_id, channel_account_id, provider_sender_ref, scope_key) VALUES
        ('${IDENT_A}', '${TENANT_A}', '${ACC_A}', 'U-alpha', 'alpha:1'),
        ('${IDENT_B}', '${TENANT_B}', '${ACC_B}', 'U-beta', 'beta:1');

      INSERT INTO conversations (id, tenant_id, channel_account_id, identity_id, status, last_seq, last_message_at, created_at) VALUES
        ${convRowsA.join(",\n        ")},
        ('${convIdsB[0]}', '${TENANT_B}', '${ACC_B}', '${IDENT_B}', 'OPEN', 1, '${t3}'::timestamptz, '${t3}'::timestamptz),
        ('${convIdsB[1]}', '${TENANT_B}', '${ACC_B}', '${IDENT_B}', 'OPEN', 1, '${t2}'::timestamptz, '${t2}'::timestamptz);

      INSERT INTO messages (id, tenant_id, conversation_id, seq, direction, sender_kind, body, received_at) VALUES
        ${msgRowsA.join(",\n        ")},
        ('b0000101-2222-4222-8222-000000000001', '${TENANT_B}', '${convIdsB[0]}', 1, 'IN', 'CUSTOMER', 'beta-msg', '${t3}'::timestamptz);

      INSERT INTO outbox_events (id, tenant_id, aggregate_key, aggregate_version, type, payload_minimal, state)
      VALUES ('00000101-1111-4111-8111-000000000001', '${TENANT_A}', 'conv:${convIdsA[1]}', 1, 'message.created', '{"seq":9}'::jsonb, 'PENDING');
      COMMIT;
      `
    );
  });

  after(() => {
    cleanupFixture(ownerEnv);
    runPsqlCommand(ownerEnv, `DROP ROLE IF EXISTS ${SYNTHETIC_APP_LOGIN};`);
  });

  it("provisions all bounded storage & cursor indexes and enforces message tenant FK scope", () => {
    const idxList = runPsqlCommand(
      ownerEnv,
      `
      SELECT indexname
      FROM pg_indexes
      WHERE schemaname = 'public' AND indexname LIKE 'idx_%'
      ORDER BY indexname;
      `
    ).split(/\r?\n/);

    for (const expected of [
      "idx_conversation_assignments_active_lease",
      "idx_conversations_tenant_activity_cursor",
      "idx_conversations_tenant_status_activity_cursor",
      "idx_messages_tenant_conv_seq_desc",
      "idx_messages_tenant_conv_time_tie",
      "idx_outbound_intents_tenant_active",
      "idx_outbox_events_dispatch_due",
      "idx_outbox_events_tenant_stream",
      "idx_web_chat_browser_links_active_expiry",
    ]) {
      assert.ok(idxList.includes(expected), `Expected index missing: ${expected}`);
    }

    const crossTenantMsg = runPsqlExpectFailure(
      appEnv,
      `
      INSERT INTO messages (id, tenant_id, conversation_id, seq, direction, sender_kind, body)
      VALUES ('99990101-9999-4999-8999-000000000099', '${TENANT_B}', '${convIdsA[1]}', 10, 'IN', 'CUSTOMER', 'cross-tenant');
      `
    );
    assert.notEqual(crossTenantMsg.status, 0);
    assert.match(crossTenantMsg.stderr, /23503|foreign key constraint/i);
  });

  it("pages conversation timestamp ties and NULL last_message_at without skips, duplicates, or cross-tenant leaks", () => {
    const collected: string[] = [];
    let cursorTs: string | null = null;
    let cursorId: string | null = null;

    // Page size 3 cuts across 4-row timestamp tie groups twice.
    for (let page = 0; page < 10; page++) {
      const predicate =
        cursorTs === null || cursorId === null
          ? ""
          : `AND COALESCE(last_message_at, created_at) <= '${cursorTs}'::timestamptz
             AND (COALESCE(last_message_at, created_at) < '${cursorTs}'::timestamptz OR id < '${cursorId}'::uuid)`;

      const out = runPsqlCommand(
        appEnv,
        `
        SELECT id || '|' || to_char(COALESCE(last_message_at, created_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
        FROM conversations
        WHERE tenant_id = '${TENANT_A}'
          ${predicate}
        ORDER BY COALESCE(last_message_at, created_at) DESC, id DESC
        LIMIT 3;
        `
      );
      if (!out) {
        break;
      }
      const rows = out.split(/\r?\n/);
      for (const row of rows) {
        const [id, ts] = row.split("|");
        assert.ok(id && ts);
        assert.ok(convIdsA.includes(id), `Cross-tenant leak detected: ${id}`);
        collected.push(id);
        cursorId = id;
        cursorTs = ts;
      }
    }

    assert.equal(collected.length, 10);
    assert.equal(new Set(collected).size, 10);
    assert.deepEqual(collected, [
      convIdsA[3],
      convIdsA[2],
      convIdsA[1],
      convIdsA[0],
      convIdsA[7],
      convIdsA[6],
      convIdsA[5],
      convIdsA[4],
      convIdsA[9],
      convIdsA[8],
    ]);
  });

  it("pages message timestamp ties and reverse seq keyset cursors deterministically without skips or duplicates", () => {
    // 1. Keyset cursor by (received_at DESC, seq DESC) with page size 2 across 3-row timestamp ties
    const timePagedSeqs: number[] = [];
    let cursorTs: string | null = null;
    let cursorSeq: number | null = null;

    for (let page = 0; page < 10; page++) {
      const predicate =
        cursorTs === null || cursorSeq === null
          ? ""
          : `AND received_at <= '${cursorTs}'::timestamptz
             AND (received_at < '${cursorTs}'::timestamptz OR seq < ${cursorSeq})`;

      const out = runPsqlCommand(
        appEnv,
        `
        SELECT seq || '|' || to_char(received_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
        FROM messages
        WHERE tenant_id = '${TENANT_A}'
          AND conversation_id = '${convIdsA[1]}'
          ${predicate}
        ORDER BY received_at DESC, seq DESC
        LIMIT 2;
        `
      );
      if (!out) {
        break;
      }
      for (const row of out.split(/\r?\n/)) {
        const [seqStr, ts] = row.split("|");
        assert.ok(seqStr && ts);
        const seq = Number(seqStr);
        timePagedSeqs.push(seq);
        cursorSeq = seq;
        cursorTs = ts;
      }
    }
    assert.deepEqual(timePagedSeqs, [9, 8, 7, 6, 5, 4, 3, 2, 1]);

    // 2. Pure reverse seq cursor paging
    const seqPaged: number[] = [];
    let maxSeq: number | null = null;
    for (let page = 0; page < 10; page++) {
      const out = runPsqlCommand(
        appEnv,
        `
        SELECT seq
        FROM messages
        WHERE tenant_id = '${TENANT_A}'
          AND conversation_id = '${convIdsA[1]}'
          ${maxSeq === null ? "" : `AND seq < ${maxSeq}`}
        ORDER BY seq DESC
        LIMIT 3;
        `
      );
      if (!out) {
        break;
      }
      for (const s of out.split(/\r?\n/)) {
        const n = Number(s);
        seqPaged.push(n);
        maxSeq = n;
      }
    }
    assert.deepEqual(seqPaged, [9, 8, 7, 6, 5, 4, 3, 2, 1]);
  });

  it("verifies via PostgreSQL EXPLAIN that cursor queries use scoped indexes with zero outer Sort node", () => {
    const explainQueries: Array<{ name: string; expectedIndex: string; sql: string }> = [
      {
        name: "conversation_activity_tie_cursor",
        expectedIndex: "idx_conversations_tenant_activity_cursor",
        sql: `
          SELECT id
          FROM conversations
          WHERE tenant_id = '${TENANT_A}'
            AND COALESCE(last_message_at, created_at) <= '2026-11-13T10:30:00Z'::timestamptz
            AND (
              COALESCE(last_message_at, created_at) < '2026-11-13T10:30:00Z'::timestamptz
              OR id < '${convIdsA[3]}'::uuid
            )
          ORDER BY COALESCE(last_message_at, created_at) DESC, id DESC
          LIMIT 3
        `,
      },
      {
        name: "message_seq_reverse_cursor",
        expectedIndex: "idx_messages_tenant_conv_seq_desc",
        sql: `
          SELECT id, seq
          FROM messages
          WHERE tenant_id = '${TENANT_A}'
            AND conversation_id = '${convIdsA[1]}'
            AND seq < 8
          ORDER BY seq DESC
          LIMIT 3
        `,
      },
      {
        name: "message_time_tie_cursor",
        expectedIndex: "idx_messages_tenant_conv_time_tie",
        sql: `
          SELECT id, seq
          FROM messages
          WHERE tenant_id = '${TENANT_A}'
            AND conversation_id = '${convIdsA[1]}'
            AND received_at <= '2026-11-13T10:20:00Z'::timestamptz
            AND (received_at < '2026-11-13T10:20:00Z'::timestamptz OR seq < 6)
          ORDER BY received_at DESC, seq DESC
          LIMIT 2
        `,
      },
      {
        name: "outbox_pending_dispatch",
        expectedIndex: "idx_outbox_events_dispatch_due",
        sql: `
          SELECT id
          FROM outbox_events
          WHERE state IN ('PENDING', 'RETRY_WAIT')
            AND available_at <= NOW()
          ORDER BY available_at ASC, created_at ASC
          LIMIT 10
        `,
      },
    ];

    for (const q of explainQueries) {
      const jsonOut = runPsqlScript(
        appEnv,
        `
        BEGIN;
        SET LOCAL enable_seqscan = off;
        EXPLAIN (FORMAT JSON) ${q.sql};
        COMMIT;
        `
      );
      const parsed = JSON.parse(jsonOut) as ExplainRoot[];
      assert.ok(parsed[0]?.Plan, `Missing EXPLAIN Plan for ${q.name}`);
      const nodes = collectPlanNodes(parsed[0].Plan);
      const indexNodes = nodes.filter((n) => n["Index Name"] === q.expectedIndex);
      assert.ok(
        indexNodes.length > 0,
        `Expected index ${q.expectedIndex} in EXPLAIN for ${q.name}, got: ${nodes.map((n) => `${n["Node Type"]}(${n["Index Name"] ?? ""})`).join(", ")}`
      );
      if (q.name !== "outbox_pending_dispatch") {
        const sortNodes = nodes.filter((n) => n["Node Type"] === "Sort");
        assert.equal(
          sortNodes.length,
          0,
          `Unexpected Sort node above index scan in ${q.name}: ${JSON.stringify(nodes)}`
        );
      }
    }
  });
});
