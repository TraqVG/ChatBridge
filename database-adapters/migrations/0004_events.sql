BEGIN;

-- CB-03-03: Additive ChatBridge transactional outbox & consumer inbox durability migration.
-- Compatible with 0001_tenant_and_chat.sql, 0002_conversation_message_indexes.sql, and 0003_receipts.sql.

CREATE SEQUENCE IF NOT EXISTS outbox_stream_seq
  AS BIGINT
  START WITH 1
  INCREMENT BY 1
  MINVALUE 1
  NO CYCLE;

ALTER TABLE outbox_events
  ADD COLUMN IF NOT EXISTS channel_account_id UUID NULL,
  ADD COLUMN IF NOT EXISTS event_schema_version INTEGER NOT NULL DEFAULT 1
    CHECK (event_schema_version >= 1),
  ADD COLUMN IF NOT EXISTS contract_revision TEXT NOT NULL DEFAULT '1.0.0'
    CHECK (contract_revision ~ '^[0-9]+\.[0-9]+\.[0-9]+$'),
  ADD COLUMN IF NOT EXISTS producer TEXT NOT NULL DEFAULT 'chatbridge',
  ADD COLUMN IF NOT EXISTS owner_db TEXT NOT NULL DEFAULT 'chatbridge_db',
  ADD COLUMN IF NOT EXISTS aggregate_type TEXT NOT NULL DEFAULT 'conversation'
    CHECK (aggregate_type ~ '^[a-z][a-z0-9_]*$'),
  ADD COLUMN IF NOT EXISTS outbox_seq BIGINT NOT NULL DEFAULT nextval('outbox_stream_seq')
    CHECK (outbox_seq >= 1),
  ADD COLUMN IF NOT EXISTS cursor_token TEXT NULL,
  ADD COLUMN IF NOT EXISTS is_replay BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS replay_id UUID NULL,
  ADD COLUMN IF NOT EXISTS original_occurred_at TIMESTAMPTZ NULL,
  ADD COLUMN IF NOT EXISTS historical_import BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS suppress_side_effects BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS source_kind TEXT NOT NULL DEFAULT 'LIVE_WEBHOOK'
    CHECK (source_kind IN ('LIVE_WEBHOOK', 'LIVE_API', 'INCREMENTAL_SYNC', 'INITIAL_BACKFILL', 'CSV_IMPORT')),
  ADD COLUMN IF NOT EXISTS occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  ADD COLUMN IF NOT EXISTS correlation_id TEXT NOT NULL DEFAULT 'corr-unassigned'
    CHECK (char_length(correlation_id) >= 8),
  ADD COLUMN IF NOT EXISTS claimed_by TEXT NULL,
  ADD COLUMN IF NOT EXISTS published_at TIMESTAMPTZ NULL,
  ADD COLUMN IF NOT EXISTS last_error_safe TEXT NULL;

UPDATE outbox_events
SET cursor_token = replace(encode(convert_to('cb_seq:' || outbox_seq::text, 'utf8'), 'base64'), E'\n', '')
WHERE cursor_token IS NULL;

ALTER TABLE outbox_events
  ALTER COLUMN cursor_token SET NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'fk_outbox_events_tenant_channel_account'
  ) THEN
    ALTER TABLE outbox_events
      ADD CONSTRAINT fk_outbox_events_tenant_channel_account
      FOREIGN KEY (tenant_id, channel_account_id)
      REFERENCES channel_accounts (tenant_id, id);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'chk_outbox_historical_suppress'
  ) THEN
    ALTER TABLE outbox_events
      ADD CONSTRAINT chk_outbox_historical_suppress
      CHECK (NOT historical_import OR suppress_side_effects);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'chk_outbox_replay_fields'
  ) THEN
    ALTER TABLE outbox_events
      ADD CONSTRAINT chk_outbox_replay_fields
      CHECK (NOT is_replay OR (replay_id IS NOT NULL AND original_occurred_at IS NOT NULL));
  END IF;
END
$$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_outbox_events_outbox_seq
  ON outbox_events (outbox_seq);

CREATE UNIQUE INDEX IF NOT EXISTS uq_outbox_events_tenant_stream_seq
  ON outbox_events (tenant_id, outbox_seq);

CREATE INDEX IF NOT EXISTS idx_outbox_events_claimed_lease
  ON outbox_events (locked_until ASC)
  WHERE state = 'CLAIMED' AND locked_until IS NOT NULL;

ALTER TABLE inbox_receipts
  ADD COLUMN IF NOT EXISTS producer TEXT NOT NULL DEFAULT 'chatbridge',
  ADD COLUMN IF NOT EXISTS event_type TEXT NOT NULL DEFAULT 'unknown.event',
  ADD COLUMN IF NOT EXISTS event_schema_version INTEGER NOT NULL DEFAULT 1
    CHECK (event_schema_version >= 1),
  ADD COLUMN IF NOT EXISTS contract_revision TEXT NOT NULL DEFAULT '1.0.0'
    CHECK (contract_revision ~ '^[0-9]+\.[0-9]+\.[0-9]+$'),
  ADD COLUMN IF NOT EXISTS aggregate_type TEXT NOT NULL DEFAULT 'conversation',
  ADD COLUMN IF NOT EXISTS aggregate_key TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS aggregate_seq BIGINT NOT NULL DEFAULT 1
    CHECK (aggregate_seq >= 1),
  ADD COLUMN IF NOT EXISTS historical_import BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS suppress_side_effects BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS source_kind TEXT NOT NULL DEFAULT 'LIVE_WEBHOOK'
    CHECK (source_kind IN ('LIVE_WEBHOOK', 'LIVE_API', 'INCREMENTAL_SYNC', 'INITIAL_BACKFILL', 'CSV_IMPORT')),
  ADD COLUMN IF NOT EXISTS correlation_id TEXT NULL,
  ADD COLUMN IF NOT EXISTS delivery_outcome TEXT NOT NULL DEFAULT 'APPLIED'
    CHECK (delivery_outcome IN ('APPLIED', 'SUPPRESSED_HISTORY', 'IGNORED_STALE')),
  ADD COLUMN IF NOT EXISTS effect_count INTEGER NOT NULL DEFAULT 0
    CHECK (effect_count >= 0);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'chk_inbox_historical_suppress'
  ) THEN
    ALTER TABLE inbox_receipts
      ADD CONSTRAINT chk_inbox_historical_suppress
      CHECK (NOT historical_import OR suppress_side_effects);
  END IF;
END
$$;

CREATE TABLE IF NOT EXISTS consumer_aggregate_cursors (
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  consumer TEXT NOT NULL,
  aggregate_type TEXT NOT NULL,
  aggregate_key TEXT NOT NULL,
  last_applied_aggregate_seq BIGINT NOT NULL CHECK (last_applied_aggregate_seq >= 1),
  last_event_id UUID NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, consumer, aggregate_type, aggregate_key)
);

CREATE INDEX IF NOT EXISTS idx_inbox_receipts_tenant_consumer_processed
  ON inbox_receipts (tenant_id, consumer, processed_at DESC);

-- Preserve least-privilege DML/sequence grants for runtime chatbridge_app (no DDL, zero PUBLIC access).
REVOKE ALL ON TABLE consumer_aggregate_cursors FROM PUBLIC;
REVOKE ALL ON SEQUENCE outbox_stream_seq FROM PUBLIC;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'chatbridge_app') THEN
    GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO chatbridge_app;
    GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO chatbridge_app;
  END IF;
END
$$;

COMMIT;
