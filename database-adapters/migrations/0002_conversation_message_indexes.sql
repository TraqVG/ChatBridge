-- ID-01-01: ChatBridge conversation, message, claim lease & outbox storage indexes
-- Authority: Implementation-Specification/01-Database.md (DB-03.1) & contracts/db-ownership.md (T22 additive expand)
-- Bounded index set supporting tenant-scoped keyset pagination with deterministic timestamp-tie breaking.

BEGIN;

-- 1. Conversation inbox pagination ordered by latest activity (COALESCE falls back to created_at) + UUID tie-breaker.
CREATE INDEX IF NOT EXISTS idx_conversations_tenant_activity_cursor
  ON conversations (tenant_id, (COALESCE(last_message_at, created_at)) DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_conversations_tenant_status_activity_cursor
  ON conversations (tenant_id, status, (COALESCE(last_message_at, created_at)) DESC, id DESC);

-- 2. Message reverse sequence pagination & timestamp-tie stable cursor pagination within a tenant conversation.
CREATE INDEX IF NOT EXISTS idx_messages_tenant_conv_seq_desc
  ON messages (tenant_id, conversation_id, seq DESC);

CREATE INDEX IF NOT EXISTS idx_messages_tenant_conv_time_tie
  ON messages (tenant_id, conversation_id, received_at DESC, seq DESC, id DESC);

-- 3. Stale Auto-Claim lease reaper partial index (DB-03.1).
CREATE INDEX IF NOT EXISTS idx_conversation_assignments_active_lease
  ON conversation_assignments (lease_until)
  WHERE claimant_principal_id IS NOT NULL AND lease_until IS NOT NULL;

-- 4. Durable outbox dispatch & tenant realtime stream catchup indexes (DB-03.1, DD-06).
CREATE INDEX IF NOT EXISTS idx_outbox_events_dispatch_due
  ON outbox_events (state, available_at ASC, created_at ASC)
  WHERE state IN ('PENDING', 'RETRY_WAIT');

CREATE INDEX IF NOT EXISTS idx_outbox_events_tenant_stream
  ON outbox_events (tenant_id, created_at ASC, id ASC);

-- 5. Active outbound reconciliation & non-revoked 60-day browser link expiry partial indexes.
CREATE INDEX IF NOT EXISTS idx_outbound_intents_tenant_active
  ON outbound_intents (tenant_id, state, created_at ASC)
  WHERE state IN ('QUEUED', 'PROCESSING', 'UNKNOWN');

CREATE INDEX IF NOT EXISTS idx_web_chat_browser_links_active_expiry
  ON web_chat_browser_links (expires_at)
  WHERE revoked_at IS NULL;

-- Preserve least-privilege DML on existing/future tables for chatbridge_app (no DDL).
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'chatbridge_app') THEN
    GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO chatbridge_app;
  END IF;
END
$$;

COMMIT;
