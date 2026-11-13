BEGIN;

-- CB-02-01: Additive durable inbound receipt schema & scoped deduplication indexes.
-- Compatible with existing 0001_tenant_and_chat.sql and 0002_conversation_message_indexes.sql.

ALTER TABLE webhook_receipts
  ADD COLUMN IF NOT EXISTS provider TEXT NOT NULL DEFAULT 'line_oa'
    CHECK (provider IN ('line_oa', 'tiktok', 'web_chat')),
  ADD COLUMN IF NOT EXISTS event_kind TEXT NOT NULL DEFAULT 'message.inbound',
  ADD COLUMN IF NOT EXISTS provider_event_id TEXT NULL,
  ADD COLUMN IF NOT EXISTS provider_message_id TEXT NULL,
  ADD COLUMN IF NOT EXISTS canonical_dedup_key TEXT NULL,
  ADD COLUMN IF NOT EXISTS dedup_strategy TEXT NOT NULL DEFAULT 'CANONICAL_BODY_FALLBACK'
    CHECK (dedup_strategy IN (
      'PROVIDER_EVENT_ID',
      'PROVIDER_MESSAGE_ID',
      'WEB_CHAT_CLIENT_MESSAGE_ID',
      'CANONICAL_BODY_FALLBACK'
    )),
  ADD COLUMN IF NOT EXISTS signature_verified BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS correlation_id TEXT NULL,
  ADD COLUMN IF NOT EXISTS payload_redacted JSONB NOT NULL DEFAULT '{}'::jsonb;

UPDATE webhook_receipts
SET canonical_dedup_key = COALESCE(
  canonical_dedup_key,
  NULLIF(BTRIM(scoped_provider_key), ''),
  'body:' || event_kind || ':' || body_hash
)
WHERE canonical_dedup_key IS NULL;

ALTER TABLE webhook_receipts
  ALTER COLUMN canonical_dedup_key SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_webhook_receipts_tenant_account_canonical_key
  ON webhook_receipts (tenant_id, channel_account_id, canonical_dedup_key);

CREATE UNIQUE INDEX IF NOT EXISTS uq_webhook_receipts_tenant_account_kind_msg
  ON webhook_receipts (tenant_id, channel_account_id, event_kind, provider_message_id)
  WHERE provider_message_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_webhook_receipts_tenant_account_kind_evt
  ON webhook_receipts (tenant_id, channel_account_id, event_kind, provider_event_id)
  WHERE provider_event_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_webhook_receipts_tenant_pending
  ON webhook_receipts (tenant_id, state, received_at ASC, id ASC)
  WHERE state IN ('RECEIVED', 'VERIFIED');

COMMIT;
