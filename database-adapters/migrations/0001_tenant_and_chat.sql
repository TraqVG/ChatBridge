-- ID-01-04: ChatBridge base multi-tenant, channel, conversation, message, outbox & ownership migration
-- Authority: contracts/db-ownership.md (FD-03-07) & Implementation-Specification/01-Database.md (DB-02, DB-03, DB-10, DB-12)
-- Designed to execute idempotently under either cluster owner/superuser (CI) or dedicated chatbridge_migrator role.

BEGIN;

-- 1. Provision least-privilege runtime/migrator role definitions when cluster privileges allow.
DO $$
DECLARE
  can_manage_roles BOOLEAN := FALSE;
BEGIN
  SELECT (rolsuper OR rolcreaterole)
    INTO can_manage_roles
    FROM pg_roles
   WHERE rolname = current_user;

  IF can_manage_roles THEN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'chatbridge_app') THEN
      CREATE ROLE chatbridge_app NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
    ELSIF EXISTS (
      SELECT 1 FROM pg_roles
       WHERE rolname = 'chatbridge_app'
         AND (rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls)
    ) THEN
      ALTER ROLE chatbridge_app NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'chatbridge_migrator') THEN
      CREATE ROLE chatbridge_migrator NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
    ELSIF EXISTS (
      SELECT 1 FROM pg_roles
       WHERE rolname = 'chatbridge_migrator'
         AND (rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls)
    ) THEN
      ALTER ROLE chatbridge_migrator NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
    END IF;
  END IF;
END
$$;

-- 2. Harden database & schema grants: revoke PUBLIC and MangMie principals if present in shared cluster.
DO $$
DECLARE
  db_name TEXT := current_database();
  foreign_role TEXT;
BEGIN
  EXECUTE format('REVOKE ALL ON DATABASE %I FROM PUBLIC', db_name);
  REVOKE ALL ON SCHEMA public FROM PUBLIC;

  FOREACH foreign_role IN ARRAY ARRAY['mangmie_app', 'mangmie_migrator'] LOOP
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = foreign_role) THEN
      EXECUTE format('REVOKE ALL ON DATABASE %I FROM %I', db_name, foreign_role);
      EXECUTE format('REVOKE ALL ON SCHEMA public FROM %I', foreign_role);
    END IF;
  END LOOP;

  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'chatbridge_app') THEN
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO chatbridge_app', db_name);
    GRANT USAGE ON SCHEMA public TO chatbridge_app;
  END IF;

  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'chatbridge_migrator') THEN
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO chatbridge_migrator', db_name);
    GRANT USAGE, CREATE ON SCHEMA public TO chatbridge_migrator;
  END IF;
END
$$;

-- 3. Core Tenant & Integration Auth Tables
CREATE TABLE IF NOT EXISTS tenants (
  id UUID PRIMARY KEY,
  slug TEXT UNIQUE,
  display_name TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'SUSPENDED', 'ARCHIVED')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS integration_clients (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  client_key TEXT NOT NULL,
  secret_hash TEXT NOT NULL,
  secret_version INTEGER NOT NULL DEFAULT 1 CHECK (secret_version >= 1),
  scopes JSONB NOT NULL DEFAULT '[]'::jsonb,
  status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'ROTATING', 'REVOKED')),
  rotated_at TIMESTAMPTZ NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, id),
  UNIQUE (tenant_id, client_key)
);

-- 4. Channel Accounts, Credentials & Scoped Identities
CREATE TABLE IF NOT EXISTS channel_accounts (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  provider TEXT NOT NULL CHECK (provider IN ('LINE', 'TIKTOK', 'WEB')),
  external_ref TEXT NULL,
  display_name TEXT NOT NULL,
  capabilities JSONB NOT NULL DEFAULT '{}'::jsonb,
  connection_status TEXT NOT NULL DEFAULT 'PENDING'
    CHECK (connection_status IN ('PENDING', 'CONNECTED', 'DEGRADED', 'DISCONNECTED', 'REVOKED')),
  credential_ref TEXT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, id)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_channel_accounts_tenant_provider_external_ref
  ON channel_accounts (tenant_id, provider, external_ref)
  WHERE external_ref IS NOT NULL;

CREATE TABLE IF NOT EXISTS channel_credentials (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  channel_account_id UUID NOT NULL,
  encrypted_ref TEXT NOT NULL,
  key_version INTEGER NOT NULL DEFAULT 1 CHECK (key_version >= 1),
  expiry_at TIMESTAMPTZ NULL,
  revoked_at TIMESTAMPTZ NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, id),
  UNIQUE (tenant_id, channel_account_id),
  FOREIGN KEY (tenant_id, channel_account_id)
    REFERENCES channel_accounts (tenant_id, id)
);

-- 5. Native Web Chat (Public Site Key, Pseudonymous Guest & 60-Day Remembered Browser Links)
CREATE TABLE IF NOT EXISTS web_chat_sites (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  channel_account_id UUID NOT NULL,
  site_key TEXT NOT NULL UNIQUE,
  allowed_origins JSONB NOT NULL DEFAULT '[]'::jsonb,
  status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'SUSPENDED', 'REVOKED')),
  revoked_at TIMESTAMPTZ NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, id),
  FOREIGN KEY (tenant_id, channel_account_id)
    REFERENCES channel_accounts (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS web_chat_participants (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  site_id UUID NOT NULL,
  pseudonymous_handle TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, id),
  UNIQUE (tenant_id, site_id, id),
  FOREIGN KEY (tenant_id, site_id)
    REFERENCES web_chat_sites (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS web_chat_browser_links (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  site_id UUID NOT NULL,
  participant_id UUID NOT NULL,
  recognition_secret_hash TEXT NOT NULL,
  issued_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  expires_at TIMESTAMPTZ NOT NULL,
  revoked_at TIMESTAMPTZ NULL,
  UNIQUE (tenant_id, id),
  UNIQUE (tenant_id, site_id, recognition_secret_hash),
  CHECK (expires_at > issued_at AND expires_at <= issued_at + INTERVAL '60 days'),
  FOREIGN KEY (tenant_id, site_id, participant_id)
    REFERENCES web_chat_participants (tenant_id, site_id, id)
);

CREATE TABLE IF NOT EXISTS web_chat_sessions (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  site_id UUID NOT NULL,
  participant_id UUID NOT NULL,
  token_hash TEXT NOT NULL UNIQUE,
  issued_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  expires_at TIMESTAMPTZ NOT NULL,
  revoked_at TIMESTAMPTZ NULL,
  rotated_from_id UUID NULL,
  UNIQUE (tenant_id, id),
  CHECK (expires_at > issued_at),
  FOREIGN KEY (tenant_id, site_id, participant_id)
    REFERENCES web_chat_participants (tenant_id, site_id, id),
  FOREIGN KEY (tenant_id, rotated_from_id)
    REFERENCES web_chat_sessions (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS channel_identities (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  channel_account_id UUID NOT NULL,
  provider_sender_ref TEXT NULL,
  scope_key TEXT NOT NULL,
  display_name TEXT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, id),
  FOREIGN KEY (tenant_id, channel_account_id)
    REFERENCES channel_accounts (tenant_id, id)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_channel_identities_tenant_account_sender_ref
  ON channel_identities (tenant_id, channel_account_id, provider_sender_ref)
  WHERE provider_sender_ref IS NOT NULL;

-- 6. Private Omega Data (SeaweedFS) Media Assets — opaque object keys only, never presigned URLs
CREATE TABLE IF NOT EXISTS media_assets (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  content_type TEXT NOT NULL,
  bytes BIGINT NOT NULL CHECK (bytes >= 0),
  sha256_hex TEXT NULL,
  provider_ref TEXT NULL,
  storage_provider TEXT NOT NULL DEFAULT 'OMEGA_SEAWEEDFS'
    CHECK (storage_provider IN ('OMEGA_SEAWEEDFS')),
  storage_object_key TEXT NULL
    CHECK (
      storage_object_key IS NULL
      OR (storage_object_key !~* '^https?://' AND position('X-Amz-Signature' in storage_object_key) = 0)
    ),
  thumbnail_key TEXT NULL
    CHECK (
      thumbnail_key IS NULL
      OR (thumbnail_key !~* '^https?://' AND position('X-Amz-Signature' in thumbnail_key) = 0)
    ),
  ingest_state TEXT NOT NULL DEFAULT 'PENDING'
    CHECK (ingest_state IN ('PENDING', 'STORED', 'FAILED', 'DELETED')),
  quarantine_state TEXT NOT NULL DEFAULT 'PENDING'
    CHECK (quarantine_state IN ('PENDING', 'CLEAN', 'QUARANTINED', 'BLOCKED')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, id)
);

-- 7. Conversations, Participants, Messages & Parts
CREATE TABLE IF NOT EXISTS conversations (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  channel_account_id UUID NOT NULL,
  identity_id UUID NULL,
  web_participant_id UUID NULL,
  status TEXT NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN', 'RESOLVED')),
  last_seq BIGINT NOT NULL DEFAULT 0 CHECK (last_seq >= 0),
  last_message_at TIMESTAMPTZ NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  version BIGINT NOT NULL DEFAULT 1 CHECK (version >= 1),
  UNIQUE (tenant_id, id),
  FOREIGN KEY (tenant_id, channel_account_id)
    REFERENCES channel_accounts (tenant_id, id),
  FOREIGN KEY (tenant_id, identity_id)
    REFERENCES channel_identities (tenant_id, id),
  FOREIGN KEY (tenant_id, web_participant_id)
    REFERENCES web_chat_participants (tenant_id, id),
  CHECK ((identity_id IS NOT NULL)::int + (web_participant_id IS NOT NULL)::int >= 1)
);

CREATE TABLE IF NOT EXISTS conversation_participants (
  tenant_id UUID NOT NULL,
  conversation_id UUID NOT NULL,
  actor_kind TEXT NOT NULL CHECK (actor_kind IN ('CUSTOMER', 'WEB_GUEST', 'STAFF', 'ADMIN', 'OWNER', 'AI', 'SYSTEM')),
  actor_id TEXT NOT NULL,
  role TEXT NOT NULL,
  joined_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, conversation_id, actor_kind, actor_id),
  FOREIGN KEY (tenant_id, conversation_id)
    REFERENCES conversations (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS messages (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  conversation_id UUID NOT NULL,
  seq BIGINT NOT NULL CHECK (seq >= 1),
  direction TEXT NOT NULL CHECK (direction IN ('IN', 'OUT')),
  sender_kind TEXT NOT NULL CHECK (sender_kind IN ('CUSTOMER', 'WEB_GUEST', 'STAFF', 'ADMIN', 'OWNER', 'AI', 'SYSTEM')),
  sender_ref TEXT NULL,
  kind TEXT NOT NULL DEFAULT 'TEXT',
  body TEXT NULL,
  received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  provider_at TIMESTAMPTZ NULL,
  delivery_state TEXT NOT NULL DEFAULT 'RECEIVED'
    CHECK (delivery_state IN ('RECEIVED', 'QUEUED', 'PROCESSING', 'SENT', 'DELIVERED', 'FAILED', 'UNKNOWN', 'UNSUPPORTED')),
  historical_import BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, id),
  UNIQUE (tenant_id, conversation_id, seq),
  FOREIGN KEY (tenant_id, conversation_id)
    REFERENCES conversations (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS message_parts (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  message_id UUID NOT NULL,
  part_index INTEGER NOT NULL CHECK (part_index >= 0),
  kind TEXT NOT NULL CHECK (kind IN ('TEXT', 'STICKER', 'IMAGE', 'FILE', 'VIDEO', 'AUDIO', 'LINK', 'UNSUPPORTED')),
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  media_asset_id UUID NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, id),
  UNIQUE (tenant_id, message_id, part_index),
  FOREIGN KEY (tenant_id, message_id)
    REFERENCES messages (tenant_id, id),
  FOREIGN KEY (tenant_id, media_asset_id)
    REFERENCES media_assets (tenant_id, id)
);

-- 8. Webhook Ingress, Historical Sync Checkpoints & Deduplication
CREATE TABLE IF NOT EXISTS webhook_receipts (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  channel_account_id UUID NOT NULL,
  scoped_provider_key TEXT NULL,
  body_hash TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'RECEIVED'
    CHECK (state IN ('RECEIVED', 'VERIFIED', 'PROCESSED', 'REJECTED', 'FAILED')),
  received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  processed_at TIMESTAMPTZ NULL,
  UNIQUE (tenant_id, id),
  FOREIGN KEY (tenant_id, channel_account_id)
    REFERENCES channel_accounts (tenant_id, id)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_webhook_receipts_tenant_account_scoped_key
  ON webhook_receipts (tenant_id, channel_account_id, scoped_provider_key)
  WHERE scoped_provider_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS provider_history_sync_runs (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  channel_account_id UUID NOT NULL,
  mode TEXT NOT NULL CHECK (mode IN ('INITIAL', 'INCREMENTAL', 'RECONCILE')),
  status TEXT NOT NULL DEFAULT 'RUNNING'
    CHECK (status IN ('RUNNING', 'COMPLETED', 'PARTIAL', 'FAILED')),
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ NULL,
  UNIQUE (tenant_id, id),
  FOREIGN KEY (tenant_id, channel_account_id)
    REFERENCES channel_accounts (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS provider_history_checkpoints (
  tenant_id UUID NOT NULL,
  channel_account_id UUID NOT NULL,
  conversation_ref TEXT NOT NULL,
  forward_cursor TEXT NULL,
  backward_cursor TEXT NULL,
  coverage_state TEXT NOT NULL CHECK (coverage_state IN ('COMPLETE', 'PARTIAL', 'UNSUPPORTED')),
  last_synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, channel_account_id, conversation_ref),
  FOREIGN KEY (tenant_id, channel_account_id)
    REFERENCES channel_accounts (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS message_source_refs (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  message_id UUID NOT NULL,
  channel_account_id UUID NOT NULL,
  source_kind TEXT NOT NULL CHECK (source_kind IN ('WEBHOOK', 'HISTORY_SYNC', 'OUTBOUND', 'MANUAL_IMPORT')),
  source_fingerprint TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, id),
  UNIQUE (tenant_id, channel_account_id, source_kind, source_fingerprint),
  FOREIGN KEY (tenant_id, message_id)
    REFERENCES messages (tenant_id, id),
  FOREIGN KEY (tenant_id, channel_account_id)
    REFERENCES channel_accounts (tenant_id, id)
);

-- 9. Outbound Intents & Delivery Reconciliation (no blind retry on UNKNOWN)
CREATE TABLE IF NOT EXISTS outbound_intents (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  conversation_id UUID NOT NULL,
  message_id UUID NULL,
  idempotency_key TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  actor_ref TEXT NOT NULL,
  source TEXT NOT NULL CHECK (source IN ('STAFF', 'ADMIN', 'OWNER', 'AI_AUTO', 'SYSTEM')),
  state TEXT NOT NULL DEFAULT 'QUEUED'
    CHECK (state IN ('QUEUED', 'PROCESSING', 'SENT', 'FAILED', 'UNKNOWN')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, id),
  UNIQUE (tenant_id, conversation_id, idempotency_key),
  FOREIGN KEY (tenant_id, conversation_id)
    REFERENCES conversations (tenant_id, id),
  FOREIGN KEY (tenant_id, message_id)
    REFERENCES messages (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS delivery_attempts (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  intent_id UUID NOT NULL,
  attempt_number INTEGER NOT NULL CHECK (attempt_number >= 1),
  request_id TEXT NOT NULL,
  provider_result JSONB NOT NULL DEFAULT '{}'::jsonb,
  state TEXT NOT NULL CHECK (state IN ('PENDING', 'SENT', 'FAILED', 'UNKNOWN')),
  attempted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, id),
  UNIQUE (tenant_id, intent_id, attempt_number),
  FOREIGN KEY (tenant_id, intent_id)
    REFERENCES outbound_intents (tenant_id, id)
);

-- 10. Read Cursors, Auto-Claim Leases & Immutable Work Audit Events
CREATE TABLE IF NOT EXISTS conversation_read_cursors (
  tenant_id UUID NOT NULL,
  conversation_id UUID NOT NULL,
  staff_principal_id TEXT NOT NULL,
  last_read_seq BIGINT NOT NULL DEFAULT 0 CHECK (last_read_seq >= 0),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, conversation_id, staff_principal_id),
  FOREIGN KEY (tenant_id, conversation_id)
    REFERENCES conversations (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS conversation_assignments (
  tenant_id UUID NOT NULL,
  conversation_id UUID NOT NULL,
  claimant_principal_id TEXT NULL,
  claim_version BIGINT NOT NULL DEFAULT 1 CHECK (claim_version >= 1),
  lease_id UUID NULL,
  lease_until TIMESTAMPTZ NULL,
  claimed_at TIMESTAMPTZ NULL,
  released_at TIMESTAMPTZ NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, conversation_id),
  FOREIGN KEY (tenant_id, conversation_id)
    REFERENCES conversations (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS conversation_work_events (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  conversation_id UUID NOT NULL,
  event_kind TEXT NOT NULL CHECK (
    event_kind IN ('CLAIMED', 'RELEASED', 'EXPIRED_RELEASED', 'REOPENED', 'RESOLVED', 'OWNER_OVERRIDE_REPLY')
  ),
  actor_ref TEXT NOT NULL,
  from_assignee TEXT NULL,
  to_assignee TEXT NULL,
  version_before BIGINT NULL,
  version_after BIGINT NOT NULL,
  reason TEXT NULL,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, id),
  FOREIGN KEY (tenant_id, conversation_id)
    REFERENCES conversations (tenant_id, id)
);

-- 11. Transactional Outbox, Consumer Inbox Deduplication & Audit Logs
CREATE TABLE IF NOT EXISTS outbox_events (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  aggregate_key TEXT NOT NULL,
  aggregate_version BIGINT NOT NULL CHECK (aggregate_version >= 1),
  type TEXT NOT NULL,
  schema_version TEXT NOT NULL DEFAULT '1.0.0',
  payload_minimal JSONB NOT NULL,
  state TEXT NOT NULL DEFAULT 'PENDING'
    CHECK (state IN ('PENDING', 'CLAIMED', 'SUCCEEDED', 'RETRY_WAIT', 'QUARANTINED')),
  available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  locked_until TIMESTAMPTZ NULL,
  attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, id),
  UNIQUE (tenant_id, aggregate_key, aggregate_version, type)
);

CREATE TABLE IF NOT EXISTS inbox_receipts (
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  consumer TEXT NOT NULL,
  external_event_id TEXT NOT NULL,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, consumer, external_event_id)
);

CREATE TABLE IF NOT EXISTS audit_logs (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  actor TEXT NOT NULL,
  action TEXT NOT NULL,
  result TEXT NOT NULL CHECK (result IN ('ALLOW', 'DENY', 'SUCCESS', 'FAILURE')),
  target TEXT NOT NULL,
  trace_id TEXT NULL,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  metadata_redacted JSONB NOT NULL DEFAULT '{}'::jsonb,
  UNIQUE (tenant_id, id)
);

-- 12. Enforce least-privilege DML on all schema tables for runtime chatbridge_app (zero DDL, zero PUBLIC access).
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM PUBLIC;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM PUBLIC;

DO $$
DECLARE
  foreign_role TEXT;
BEGIN
  FOREACH foreign_role IN ARRAY ARRAY['mangmie_app', 'mangmie_migrator'] LOOP
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = foreign_role) THEN
      EXECUTE format('REVOKE ALL ON ALL TABLES IN SCHEMA public FROM %I', foreign_role);
      EXECUTE format('REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM %I', foreign_role);
    END IF;
  END LOOP;

  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'chatbridge_app') THEN
    GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO chatbridge_app;
    GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO chatbridge_app;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE ALL ON TABLES FROM PUBLIC;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO chatbridge_app;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO chatbridge_app;
  END IF;
END
$$;

COMMIT;
