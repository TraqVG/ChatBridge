export type ChatBridgeRuntimeEnv = "development" | "test" | "staging";

export interface ChatBridgeServiceSecrets {
  readonly databaseUrl: string;
  readonly integrationHmacSecret: string;
  readonly webhookSigningSecret: string;
  readonly objectStorageAccessKeyId: string;
  readonly objectStorageSecretAccessKey: string;
}

export interface ChatBridgeConfig {
  readonly serviceName: "chatbridge";
  readonly version: string;
  readonly nodeEnv: ChatBridgeRuntimeEnv;
  readonly host: string;
  readonly port: number;
  readonly objectStorageEndpoint: string;
  readonly workerPollIntervalMs: number;
  readonly allowBlindSendRetryOnUnknown: false;
  readonly secrets: ChatBridgeServiceSecrets;
}

export interface RedactedChatBridgeConfig {
  readonly serviceName: "chatbridge";
  readonly version: string;
  readonly nodeEnv: ChatBridgeRuntimeEnv;
  readonly host: string;
  readonly port: number;
  readonly objectStorageEndpoint: string;
  readonly workerPollIntervalMs: number;
  readonly allowBlindSendRetryOnUnknown: false;
  readonly standaloneMode: true;
  readonly credentialsConfigured: {
    readonly databaseUrl: "[REDACTED]";
    readonly integrationHmacSecret: "[REDACTED]";
    readonly webhookSigningSecret: "[REDACTED]";
    readonly objectStorageAccessKeyId: "[REDACTED]";
    readonly objectStorageSecretAccessKey: "[REDACTED]";
  };
}

export class ChatBridgeConfigError extends Error {
  public readonly issues: readonly string[];

  constructor(issues: readonly string[]) {
    super(`ChatBridge configuration validation failed (fail-closed): ${issues.join("; ")}`);
    this.name = "ChatBridgeConfigError";
    this.issues = issues;
  }
}

export const SYNTHETIC_CHATBRIDGE_DEV_ENV = Object.freeze({
  CHATBRIDGE_ENV: "development",
  CHATBRIDGE_HOST: "127.0.0.1",
  CHATBRIDGE_PORT: "4200",
  CHATBRIDGE_DATABASE_URL:
    "postgres://chatbridge_app:synthetic_dev_password_only@127.0.0.1:5432/chatbridge_db",
  CHATBRIDGE_INTEGRATION_HMAC_SECRET:
    "synthetic-dev-integration-hmac-secret-minimum-32-chars!!",
  CHATBRIDGE_WEBHOOK_SIGNING_SECRET:
    "synthetic-dev-webhook-signing-secret-minimum-32-chars!!",
  CHATBRIDGE_OBJECT_STORAGE_ENDPOINT: "http://127.0.0.1:8333",
  CHATBRIDGE_OBJECT_STORAGE_ACCESS_KEY_ID: "synthetic-storage-access-key",
  CHATBRIDGE_OBJECT_STORAGE_SECRET_ACCESS_KEY:
    "synthetic-storage-secret-key-minimum-24-chars",
  CHATBRIDGE_WORKER_POLL_INTERVAL_MS: "2000",
  CHATBRIDGE_ALLOW_BLIND_SEND_RETRY: "false",
} as const);

function isValidHttpUrl(raw: string): boolean {
  try {
    const parsed = new URL(raw);
    return parsed.protocol === "http:" || parsed.protocol === "https:";
  } catch {
    return false;
  }
}

function isValidChatBridgePostgresUrl(raw: string): boolean {
  try {
    const parsed = new URL(raw);
    if (parsed.protocol !== "postgres:" && parsed.protocol !== "postgresql:") {
      return false;
    }
    const dbName = parsed.pathname.replace(/^\//, "");
    if (!dbName || !dbName.toLowerCase().includes("chatbridge")) {
      return false;
    }
    return Boolean(parsed.hostname);
  } catch {
    return false;
  }
}

/**
 * Loads and validates standalone ChatBridge configuration.
 * ChatBridge does NOT require any external storefront or Auth Gate configuration to boot.
 * Fails closed on missing or invalid required ChatBridge settings.
 */
export function loadChatBridgeConfig(
  env: Readonly<Record<string, string | undefined>> = process.env
): ChatBridgeConfig {
  const issues: string[] = [];

  const rawEnv = env.CHATBRIDGE_ENV?.trim();
  if (!rawEnv) {
    issues.push("CHATBRIDGE_ENV is required (allowed: development | test | staging)");
  } else if (rawEnv !== "development" && rawEnv !== "test" && rawEnv !== "staging") {
    issues.push(
      `CHATBRIDGE_ENV "${rawEnv}" is invalid or unauthorized for non-production bootstrap`
    );
  }

  const host = env.CHATBRIDGE_HOST?.trim() ?? "";
  if (!host) {
    issues.push("CHATBRIDGE_HOST is required");
  }

  const rawPort = env.CHATBRIDGE_PORT?.trim() ?? "";
  const port = Number(rawPort);
  if (!rawPort || !Number.isInteger(port) || port < 0 || port > 65535) {
    issues.push(`CHATBRIDGE_PORT must be an integer between 0 and 65535 (got "${rawPort}")`);
  }

  const databaseUrl = env.CHATBRIDGE_DATABASE_URL?.trim() ?? "";
  if (!databaseUrl || !isValidChatBridgePostgresUrl(databaseUrl)) {
    issues.push(
      "CHATBRIDGE_DATABASE_URL must be a valid postgres:// URL targeting ChatBridge's dedicated logical database (e.g. chatbridge_db)"
    );
  }

  const integrationHmacSecret = env.CHATBRIDGE_INTEGRATION_HMAC_SECRET?.trim() ?? "";
  if (integrationHmacSecret.length < 32) {
    issues.push("CHATBRIDGE_INTEGRATION_HMAC_SECRET is required (minimum 32 characters)");
  }

  const webhookSigningSecret = env.CHATBRIDGE_WEBHOOK_SIGNING_SECRET?.trim() ?? "";
  if (webhookSigningSecret.length < 32) {
    issues.push("CHATBRIDGE_WEBHOOK_SIGNING_SECRET is required (minimum 32 characters)");
  }

  const objectStorageEndpoint = env.CHATBRIDGE_OBJECT_STORAGE_ENDPOINT?.trim() ?? "";
  if (!objectStorageEndpoint || !isValidHttpUrl(objectStorageEndpoint)) {
    issues.push("CHATBRIDGE_OBJECT_STORAGE_ENDPOINT must be a valid http/https URL");
  }

  const objectStorageAccessKeyId = env.CHATBRIDGE_OBJECT_STORAGE_ACCESS_KEY_ID?.trim() ?? "";
  if (objectStorageAccessKeyId.length < 8) {
    issues.push("CHATBRIDGE_OBJECT_STORAGE_ACCESS_KEY_ID is required (minimum 8 characters)");
  }

  const objectStorageSecretAccessKey =
    env.CHATBRIDGE_OBJECT_STORAGE_SECRET_ACCESS_KEY?.trim() ?? "";
  if (objectStorageSecretAccessKey.length < 16) {
    issues.push(
      "CHATBRIDGE_OBJECT_STORAGE_SECRET_ACCESS_KEY is required (minimum 16 characters)"
    );
  }

  const rawPollInterval = (env.CHATBRIDGE_WORKER_POLL_INTERVAL_MS ?? "2000").trim();
  const workerPollIntervalMs = Number(rawPollInterval);
  if (
    !Number.isInteger(workerPollIntervalMs) ||
    workerPollIntervalMs < 100 ||
    workerPollIntervalMs > 60000
  ) {
    issues.push("CHATBRIDGE_WORKER_POLL_INTERVAL_MS must be an integer between 100 and 60000");
  }

  const rawBlindRetry = (env.CHATBRIDGE_ALLOW_BLIND_SEND_RETRY ?? "false")
    .trim()
    .toLowerCase();
  if (rawBlindRetry !== "false") {
    issues.push(
      "CHATBRIDGE_ALLOW_BLIND_SEND_RETRY must be 'false' (outbound UNKNOWN outcomes require reconciliation before retry)"
    );
  }

  if (issues.length > 0) {
    throw new ChatBridgeConfigError(issues);
  }

  return Object.freeze({
    serviceName: "chatbridge",
    version: "0.1.0",
    nodeEnv: rawEnv as ChatBridgeRuntimeEnv,
    host,
    port,
    objectStorageEndpoint,
    workerPollIntervalMs,
    allowBlindSendRetryOnUnknown: false,
    secrets: Object.freeze({
      databaseUrl,
      integrationHmacSecret,
      webhookSigningSecret,
      objectStorageAccessKeyId,
      objectStorageSecretAccessKey,
    }),
  });
}

export function redactChatBridgeConfig(
  config: ChatBridgeConfig
): RedactedChatBridgeConfig {
  return Object.freeze({
    serviceName: config.serviceName,
    version: config.version,
    nodeEnv: config.nodeEnv,
    host: config.host,
    port: config.port,
    objectStorageEndpoint: config.objectStorageEndpoint,
    workerPollIntervalMs: config.workerPollIntervalMs,
    allowBlindSendRetryOnUnknown: config.allowBlindSendRetryOnUnknown,
    standaloneMode: true,
    credentialsConfigured: {
      databaseUrl: "[REDACTED]" as const,
      integrationHmacSecret: "[REDACTED]" as const,
      webhookSigningSecret: "[REDACTED]" as const,
      objectStorageAccessKeyId: "[REDACTED]" as const,
      objectStorageSecretAccessKey: "[REDACTED]" as const,
    },
  });
}
