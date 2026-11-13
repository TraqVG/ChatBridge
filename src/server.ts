import http from "node:http";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import { buildPgEnv, runPsqlStdin } from "../chat-core/src/receipt-store.js";
import {
  loadChatBridgeConfig,
  redactChatBridgeConfig,
  SYNTHETIC_CHATBRIDGE_DEV_ENV,
  type ChatBridgeConfig,
} from "./config.js";

export interface ChatBridgeDependencyState {
  readonly database: "READY" | "DEGRADED";
  readonly outboxQueue: "READY" | "DEGRADED";
  readonly objectStorage: "READY" | "DEGRADED";
  readonly downstreamClients: "CONNECTED" | "ABSENT";
}

export type ChatBridgeDependencyProbe =
  | ChatBridgeDependencyState
  | (() => Promise<ChatBridgeDependencyState>);

export interface ChatBridgeServerHandle {
  readonly server: http.Server;
  readonly config: ChatBridgeConfig;
  readonly port: number;
  readonly baseUrl: string;
  close(): Promise<void>;
}

const DEFAULT_DEPENDENCY_STATE: ChatBridgeDependencyState = Object.freeze({
  database: "READY",
  outboxQueue: "READY",
  objectStorage: "READY",
  downstreamClients: "ABSENT",
});

/**
 * Probes ChatBridge PostgreSQL connectivity and `outbox_events` readability
 * using the configured runtime database credentials.
 */
export async function probePostgresReadiness(
  config: ChatBridgeConfig
): Promise<ChatBridgeDependencyState> {
  try {
    const out = await runPsqlStdin(
      buildPgEnv(config.secrets.databaseUrl),
      "SET statement_timeout = 1500; SELECT CASE WHEN to_regclass('public.outbox_events') IS NOT NULL AND has_table_privilege(current_user, 'public.outbox_events', 'SELECT') THEN 'ready' ELSE 'missing_schema' END;"
    );
    if (out.split(/\r?\n/).some((line) => line.trim() === "ready")) {
      return {
        database: "READY",
        outboxQueue: "READY",
        objectStorage: "READY",
        downstreamClients: "ABSENT",
      };
    }
    return {
      database: "READY",
      outboxQueue: "DEGRADED",
      objectStorage: "READY",
      downstreamClients: "ABSENT",
    };
  } catch {
    return {
      database: "DEGRADED",
      outboxQueue: "DEGRADED",
      objectStorage: "READY",
      downstreamClients: "ABSENT",
    };
  }
}

function writeJson(
  res: http.ServerResponse,
  statusCode: number,
  body: Record<string, unknown>,
  correlationId: string
): void {
  const payload = JSON.stringify(body);
  res.writeHead(statusCode, {
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-store",
    "x-correlation-id": correlationId,
    "content-length": Buffer.byteLength(payload),
  });
  res.end(payload);
}

/**
 * Builds the standalone ChatBridge HTTP server with `/healthz`, `/readyz`,
 * and `/v1/system/health` endpoints.
 * Readiness is determined strictly by ChatBridge's own database and queue durability
 * and remains `ready` (HTTP 200) even when downstream client backends are absent.
 */
export function createChatBridgeServer(
  config: ChatBridgeConfig,
  depsOrProbe: ChatBridgeDependencyProbe = DEFAULT_DEPENDENCY_STATE
): http.Server {
  const startedAt = new Date().toISOString();
  const redacted = redactChatBridgeConfig(config);
  const resolveDeps =
    typeof depsOrProbe === "function" ? depsOrProbe : async () => depsOrProbe;

  return http.createServer((req, res) => {
    void (async () => {
      const correlationId =
        (typeof req.headers["x-correlation-id"] === "string" &&
          req.headers["x-correlation-id"].trim()) ||
        randomUUID();

      const method = req.method ?? "GET";
      const url = new URL(req.url ?? "/", `http://${config.host}:${config.port || 80}`);

      if (method === "GET" && url.pathname === "/healthz") {
        writeJson(
          res,
          200,
          {
            status: "ok",
            service: config.serviceName,
            version: config.version,
            environment: config.nodeEnv,
            standaloneMode: true,
            startedAt,
            correlationId,
            config: redacted,
          },
          correlationId
        );
        return;
      }

      if (method === "GET" && url.pathname === "/readyz") {
        const deps = await resolveDeps();
        // ChatBridge boots and reports readiness independently of downstream clients.
        const coreReady = deps.database === "READY" && deps.outboxQueue === "READY";
        const overallStatus = !coreReady
          ? "not_ready"
          : deps.objectStorage !== "READY"
            ? "degraded"
            : "ready";

        writeJson(
          res,
          coreReady ? 200 : 503,
          {
            status: overallStatus,
            service: config.serviceName,
            version: config.version,
            environment: config.nodeEnv,
            standaloneMode: true,
            correlationId,
            dependencies: {
              database: deps.database,
              outboxQueue: deps.outboxQueue,
              objectStorage: deps.objectStorage,
              downstreamClients: deps.downstreamClients,
            },
            safeguards: {
              allowBlindSendRetryOnUnknown: config.allowBlindSendRetryOnUnknown,
              credentialBoundary: "REDACTED",
              providerLiveSends: "DISABLED_SYNTHETIC_BOOTSTRAP",
            },
          },
          correlationId
        );
        return;
      }

      if (method === "GET" && url.pathname === "/v1/system/health") {
        const deps = await resolveDeps();
        writeJson(
          res,
          200,
          {
            data: {
              service: config.serviceName,
              version: config.version,
              environment: config.nodeEnv,
              standaloneMode: true,
              startedAt,
              dependencies: deps,
              config: redacted,
            },
            meta: {
              correlationId,
            },
          },
          correlationId
        );
        return;
      }

      writeJson(
        res,
        404,
        {
          error: {
            code: "ROUTE_NOT_FOUND",
            message: `No route registered for ${method} ${url.pathname}`,
            correlationId,
          },
        },
        correlationId
      );
    })();
  });
}

export async function startChatBridgeServer(
  env: Readonly<Record<string, string | undefined>> = process.env,
  depsOrProbe?: ChatBridgeDependencyProbe
): Promise<ChatBridgeServerHandle> {
  const config = loadChatBridgeConfig(env);
  const probe: ChatBridgeDependencyProbe =
    depsOrProbe ??
    (env.CHATBRIDGE_PROBE_DATABASE === "true"
      ? () => probePostgresReadiness(config)
      : DEFAULT_DEPENDENCY_STATE);
  const server = createChatBridgeServer(config, probe);

  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(config.port, config.host, () => {
      server.removeListener("error", reject);
      resolve();
    });
  });

  const address = server.address();
  const actualPort =
    typeof address === "object" && address !== null ? address.port : config.port;
  const baseUrl = `http://${config.host}:${actualPort}`;

  return {
    server,
    config,
    port: actualPort,
    baseUrl,
    close: () =>
      new Promise<void>((resolve, reject) => {
        server.close((err) => (err ? reject(err) : resolve()));
      }),
  };
}

const isMainModule =
  process.argv[1] !== undefined &&
  fileURLToPath(import.meta.url) === process.argv[1];

if (isMainModule) {
  const effectiveEnv =
    process.env.CHATBRIDGE_USE_SYNTHETIC_DEFAULTS === "true"
      ? { ...SYNTHETIC_CHATBRIDGE_DEV_ENV, ...process.env }
      : process.env;

  startChatBridgeServer(effectiveEnv)
    .then((handle) => {
      process.stdout.write(
        JSON.stringify({
          level: "info",
          service: handle.config.serviceName,
          event: "server.started",
          baseUrl: handle.baseUrl,
          config: redactChatBridgeConfig(handle.config),
          timestamp: new Date().toISOString(),
        }) + "\n"
      );

      const shutdown = () => {
        void handle.close().finally(() => process.exit(0));
      };
      process.once("SIGINT", shutdown);
      process.once("SIGTERM", shutdown);
    })
    .catch((err: unknown) => {
      process.stderr.write(
        JSON.stringify({
          level: "error",
          service: "chatbridge",
          event: "server.startup_failed",
          message: err instanceof Error ? err.message : String(err),
          timestamp: new Date().toISOString(),
        }) + "\n"
      );
      process.exit(1);
    });
}
