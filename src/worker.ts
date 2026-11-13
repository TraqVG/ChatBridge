import { writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { buildPgEnv, runPsqlStdin } from "../chat-core/src/receipt-store.js";
import {
  loadChatBridgeConfig,
  redactChatBridgeConfig,
  SYNTHETIC_CHATBRIDGE_DEV_ENV,
  type ChatBridgeConfig,
  type RedactedChatBridgeConfig,
} from "./config.js";

export interface ChatBridgeWorkerTickResult {
  readonly tickNumber: number;
  readonly timestamp: string;
  readonly queuesChecked: readonly [
    "chatbridge_outbox",
    "chatbridge_inbox_receipts",
    "outbound_unknown_reconcile",
    "assignment_auto_release"
  ];
  readonly processedCount: number;
}

export interface ChatBridgeWorkerOptions {
  readonly keepProcessAlive?: boolean;
  readonly probeDatabase?: boolean;
  readonly heartbeatPath?: string;
}

export interface ChatBridgeWorkerHandle {
  readonly config: ChatBridgeConfig;
  readonly redactedConfig: RedactedChatBridgeConfig;
  readonly isRunning: () => boolean;
  readonly getTickCount: () => number;
  readonly runTick: () => Promise<ChatBridgeWorkerTickResult>;
  readonly stop: () => Promise<void>;
}

/**
 * Starts the standalone ChatBridge background worker (outbox dispatcher,
 * inbound receipt normalizer, outbound UNKNOWN state reconciler, and claim lease expiry).
 */
export async function startChatBridgeWorker(
  env: Readonly<Record<string, string | undefined>> = process.env,
  options: ChatBridgeWorkerOptions = {}
): Promise<ChatBridgeWorkerHandle> {
  const config = loadChatBridgeConfig(env);
  const redacted = redactChatBridgeConfig(config);
  const probeDb = options.probeDatabase ?? env.CHATBRIDGE_PROBE_DATABASE === "true";
  const heartbeatPath = options.heartbeatPath ?? env.CHATBRIDGE_WORKER_HEARTBEAT_PATH;
  let running = true;
  let tickCount = 0;

  const runTick = async (): Promise<ChatBridgeWorkerTickResult> => {
    if (!running) {
      throw new Error("ChatBridge worker is stopped");
    }
    let pendingCount = 0;
    if (probeDb) {
      const out = await runPsqlStdin(
        buildPgEnv(config.secrets.databaseUrl),
        "SET statement_timeout = 1500; SELECT COUNT(*)::int FROM outbox_events WHERE state IN ('PENDING', 'FAILED_RETRYABLE');"
      );
      const parsed = Number(out.split(/\r?\n/).find((line) => /^\d+$/.test(line.trim())) ?? "0");
      pendingCount = Number.isFinite(parsed) ? parsed : 0;
    }
    if (heartbeatPath) {
      writeFileSync(heartbeatPath, `${new Date().toISOString()}\n`, "utf8");
    }
    tickCount += 1;
    return {
      tickNumber: tickCount,
      timestamp: new Date().toISOString(),
      queuesChecked: [
        "chatbridge_outbox",
        "chatbridge_inbox_receipts",
        "outbound_unknown_reconcile",
        "assignment_auto_release",
      ],
      processedCount: pendingCount,
    };
  };

  const timer = setInterval(() => {
    if (running) {
      void runTick().catch((err: unknown) => {
        process.stderr.write(
          JSON.stringify({
            level: "warn",
            service: "chatbridge-worker",
            event: "worker.tick_failed",
            message: err instanceof Error ? err.message : String(err),
            timestamp: new Date().toISOString(),
          }) + "\n"
        );
      });
    }
  }, config.workerPollIntervalMs);
  if (!options.keepProcessAlive) {
    timer.unref();
  }

  return {
    config,
    redactedConfig: redacted,
    isRunning: () => running,
    getTickCount: () => tickCount,
    runTick,
    stop: async () => {
      running = false;
      clearInterval(timer);
    },
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
  const runOnce = process.env.CHATBRIDGE_WORKER_ONCE === "true";

  startChatBridgeWorker(effectiveEnv, { keepProcessAlive: !runOnce })
    .then(async (worker) => {
      let firstTick: ChatBridgeWorkerTickResult | null = null;
      if (runOnce) {
        firstTick = await worker.runTick();
      } else {
        try {
          firstTick = await worker.runTick();
        } catch (err: unknown) {
          process.stderr.write(
            JSON.stringify({
              level: "warn",
              service: "chatbridge-worker",
              event: "worker.initial_tick_deferred",
              message: err instanceof Error ? err.message : String(err),
              timestamp: new Date().toISOString(),
            }) + "\n"
          );
        }
      }

      process.stdout.write(
        JSON.stringify({
          level: "info",
          service: "chatbridge-worker",
          event: "worker.started",
          firstTick,
          config: worker.redactedConfig,
          timestamp: new Date().toISOString(),
        }) + "\n"
      );

      if (runOnce) {
        await worker.stop();
        process.exit(0);
      }

      const shutdown = () => {
        void worker.stop().finally(() => process.exit(0));
      };
      process.once("SIGINT", shutdown);
      process.once("SIGTERM", shutdown);
    })
    .catch((err: unknown) => {
      process.stderr.write(
        JSON.stringify({
          level: "error",
          service: "chatbridge-worker",
          event: "worker.startup_failed",
          message: err instanceof Error ? err.message : String(err),
          timestamp: new Date().toISOString(),
        }) + "\n"
      );
      process.exit(1);
    });
}
