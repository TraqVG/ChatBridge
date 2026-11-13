import { readdirSync, readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { buildPgEnv, runPsqlStdin, sqlTextB64 } from "../chat-core/src/receipt-store.js";

function quotePgIdent(ident: string): string {
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(ident)) {
    throw new Error(`Invalid PostgreSQL role identifier: ${ident}`);
  }
  return `"${ident}"`;
}

/**
 * Applies ChatBridge SQL migrations in lexical order and ensures the non-owner
 * least-privilege runtime role (`chatbridge_app` member) exists for API/worker containers.
 */
export async function runChatBridgeMigrations(
  env: Readonly<Record<string, string | undefined>> = process.env
): Promise<{ readonly migrationsApplied: readonly string[]; readonly runtimeUser: string }> {
  const runtimeUser = (env.CHATBRIDGE_DB_APP_USER ?? "chatbridge_runtime").trim();
  const runtimePassword = (env.CHATBRIDGE_DB_APP_PASSWORD ?? "").trim();
  if (!runtimePassword || runtimePassword.length < 12) {
    throw new Error("CHATBRIDGE_DB_APP_PASSWORD is required (minimum 12 characters)");
  }
  const quotedUser = quotePgIdent(runtimeUser);

  const adminUrl = env.CHATBRIDGE_PG_ADMIN_URL?.trim();
  const pgEnv: NodeJS.ProcessEnv = adminUrl
    ? buildPgEnv(adminUrl)
    : {
        ...process.env,
        PGHOST: env.PGHOST ?? "postgres",
        PGPORT: env.PGPORT ?? "5432",
        PGUSER: env.PGUSER ?? "chatbridge_owner",
        PGPASSWORD: env.PGPASSWORD ?? "",
        PGDATABASE: env.PGDATABASE ?? "chatbridge_db",
        PGSSLMODE: env.PGSSLMODE ?? "disable",
        PGOPTIONS: "-c client_min_messages=warning",
      };

  const migrationsDir = path.resolve(
    env.CHATBRIDGE_MIGRATIONS_DIR ?? path.join(process.cwd(), "database-adapters", "migrations")
  );
  const files = readdirSync(migrationsDir)
    .filter((f) => f.endsWith(".sql"))
    .sort((a, b) => a.localeCompare(b));

  for (const file of files) {
    const sql = readFileSync(path.join(migrationsDir, file), "utf8");
    await runPsqlStdin(pgEnv, sql);
  }

  const roleSql = `
BEGIN;
DO $$
DECLARE
  pass TEXT := ${sqlTextB64(runtimePassword)};
  target_role TEXT := ${sqlTextB64(runtimeUser)};
  db_name TEXT := current_database();
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = target_role) THEN
    EXECUTE format(
      'CREATE ROLE %I WITH LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS INHERIT',
      target_role,
      pass
    );
  ELSE
    EXECUTE format(
      'ALTER ROLE %I WITH LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS INHERIT',
      target_role,
      pass
    );
  END IF;
  EXECUTE format('REVOKE ALL ON SCHEMA public FROM %I', target_role);
  EXECUTE format('REVOKE CREATE ON DATABASE %I FROM %I', db_name, target_role);
  EXECUTE format('GRANT USAGE ON SCHEMA public TO %I', target_role);
  EXECUTE format('GRANT chatbridge_app TO %I', target_role);
END
$$;
COMMENT ON ROLE ${quotedUser} IS 'ChatBridge least-privilege runtime role (DML via chatbridge_app only, zero DDL)';
COMMIT;`;

  await runPsqlStdin(pgEnv, roleSql);
  return { migrationsApplied: files, runtimeUser };
}

const isMainModule =
  process.argv[1] !== undefined &&
  fileURLToPath(import.meta.url) === process.argv[1];

if (isMainModule) {
  runChatBridgeMigrations()
    .then((result) => {
      process.stdout.write(
        JSON.stringify({
          level: "info",
          service: "chatbridge-migrate",
          event: "migrations.completed",
          migrationsApplied: result.migrationsApplied,
          runtimeUser: result.runtimeUser,
          timestamp: new Date().toISOString(),
        }) + "\n"
      );
    })
    .catch((err: unknown) => {
      process.stderr.write(
        JSON.stringify({
          level: "error",
          service: "chatbridge-migrate",
          event: "migrations.failed",
          message: err instanceof Error ? err.message : String(err),
          timestamp: new Date().toISOString(),
        }) + "\n"
      );
      process.exit(1);
    });
}
