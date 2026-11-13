import { randomBytes, scryptSync, timingSafeEqual } from "node:crypto";
import { assertValidClientKey, assertValidUuid } from "./types.js";

const SCRYPT_N = 16384;
const SCRYPT_R = 8;
const SCRYPT_P = 1;
const SCRYPT_KEY_LEN = 64;
const SECRET_BYTES = 32;
const CREDENTIAL_PREFIX = "cb1";

export interface ParsedIntegrationCredential {
  readonly tenantId: string;
  readonly clientKey: string;
  readonly rawSecret: string;
}

export function generateRawSecret(): string {
  return randomBytes(SECRET_BYTES).toString("base64url");
}

export function formatIntegrationCredential(
  tenantId: string,
  clientKey: string,
  rawSecret: string
): string {
  const safeTenantId = assertValidUuid(tenantId, "tenantId");
  const safeClientKey = assertValidClientKey(clientKey);
  return `${CREDENTIAL_PREFIX}.${safeTenantId}.${safeClientKey}.${rawSecret}`;
}

export function parseIntegrationCredential(
  credential: string
): ParsedIntegrationCredential | null {
  if (typeof credential !== "string") {
    return null;
  }
  const parts = credential.trim().split(".");
  if (parts.length !== 4) {
    return null;
  }
  const [prefix, tenantPart, keyPart, secretPart] = parts;
  if (prefix !== CREDENTIAL_PREFIX || !tenantPart || !keyPart || !secretPart) {
    return null;
  }
  if (secretPart.length < 16 || !/^[A-Za-z0-9_-]+$/.test(secretPart)) {
    return null;
  }

  try {
    const tenantId = assertValidUuid(tenantPart, "tenantId");
    const clientKey = assertValidClientKey(keyPart);
    return { tenantId, clientKey, rawSecret: secretPart };
  } catch {
    return null;
  }
}

export function hashIntegrationSecret(rawSecret: string): string {
  const salt = randomBytes(16).toString("base64url");
  const derived = scryptSync(rawSecret, salt, SCRYPT_KEY_LEN, {
    N: SCRYPT_N,
    r: SCRYPT_R,
    p: SCRYPT_P,
  });
  return `scrypt$${SCRYPT_N}$${SCRYPT_R}$${SCRYPT_P}$${salt}$${derived.toString("base64url")}`;
}

export function verifyIntegrationSecret(
  rawSecret: string,
  storedHash: string
): boolean {
  const parts = storedHash.split("$");
  if (parts.length !== 6) {
    return false;
  }
  const [alg, nStr, rStr, pStr, salt, expectedB64] = parts;
  if (alg !== "scrypt" || !nStr || !rStr || !pStr || !salt || !expectedB64) {
    return false;
  }

  const n = Number(nStr);
  const r = Number(rStr);
  const p = Number(pStr);
  if (
    !Number.isSafeInteger(n) ||
    n < 1024 ||
    !Number.isSafeInteger(r) ||
    r < 1 ||
    !Number.isSafeInteger(p) ||
    p < 1
  ) {
    return false;
  }

  const expectedBuf = Buffer.from(expectedB64, "base64url");
  if (expectedBuf.length === 0) {
    return false;
  }

  const derived = scryptSync(rawSecret, salt, expectedBuf.length, { N: n, r, p });
  return timingSafeEqual(derived, expectedBuf);
}
