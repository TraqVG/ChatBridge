import { describe, it } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import {
  InMemoryIntegrationClientStore,
  IntegrationCredentialError,
  IntegrationCredentialService,
} from "../channel-management/src/integrations/index.js";

const TENANT_A = "11111111-1111-4111-8111-111111111111";
const TENANT_B = "22222222-2222-4222-8222-222222222222";

describe("ID-01-05 Standalone ChatBridge integration credentials", () => {
  it("contains zero downstream storefront/shop imports or coupling in channel-management/src/integrations", () => {
    const dir = path.resolve(process.cwd(), "channel-management/src/integrations");
    const files = fs.readdirSync(dir).filter((name) => name.endsWith(".ts"));
    assert.ok(files.length >= 5);

    for (const file of files) {
      const content = fs.readFileSync(path.join(dir, file), "utf8");
      assert.equal(
        /\bmangmie\b|\bshop_id\b|\bBetterAuth\b/i.test(content),
        false,
        `${file} must remain standalone without external storefront coupling`
      );
    }
  });

  it("enforces two-tenant isolation, capability scopes, rotation, and revocation", async () => {
    const store = new InMemoryIntegrationClientStore();
    let tick = 0;
    const service = new IntegrationCredentialService(
      store,
      () => new Date(Date.UTC(2026, 10, 13, 18, 0, tick++))
    );

    // Both independent tenants may register the same clientKey without collision.
    const tenantAReadOnly = await service.provisionClient({
      tenantId: TENANT_A,
      clientKey: "sync-worker",
      scopes: ["conversations:read", "messages:read"],
      actor: "tenant_a_admin",
      traceId: "tr-prov-a1",
    });

    const tenantBFull = await service.provisionClient({
      tenantId: TENANT_B,
      clientKey: "sync-worker",
      scopes: ["conversations:read", "messages:read", "messages:send"],
      actor: "tenant_b_admin",
      traceId: "tr-prov-b1",
    });

    assert.equal(tenantAReadOnly.client.secretVersion, 1);
    assert.equal(tenantAReadOnly.client.status, "ACTIVE");
    assert.deepEqual(tenantAReadOnly.client.scopes, ["conversations:read", "messages:read"]);
    assert.equal("secretHash" in tenantAReadOnly.client, false);

    // Tenant A read-only client can read Tenant A, cannot send in Tenant A, cannot access Tenant B.
    const aRead = await service.authorizeRead(tenantAReadOnly.credential, TENANT_A, "messages:read");
    assert.equal(aRead.ok, true);

    const aSendDenied = await service.authorizeSend(tenantAReadOnly.credential, TENANT_A);
    assert.equal(aSendDenied.ok, false);
    if (!aSendDenied.ok) {
      assert.equal(aSendDenied.code, "FORBIDDEN");
      assert.equal(aSendDenied.reason, "INSUFFICIENT_SCOPE");
    }

    const aCrossTenant = await service.authorizeRead(tenantAReadOnly.credential, TENANT_B);
    assert.equal(aCrossTenant.ok, false);
    if (!aCrossTenant.ok) {
      assert.equal(aCrossTenant.code, "FORBIDDEN");
      assert.equal(aCrossTenant.reason, "CROSS_TENANT_DENIED");
    }

    // Cross-tenant rotate/revoke fails closed with 404 NOT_FOUND.
    await assert.rejects(
      () =>
        service.rotateClientSecret({
          tenantId: TENANT_B,
          clientId: tenantAReadOnly.client.id,
          actor: "tenant_b_attacker",
        }),
      (err: unknown) => err instanceof IntegrationCredentialError && err.code === "NOT_FOUND"
    );

    // Secret rotation refuses silent capability escalation by default.
    await assert.rejects(
      () =>
        service.rotateClientSecret({
          tenantId: TENANT_A,
          clientId: tenantAReadOnly.client.id,
          expectedVersion: 1,
          nextScopes: ["conversations:read", "messages:read", "messages:send"],
          actor: "tenant_a_admin",
        }),
      (err: unknown) => err instanceof IntegrationCredentialError && err.code === "FORBIDDEN"
    );

    // Rotate Tenant B client preserving/narrowing scope; old secret stops working immediately.
    const rotatedB = await service.rotateClientSecret({
      tenantId: TENANT_B,
      clientId: tenantBFull.client.id,
      expectedVersion: 1,
      nextScopes: ["messages:read", "messages:send"],
      actor: "tenant_b_admin",
      traceId: "tr-rot-b1",
    });

    assert.equal(rotatedB.previousSecretVersion, 1);
    assert.equal(rotatedB.client.secretVersion, 2);
    assert.notEqual(rotatedB.client.rotatedAt, null);
    assert.deepEqual(rotatedB.client.scopes, ["messages:read", "messages:send"]);

    const oldSecretCheck = await service.authorizeSend(tenantBFull.credential, TENANT_B);
    assert.equal(oldSecretCheck.ok, false);
    if (!oldSecretCheck.ok) {
      assert.equal(oldSecretCheck.code, "UNAUTHENTICATED");
      assert.equal(oldSecretCheck.reason, "INVALID_SECRET");
    }

    // Stale expectedVersion fails with 412 STALE_VERSION.
    await assert.rejects(
      () =>
        service.rotateClientSecret({
          tenantId: TENANT_B,
          clientId: tenantBFull.client.id,
          expectedVersion: 1,
          actor: "tenant_b_stale_worker",
        }),
      (err: unknown) =>
        err instanceof IntegrationCredentialError &&
        err.code === "STALE_VERSION" &&
        err.httpStatus === 412 &&
        err.currentVersion === 2
    );

    // Rotated key keeps allowed scope (send OK) and rejects dropped scope (conversations:read FORBIDDEN).
    const rotatedBSend = await service.authorizeSend(rotatedB.credential, TENANT_B);
    assert.equal(rotatedBSend.ok, true);

    const rotatedBDroppedScope = await service.authorizeRead(
      rotatedB.credential,
      TENANT_B,
      "conversations:read"
    );
    assert.equal(rotatedBDroppedScope.ok, false);
    if (!rotatedBDroppedScope.ok) {
      assert.equal(rotatedBDroppedScope.code, "FORBIDDEN");
      assert.equal(rotatedBDroppedScope.reason, "INSUFFICIENT_SCOPE");
    }

    // Revoking Tenant B client blocks both read and send, and forbids further rotation.
    const revokedB = await service.revokeClient({
      tenantId: TENANT_B,
      clientId: tenantBFull.client.id,
      reason: "incident_key_retire",
      actor: "tenant_b_admin",
      traceId: "tr-rev-b1",
    });
    assert.equal(revokedB.status, "REVOKED");

    const revokedRead = await service.authorizeRead(rotatedB.credential, TENANT_B, "messages:read");
    assert.equal(revokedRead.ok, false);
    if (!revokedRead.ok) {
      assert.equal(revokedRead.code, "UNAUTHENTICATED");
      assert.equal(revokedRead.reason, "CLIENT_REVOKED");
    }

    const revokedSend = await service.authorizeSend(rotatedB.credential, TENANT_B);
    assert.equal(revokedSend.ok, false);
    if (!revokedSend.ok) {
      assert.equal(revokedSend.code, "UNAUTHENTICATED");
      assert.equal(revokedSend.reason, "CLIENT_REVOKED");
    }

    await assert.rejects(
      () =>
        service.rotateClientSecret({
          tenantId: TENANT_B,
          clientId: tenantBFull.client.id,
          actor: "tenant_b_admin",
        }),
      (err: unknown) => err instanceof IntegrationCredentialError && err.code === "STATE_CONFLICT"
    );

    // Tenant A remains unaffected after Tenant B revocation; safe listing never leaks secrets.
    assert.equal((await service.authorizeRead(tenantAReadOnly.credential, TENANT_A)).ok, true);
    const auditsB = store.listAuditsForTenant(TENANT_B);
    assert.ok(auditsB.length >= 4);
    const serializedAudits = JSON.stringify(auditsB);
    assert.equal(serializedAudits.includes(rotatedB.credential), false);
    assert.equal(serializedAudits.includes(tenantBFull.credential), false);
  });
});
