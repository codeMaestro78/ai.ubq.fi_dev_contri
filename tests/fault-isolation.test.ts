import assert from "node:assert/strict";

import { withTerminalRequestLog } from "../src/handler/terminal-log.ts";
import { handleUosEmbeddings } from "../src/embeddings/handlers.ts";
import { runEmbeddingsJobAttempt } from "../src/embeddings/jobs.ts";
import {
  embeddingsJobInputKey,
  embeddingsJobKey,
  embeddingsJobLookupKey,
  type EmbeddingsJobRecord,
} from "../src/embeddings/ledger.ts";
import { encryptEmbeddingsJobInput } from "../src/embeddings/voyage.ts";
import { sha256Hex } from "../src/utils.ts";
import { handleHealthProviders } from "../src/health.ts";
import { setKvForTest } from "../src/kv.ts";
import { CODEX_AUTH_POOL_KV_KEY, resetCodexAuthCacheForTest } from "../src/codex/index.ts";
import { refreshProviderCapacity, type ProviderCapacityCodexSource } from "../src/provider/capacity.ts";

const keyToString = (key: Deno.KvKey): string => JSON.stringify(key);

const kvEntry = (key: Deno.KvKey, value: unknown): Deno.KvEntryMaybe<unknown> =>
  ({ key, value, versionstamp: value === null || value === undefined ? null : "v1" }) as Deno.KvEntryMaybe<unknown>;

const okAtomic = (store: Map<string, unknown>) => () => {
  const ops: { type: "set" | "delete"; key: Deno.KvKey; value?: unknown }[] = [];
  const chain = {
    check: (_entry: unknown) => chain,
    set: (key: Deno.KvKey, value: unknown) => {
      ops.push({ type: "set", key, value });
      return chain;
    },
    delete: (key: Deno.KvKey) => {
      ops.push({ type: "delete", key });
      return chain;
    },
    commit: () => {
      for (const op of ops) {
        if (op.type === "set") store.set(keyToString(op.key), op.value);
        else store.delete(keyToString(op.key));
      }
      return Promise.resolve({ ok: true, versionstamp: "vtest" });
    },
  };
  return chain;
};

const emptyList = () => (async function* () {})();

const withFetchMock = async <T>(handler: (url: string, bodyText: string | null) => Response | Promise<Response>, fn: () => Promise<T>): Promise<T> => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
    const bodyText = typeof init?.body === "string" ? init.body : null;
    return await handler(url, bodyText);
  }) as typeof fetch;
  try {
    return await fn();
  } finally {
    globalThis.fetch = originalFetch;
  }
};

const voyageOkForRequest = (dimensions: number, bodyText: string | null): Response => {
  let count = 1;
  try {
    const raw = (JSON.parse(bodyText ?? "{}") as { input?: unknown }).input;
    count = Array.isArray(raw) ? raw.length : 1;
  } catch {
    count = 1;
  }
  const vectors = Array.from({ length: count }, (_, index) => ({
    embedding: Array.from({ length: dimensions }, (_, dim) => (index + 1) * 0.01 + dim / dimensions),
  }));
  return new Response(JSON.stringify({ data: vectors, usage: { total_tokens: count * 7 } }), {
    status: 200,
    headers: { "Content-Type": "application/json" },
  });
};

Deno.test("terminal log survives a synchronously throwing telemetry sink", async () => {
  let analyticsRan = false;
  let adminErrorRan = false;
  const response = await withTerminalRequestLog(
    new Response("complete", { status: 200, headers: { "Content-Type": "application/json" } }),
    {
      route: "responses",
      startedAtMonotonicMs: Date.now(),
      requestId: "fault-isolation-terminal-sync-throw",
      recordTelemetry: (): Promise<never> => {
        throw new Error("sync telemetry boom");
      },
      recordCacheAnalytics: () => {
        analyticsRan = true;
        return Promise.resolve({ status: "ignored" as const, reason: "unknown_release" as const, bucket_start_at_ms: null });
      },
      recordAdminError: () => {
        adminErrorRan = true;
        return Promise.resolve();
      },
    }
  );
  assert.equal(analyticsRan, true);
  assert.equal(adminErrorRan, true);
  assert.equal(response.status, 200);
  assert.equal(await response.text(), "complete");
});

Deno.test("sync embeddings treats rejected cache reads as misses and uses the Voyage path", async () => {
  const store = new Map<string, unknown>();
  store.set(keyToString(["uos_ai", "voyage_api_key"]), "voyage_test_key");
  let voyageCalls = 0;
  const kv = {
    get: (key: Deno.KvKey) => {
      if (key[1] === "v2" && key[2] === "cache") return Promise.reject(new Error("kv cache down"));
      return Promise.resolve(kvEntry(key, store.get(keyToString(key)) ?? null));
    },
    set: (key: Deno.KvKey, value: unknown) => {
      store.set(keyToString(key), value);
      return Promise.resolve({ ok: true } as const);
    },
    delete: (key: Deno.KvKey) => {
      store.delete(keyToString(key));
      return Promise.resolve();
    },
    list: emptyList,
    atomic: okAtomic(store),
    close: () => {},
  } as unknown as Deno.Kv;
  const request = new Request("https://ai.ubq.fi/uos/embeddings", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ model: "voyage-4-large", input: ["hello", "world"], input_type: "document", dimensions: 1024 }),
  });
  const response = await withFetchMock(
    (url, bodyText) => {
      if (url.includes("voyageai")) voyageCalls += 1;
      return voyageOkForRequest(1024, bodyText);
    },
    () => handleUosEmbeddings(request, undefined, { kv })
  );
  assert.equal(response.status, 200);
  const body = (await response.json()) as { data?: { embedding?: number[] }[] };
  assert.equal(Array.isArray(body.data), true);
  assert.equal(body.data?.length, 2);
  assert.equal(body.data?.[0]?.embedding?.length, 1024);
  assert.equal(voyageCalls > 0, true);
});

const buildEmbeddingsJob = (inputHashes: string[]): EmbeddingsJobRecord => ({
  id: "job-fault-1",
  status: "queued",
  created_at_ms: Date.now(),
  updated_at_ms: Date.now(),
  model: "voyage-4-large",
  cache_profile_key: "test-profile-v1",
  upstream: "voyage",
  upstream_model: "voyage-4-large",
  input_type: "document",
  dimensions: 1024,
  output_dtype: "float",
  encoding_format: "float",
  truncation: true,
  input_hashes: inputHashes,
  input_count: inputHashes.length,
  total_chars: 11,
  usage_total_tokens: 0,
  retry_after_seconds: null,
  locked_until_ms: null,
  error: null,
});

Deno.test("embeddings job treats rejected cache reads as misses and follows the Voyage path", async () => {
  const tokenSeed = "fault-isolation-seed";
  const tokenHash = await sha256Hex(tokenSeed);
  const texts = ["alpha", "beta"];
  const hashes = await Promise.all(texts.map((text) => sha256Hex(text)));
  const store = new Map<string, unknown>();
  for (let i = 0; i < texts.length; i += 1) {
    store.set(keyToString(embeddingsJobInputKey(tokenHash, "test-profile-v1", "job-fault-1", hashes[i])), await encryptEmbeddingsJobInput(tokenSeed, texts[i]));
  }
  const kv = {
    get: (key: Deno.KvKey) => {
      if (key[1] === "v2" && key[2] === "cache") return Promise.reject(new Error("kv cache down"));
      return Promise.resolve(kvEntry(key, store.get(keyToString(key)) ?? null));
    },
    set: (key: Deno.KvKey, value: unknown) => {
      store.set(keyToString(key), value);
      return Promise.resolve({ ok: true } as const);
    },
    delete: (key: Deno.KvKey) => {
      store.delete(keyToString(key));
      return Promise.resolve();
    },
    list: emptyList,
    atomic: okAtomic(store),
    close: () => {},
  } as unknown as Deno.Kv;
  const job = buildEmbeddingsJob(hashes);
  const jobKey = embeddingsJobKey(tokenHash, job.cache_profile_key, job.id);
  let voyageCalls = 0;
  const response = await withFetchMock(
    (url, bodyText) => {
      if (url.includes("voyageai")) voyageCalls += 1;
      return voyageOkForRequest(1024, bodyText);
    },
    () =>
      runEmbeddingsJobAttempt({
        reqId: "req-fault-1",
        kv,
        apiKey: "voyage_test_key",
        tokenSeed,
        tokenHash,
        jobKey,
        jobLookupKey: embeddingsJobLookupKey(tokenHash, job.id),
        jobEntry: { key: jobKey, value: job, versionstamp: "v1" },
        job,
        deadlineMs: Date.now() + 60_000,
      })
  );
  assert.equal(voyageCalls > 0, true);
  assert.equal(response.status, 202);
  const body = (await response.json()) as { status?: unknown };
  assert.equal(body.status, "queued");
});

Deno.test("embeddings job routes rejected input reads to failJob and still cleans up every key", async () => {
  const tokenSeed = "fault-isolation-seed";
  const tokenHash = await sha256Hex(tokenSeed);
  const texts = ["alpha", "beta"];
  const hashes = await Promise.all(texts.map((text) => sha256Hex(text)));
  const store = new Map<string, unknown>();
  const deleteAttempts: string[] = [];
  const kv = {
    get: (key: Deno.KvKey) => {
      if (key[1] === "v2" && key[2] === "cache") return Promise.reject(new Error("kv cache down"));
      if (key[1] === "jobs" && key[3] === "input") return Promise.reject(new Error("kv inputs down"));
      return Promise.resolve(kvEntry(key, store.get(keyToString(key)) ?? null));
    },
    set: (key: Deno.KvKey, value: unknown) => {
      store.set(keyToString(key), value);
      return Promise.resolve({ ok: true } as const);
    },
    delete: (key: Deno.KvKey) => {
      deleteAttempts.push(keyToString(key));
      return Promise.reject(new Error("kv delete down"));
    },
    list: emptyList,
    atomic: okAtomic(store),
    close: () => {},
  } as unknown as Deno.Kv;
  const job = buildEmbeddingsJob(hashes);
  const jobKey = embeddingsJobKey(tokenHash, job.cache_profile_key, job.id);
  const response = await runEmbeddingsJobAttempt({
    reqId: "req-fault-2",
    kv,
    apiKey: "voyage_test_key",
    tokenSeed,
    tokenHash,
    jobKey,
    jobLookupKey: embeddingsJobLookupKey(tokenHash, job.id),
    jobEntry: { key: jobKey, value: job, versionstamp: "v1" },
    job,
    deadlineMs: Date.now() + 60_000,
  });
  assert.equal(response.status, 200);
  const body = (await response.json()) as { status?: unknown; error?: { code?: unknown } };
  assert.equal(body.status, "failed");
  assert.equal(body.error?.code, "embeddings_job_input_missing");
  assert.equal(deleteAttempts.length, hashes.length);
  for (const hash of hashes) {
    assert.equal(
      deleteAttempts.includes(keyToString(embeddingsJobInputKey(tokenHash, job.cache_profile_key, job.id, hash))),
      true
    );
  }
});

Deno.test("provider health snapshot degrades rejected reads instead of failing", async () => {
  const kv = {
    get: (_key: Deno.KvKey) => Promise.reject(new Error("kv down")),
    set: (_key: Deno.KvKey, _value: unknown) => Promise.resolve({ ok: true } as const),
    delete: (_key: Deno.KvKey) => Promise.resolve(),
    list: emptyList,
    atomic: okAtomic(new Map()),
    close: () => {},
  } as unknown as Deno.Kv;
  setKvForTest(kv);
  try {
    const response = await handleHealthProviders();
    assert.equal(response.status, 200);
    const body = (await response.json()) as {
      codex: { state: unknown; account_count: unknown; accounts: { health: { state: unknown } }[] };
      cerebras: { health: { state: unknown } };
      deepseek: { health: { state: unknown } };
      lithos: { health: { state: unknown } };
      metered: { health: { state: unknown }; quota: { available: unknown } };
      surplus: { health: { state: unknown } };
    };
    assert.equal(body.codex.state, "unknown");
    assert.equal(body.codex.account_count, 0);
    assert.deepEqual(body.codex.accounts, []);
    for (const section of [body.cerebras, body.deepseek, body.lithos, body.metered, body.surplus]) {
      assert.equal(section.health.state, "unknown");
    }
    assert.equal(body.metered.quota.available, false);
  } finally {
    setKvForTest(null);
  }
});

Deno.test("capacity snapshot marks a throwing slot unreachable and keeps its sibling", async () => {
  const nowMs = 1_800_000_000_000;
  const store = new Map<string, unknown>();
  store.set(keyToString(CODEX_AUTH_POOL_KV_KEY), {
    accounts: [
      { access_token: "good-token", refresh_token: "good-refresh", account_id: "good-account", updated_at_ms: nowMs },
      { access_token: "bad-token", refresh_token: "bad-refresh", account_id: "bad\nid", updated_at_ms: nowMs },
    ],
    updated_at_ms: nowMs,
  });
  const kv = {
    get: (key: Deno.KvKey) => Promise.resolve(kvEntry(key, store.get(keyToString(key)) ?? null)),
    set: (key: Deno.KvKey, value: unknown) => {
      store.set(keyToString(key), value);
      return Promise.resolve({ ok: true } as const);
    },
    delete: (key: Deno.KvKey) => {
      store.delete(keyToString(key));
      return Promise.resolve();
    },
    list: emptyList,
    atomic: okAtomic(store),
    close: () => {},
  } as unknown as Deno.Kv;
  let badTokenSeen = false;
  const fetcher = (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const headers = new Headers(init?.headers);
    const authorization = headers.get("Authorization") ?? "";
    const url = typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
    if (url.includes("openlux")) return Promise.resolve(new Response("{}", { status: 404 }));
    if (authorization.includes("bad-token")) {
      badTokenSeen = true;
      return Promise.reject(new Error("bad slot must never be fetched"));
    }
    return Promise.resolve(
      new Response(
        JSON.stringify({
          rate_limit: {
            primary_window: { limit_window_seconds: 10_800, used_percent: 12.5, reset_at: 1_800_010_000 },
            secondary_window: { limit_window_seconds: 86_400, used_percent: 38, reset_at: 1_800_020_000 },
          },
        }),
        { status: 200, headers: { "Content-Type": "application/json" } }
      )
    );
  };
  setKvForTest(kv);
  resetCodexAuthCacheForTest();
  try {
    const view = await refreshProviderCapacity({ kv, fetcher, now: () => nowMs });
    const good = view.sources.find((source): source is ProviderCapacityCodexSource => source.source === "codex" && source.slot === 1);
    const bad = view.sources.find((source): source is ProviderCapacityCodexSource => source.source === "codex" && source.slot === 2);
    assert.equal(good?.state, "available");
    assert.notEqual(good?.windows?.primary, null);
    assert.equal(bad?.state, "unavailable");
    assert.equal(bad?.failure_kind, "unreachable");
    assert.equal(bad?.account_cohort_id, null);
    assert.equal(badTokenSeen, false);
  } finally {
    setKvForTest(null);
    resetCodexAuthCacheForTest();
  }
});
