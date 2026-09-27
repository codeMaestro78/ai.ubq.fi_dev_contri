import { config, runtimeDeploymentId, runtimeGitSha } from "./config.ts";
import { readCerebrasApiKey } from "./provider/cerebras.ts";
import { CODEX_AUTH_POOL_KV_KEY, fetchCodexModels, getJwtExpMs, parseCodexAuthFromAuthJson, parseCodexAuthPool } from "./codex/index.ts";
import { readDeepSeekApiKey } from "./deepseek/index.ts";
import { json } from "./http.ts";
import { getKv } from "./kv.ts";
import { readLithosApiKey } from "./provider/lithos.ts";
import {
  getCerebrasProviderHealth,
  getCodexProviderHealth,
  getDeepSeekProviderHealth,
  getLithosProviderHealth,
  getMeteredProviderHealth,
  getSurplusProviderHealth,
  PROVIDER_HEALTH_STALE_AFTER_MS,
  type ProviderHealthState,
  unknownProviderHealthView,
} from "./provider/health.ts";
import { decodeBase64ToString } from "./utils.ts";
import type { CodexAuthPoolState } from "./types.ts";
import { readMeteredApiKey } from "./provider/metered.ts";
import { readSurplusApiKey } from "./provider/surplus.ts";
import {
  fetchMeteredQuotaObservation,
  getCachedConfiguredMeteredQuotaSnapshot,
  type MeteredAccountCredentials,
  type MeteredQuotaSnapshot,
  readMeteredAccountCredentials,
} from "./metered-quota.ts";

const AUTH_REFRESH_WINDOW_MS = 2 * 60_000;
const ACTIVE_UPSTREAM_HEALTH_TIMEOUT_MS = 3_000;
let activeUpstreamHealthTimeoutMs = ACTIVE_UPSTREAM_HEALTH_TIMEOUT_MS;
const AUTH_NOT_CONFIGURED = "No Codex auth configured (CODEX_AUTH_JSON_B64 or KV entry missing).";

type HealthAuthMetaBase = {
  source: "kv" | "env" | "none";
  updated_at_ms: number | null;
  access_token_exp_ms: number | null;
  account_count: number;
  accounts: {
    slot: number;
    updated_at_ms: number | null;
    access_token_exp_ms: number | null;
  }[];
};

type HealthAuthMeta = HealthAuthMetaBase & {
  access_token_expired: boolean | null;
  refresh_recommended: boolean | null;
  accounts: {
    slot: number;
    updated_at_ms: number | null;
    access_token_exp_ms: number | null;
    access_token_expired: boolean | null;
    refresh_recommended: boolean | null;
  }[];
};

type CodexAuthContext = Readonly<{
  meta: HealthAuthMetaBase;
  account_ids: string[];
}>;

type ActiveProviderProbe = {
  status: number;
  provider: "chatgpt_codex" | "metered_quota";
  content_type: string | null;
  error?: string;
};

type HealthUpstreamProbe = {
  status: number;
  auth: HealthAuthMeta | null;
  probes: {
    codex: ActiveProviderProbe;
    metered_quota: ActiveProviderProbe | null;
  };
};

type HealthProbeProgress = {
  auth: HealthAuthMeta | null;
  codex: ActiveProviderProbe | undefined;
  metered_quota: ActiveProviderProbe | null | undefined;
};

let upstreamProbeInFlight: Promise<HealthUpstreamProbe> | null = null;

export const setActiveUpstreamHealthTimeoutMsForTest = (timeoutMs: number | null): void => {
  activeUpstreamHealthTimeoutMs = timeoutMs ?? ACTIVE_UPSTREAM_HEALTH_TIMEOUT_MS;
};

const nowMs = (): number => Date.now();

const enrichAuthMeta = (meta: HealthAuthMetaBase): HealthAuthMeta => {
  const now = nowMs();
  const expMs = meta.access_token_exp_ms;
  return {
    ...meta,
    access_token_expired: typeof expMs === "number" ? expMs <= now : null,
    refresh_recommended: typeof expMs === "number" ? expMs - now < AUTH_REFRESH_WINDOW_MS : null,
    accounts: meta.accounts.map((account) => ({
      ...account,
      access_token_expired: typeof account.access_token_exp_ms === "number" ? account.access_token_exp_ms <= now : null,
      refresh_recommended: typeof account.access_token_exp_ms === "number" ? account.access_token_exp_ms - now < AUTH_REFRESH_WINDOW_MS : null,
    })),
  };
};

const loadEnvCodexAuth = (): CodexAuthContext | null => {
  if (!config.codexAuthJsonB64) return null;
  try {
    const decoded = decodeBase64ToString(config.codexAuthJsonB64);
    const parsed = JSON.parse(decoded);
    const auth = parseCodexAuthFromAuthJson(parsed);
    if (!auth) return null;
    const accessTokenExpMs = getJwtExpMs(auth.access_token);
    return {
      meta: {
        source: "env",
        updated_at_ms: null,
        access_token_exp_ms: accessTokenExpMs,
        account_count: 1,
        accounts: [
          {
            slot: 1,
            updated_at_ms: null,
            access_token_exp_ms: accessTokenExpMs,
          },
        ],
      },
      account_ids: [auth.account_id],
    };
  } catch {
    return null;
  }
};

const getCodexAuthContext = async (): Promise<CodexAuthContext> => {
  try {
    const kv = await getKv();
    if (kv) {
      const entry = await kv.get<CodexAuthPoolState>(CODEX_AUTH_POOL_KV_KEY);
      const pool = parseCodexAuthPool(entry.value);
      if (pool) {
        const accounts = pool.accounts.map((account, index) => ({
          slot: index + 1,
          updated_at_ms: account.updated_at_ms,
          access_token_exp_ms: getJwtExpMs(account.access_token),
        }));
        const expirations = accounts.map((account) => account.access_token_exp_ms).filter((value): value is number => typeof value === "number");
        return {
          meta: {
            source: "kv",
            updated_at_ms: pool.updated_at_ms,
            access_token_exp_ms: expirations.length > 0 ? Math.min(...expirations) : null,
            account_count: accounts.length,
            accounts,
          },
          account_ids: pool.accounts.map((account) => account.account_id),
        };
      }
    }
  } catch {
    // A failed auth-pool read degrades to env/none metadata instead of failing the health snapshot.
  }

  const envMeta = loadEnvCodexAuth();
  if (envMeta) return envMeta;

  return {
    meta: {
      source: "none",
      updated_at_ms: null,
      access_token_exp_ms: null,
      account_count: 0,
      accounts: [],
    },
    account_ids: [],
  };
};

const getCodexAuthMeta = async (): Promise<HealthAuthMetaBase> => (await getCodexAuthContext()).meta;

const aggregateProviderStates = (states: readonly ProviderHealthState[]): ProviderHealthState => {
  if (states.length === 0 || states.every((state) => state === "unknown")) return "unknown";
  if (states.some((state) => state === "healthy")) return "healthy";
  if (states.every((state) => state === "invalid")) return "invalid";
  if (states.every((state) => state === "exhausted")) return "exhausted";
  return "degraded";
};

const quotaView = (snapshot: MeteredQuotaSnapshot | null) => {
  if (!snapshot) {
    return {
      available: false,
      cache_state: null,
      balance_credits: null,
      baseline_credits: null,
      remaining_percent: null,
      used_percent: null,
      observed_at_ms: null,
      confidence: null,
      cycle_started_at_ms: null,
      last_credit_at_ms: null,
      unlimited_quota: null,
      total_available: null,
      total_granted: null,
      total_used: null,
    };
  }
  const tokenUsage = snapshot.unlimited_quota || snapshot.total_available !== null || snapshot.total_used !== null;
  return {
    available: true,
    cache_state: snapshot.cache_state,
    balance_credits: snapshot.balance_credits,
    baseline_credits: snapshot.baseline_credits,
    remaining_percent: snapshot.remaining_percent,
    used_percent: snapshot.used_percent,
    observed_at_ms: snapshot.state.observed_at_ms,
    confidence: tokenUsage ? null : snapshot.state.confidence,
    cycle_started_at_ms: tokenUsage ? null : snapshot.state.cycle_started_at_ms,
    last_credit_at_ms: tokenUsage ? null : snapshot.state.last_credit_at_ms,
    unlimited_quota: snapshot.unlimited_quota,
    total_available: snapshot.total_available,
    total_granted: snapshot.total_granted,
    total_used: snapshot.total_used,
  };
};

export const getPassiveProviderHealthSnapshot = async (options: Readonly<{ includeQuota?: boolean }> = {}): Promise<Record<string, unknown>> => {
  const context = await getCodexAuthContext();
  const auth = enrichAuthMeta(context.meta);
  const [cerebrasHealth, codexHealth, deepseekHealth, lithosHealth, meteredHealth, surplusHealth, meteredQuota] = await Promise.all([
    getCerebrasProviderHealth().catch(() => unknownProviderHealthView()),
    Promise.all(context.account_ids.map((accountId) => getCodexProviderHealth(accountId).catch(() => unknownProviderHealthView()))),
    getDeepSeekProviderHealth().catch(() => unknownProviderHealthView()),
    getLithosProviderHealth().catch(() => unknownProviderHealthView()),
    getMeteredProviderHealth().catch(() => unknownProviderHealthView()),
    getSurplusProviderHealth().catch(() => unknownProviderHealthView()),
    getCachedConfiguredMeteredQuotaSnapshot().catch(() => null),
  ]);
  const codexAccounts = auth.accounts.map((account, index) => ({
    ...account,
    health: codexHealth[index],
  }));
  const quotaMonitoringConfigured = readMeteredAccountCredentials() !== null;
  return {
    mode: "passive",
    generated_at_ms: Date.now(),
    stale_after_ms: PROVIDER_HEALTH_STALE_AFTER_MS,
    codex: {
      configured: auth.source !== "none",
      source: auth.source,
      account_count: auth.account_count,
      state: aggregateProviderStates(codexHealth.map((health) => health.state)),
      accounts: codexAccounts,
    },
    cerebras: {
      configured: readCerebrasApiKey() !== null,
      health: cerebrasHealth,
    },
    deepseek: {
      configured: readDeepSeekApiKey() !== null,
      health: deepseekHealth,
    },
    lithos: {
      configured: readLithosApiKey() !== null,
      health: lithosHealth,
    },
    metered: {
      configured: readMeteredApiKey() !== null,
      quota_monitoring_configured: quotaMonitoringConfigured,
      health: meteredHealth,
      ...(options.includeQuota
        ? { quota: quotaView(meteredQuota) }
        : {
            quota: {
              available: meteredQuota !== null,
              cache_state: meteredQuota?.cache_state ?? null,
              observed_at_ms: meteredQuota?.state.observed_at_ms ?? null,
            },
          }),
    },
    surplus: {
      configured: readSurplusApiKey() !== null,
      quota_monitoring_configured: false,
      health: surplusHealth,
      ...(options.includeQuota
        ? {
            quota: {
              ...quotaView(null),
              source: "not_reported",
            },
          }
        : {
            quota: {
              available: false,
              observed_at_ms: null,
            },
          }),
    },
  };
};

export const handleHealthProviders = async (options: Readonly<{ includeQuota?: boolean }> = {}): Promise<Response> =>
  json(200, await getPassiveProviderHealthSnapshot(options), {
    "Cache-Control": "no-store",
    "x-uos-git-sha": runtimeGitSha(),
    "x-uos-deployment-id": runtimeDeploymentId(),
  });

const codexProbe = async (auth: HealthAuthMeta, signal: AbortSignal): Promise<ActiveProviderProbe> => {
  if (auth.source === "none") {
    return { status: 503, provider: "chatgpt_codex", content_type: null, error: AUTH_NOT_CONFIGURED };
  }
  try {
    const res = await fetchCodexModels({ signal });
    const contentType = res.headers.get("Content-Type");
    await res.body?.cancel().catch(() => {});
    if (res.ok) return { status: 200, provider: "chatgpt_codex", content_type: contentType };
    return {
      status: res.status === 401 ? 401 : 503,
      provider: "chatgpt_codex",
      content_type: contentType,
      error: "Codex models probe returned an error status.",
    };
  } catch {
    return {
      status: 503,
      provider: "chatgpt_codex",
      content_type: null,
      error: signal.aborted ? "Codex models probe timed out." : "Codex models probe failed.",
    };
  }
};

// This non-billable endpoint authenticates the monitoring account, which is
// intentionally distinct from the inference API key. Name it accordingly so
// a green quota check is never mistaken for paid-fallback availability.
const meteredQuotaProbe = async (credentials: MeteredAccountCredentials | null, signal: AbortSignal): Promise<ActiveProviderProbe | null> => {
  if (!credentials) return null;
  try {
    await fetchMeteredQuotaObservation(credentials, { signal });
    return { status: 200, provider: "metered_quota", content_type: "application/json" };
  } catch {
    return {
      status: 503,
      provider: "metered_quota",
      content_type: null,
      error: signal.aborted ? "Metered quota probe timed out." : "Metered quota probe failed.",
    };
  }
};

const aggregateUpstreamProbe = (auth: HealthAuthMeta | null, codex: ActiveProviderProbe, meteredQuota: ActiveProviderProbe | null): HealthUpstreamProbe => {
  const failures = [codex, meteredQuota].filter((probe): probe is ActiveProviderProbe => probe !== null && probe.status >= 400);
  const probes = { codex, metered_quota: meteredQuota };
  if (failures.length === 0) return { status: 200, auth, probes };
  const status = failures.some((probe) => probe.status === 401) ? 401 : 503;
  return { status, auth, probes };
};

const probeUpstream = async (
  signal: AbortSignal,
  meteredCredentials: MeteredAccountCredentials | null,
  progress: HealthProbeProgress
): Promise<HealthUpstreamProbe> => {
  let auth: HealthAuthMeta;
  try {
    auth = enrichAuthMeta(await getCodexAuthMeta());
  } catch {
    return {
      status: 503,
      auth: null,
      probes: {
        codex: {
          status: 503,
          provider: "chatgpt_codex",
          content_type: null,
          error: "Codex auth metadata could not be read.",
        },
        metered_quota: null,
      },
    };
  }
  progress.auth = auth;
  const codexPromise = codexProbe(auth, signal).then((probe) => (progress.codex = probe));
  const meteredPromise = meteredQuotaProbe(meteredCredentials, signal).then((probe) => (progress.metered_quota = probe));
  const [codex, meteredQuota] = await Promise.all([codexPromise, meteredPromise]);
  return aggregateUpstreamProbe(auth, codex, meteredQuota);
};

const timedOutProviderProbe = (provider: ActiveProviderProbe["provider"]): ActiveProviderProbe => ({
  status: 503,
  provider,
  content_type: null,
  error: provider === "chatgpt_codex" ? "Codex models probe timed out." : "Metered quota probe timed out.",
});

const timeoutProbe = (progress: HealthProbeProgress): HealthUpstreamProbe =>
  aggregateUpstreamProbe(
    progress.auth,
    progress.codex ?? timedOutProviderProbe("chatgpt_codex"),
    progress.metered_quota === undefined ? timedOutProviderProbe("metered_quota") : progress.metered_quota
  );

const probeUpstreamCoalesced = async (): Promise<HealthUpstreamProbe> => {
  if (upstreamProbeInFlight) return await upstreamProbeInFlight;
  const controller = new AbortController();
  const meteredCredentials = readMeteredAccountCredentials();
  const progress: HealthProbeProgress = {
    auth: null,
    codex: undefined,
    metered_quota: meteredCredentials ? undefined : null,
  };
  let timeoutId: Parameters<typeof clearTimeout>[0];
  const timeout = new Promise<HealthUpstreamProbe>((resolve) => {
    timeoutId = setTimeout(() => {
      controller.abort(new DOMException("Active upstream health probe timed out.", "TimeoutError"));
      resolve(timeoutProbe(progress));
    }, activeUpstreamHealthTimeoutMs);
  });
  upstreamProbeInFlight = Promise.race([probeUpstream(controller.signal, meteredCredentials, progress), timeout]).finally(() => {
    if (timeoutId !== undefined) clearTimeout(timeoutId);
    upstreamProbeInFlight = null;
  });
  return await upstreamProbeInFlight;
};

export const handleHealth = (): Response => {
  const gitSha = runtimeGitSha();
  const deploymentId = runtimeDeploymentId();
  // This endpoint is deliberately a release liveness signal, not an active
  // dependency check. It must remain available during provider/KV incidents.
  return json(
    200,
    {
      status: "available",
      release: {
        git_sha: gitSha,
        deployment_id: deploymentId,
      },
    },
    {
      "Cache-Control": "no-store",
      "x-uos-git-sha": gitSha,
      "x-uos-deployment-id": deploymentId,
    }
  );
};

export const handleHealthUpstream = async (): Promise<Response> => {
  const probe = await probeUpstreamCoalesced();
  return json(
    probe.status,
    {
      status: probe.status,
      probes: probe.probes,
      auth: probe.auth,
    },
    { "x-uos-git-sha": runtimeGitSha(), "x-uos-deployment-id": runtimeDeploymentId() }
  );
};
