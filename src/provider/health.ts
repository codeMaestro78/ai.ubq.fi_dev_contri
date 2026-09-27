import { getKv } from "../kv.ts";
import { isRecord } from "../utils.ts";

export const PROVIDER_HEALTH_KEY_PREFIX = ["uos_ai", "provider_health", "v1"] as const;
export const PROVIDER_HEALTH_HEARTBEAT_WRITE_INTERVAL_MS = 60_000;
export const PROVIDER_HEALTH_STALE_AFTER_MS = 30 * 60_000;

export type ProviderHealthState = "healthy" | "degraded" | "exhausted" | "invalid" | "unknown";
export type ProviderHealthEvent = "success" | "reachable" | "auth_invalid" | "quota_exhausted" | "upstream_error" | "refresh_success" | "refresh_failed";

export type ProviderHealthObservation = Readonly<{
  event: ProviderHealthEvent;
  status: number | null;
  observed_at_ms: number;
  /** Opaque upstream correlation only; absent on older observations. */
  provider_request_id?: string | null;
}>;

export type ProviderHealthView = Readonly<{
  state: ProviderHealthState;
  stale: boolean | null;
  last_event: ProviderHealthEvent | null;
  last_status: number | null;
  last_observed_at_ms: number | null;
  last_provider_request_id: string | null;
  last_provider_request_id_at_ms: number | null;
  last_success_at_ms: number | null;
  last_401_at_ms: number | null;
  last_429_at_ms: number | null;
  last_error_at_ms: number | null;
  last_refresh_at_ms: number | null;
  last_refresh_succeeded: boolean | null;
}>;

/**
 * The providers the passive health view (`/health/providers`) publishes a
 * top-level key for, named after the route each one reports on: OpenLux
 * reports through `metered`.
 */
export const RECORD_PROVIDER_IDS = ["cerebras", "codex", "deepseek", "lithos", "metered", "surplus"] as const;

export type RecordProvider = (typeof RECORD_PROVIDER_IDS)[number];

const PROVIDER_RECORDS = ["current", "success", "reachable", "auth_invalid", "quota_exhausted", "upstream_error", "refresh"] as const;

const lastHeartbeatWriteAtMs = new Map<string, number>();
const lastObservation = new Map<string, ProviderHealthObservation>();
const providerTransitionQueues = new Map<
  string,
  Readonly<{
    event: ProviderHealthEvent;
    status: number | null;
    providerRequestId: string | null;
    tail: Promise<void>;
  }>
>();

const MAX_PROVIDER_REQUEST_ID_CHARS = 256;

const normalizedProviderRequestId = (value: unknown): string | null => {
  if (typeof value !== "string") return null;
  const requestId = value.trim();
  if (!requestId || requestId.length > MAX_PROVIDER_REQUEST_ID_CHARS) return null;
  for (const character of requestId) {
    const code = character.codePointAt(0) ?? 0;
    if (code < 0x20 || code === 0x7f) return null;
  }
  return requestId;
};

const providerHealthKey = (provider: RecordProvider, identity: string, record: (typeof PROVIDER_RECORDS)[number]): Deno.KvKey => [
  ...PROVIDER_HEALTH_KEY_PREFIX,
  provider,
  identity,
  record,
];

const isProviderEvent = (value: unknown): value is ProviderHealthEvent =>
  value === "success" ||
  value === "reachable" ||
  value === "auth_invalid" ||
  value === "quota_exhausted" ||
  value === "upstream_error" ||
  value === "refresh_success" ||
  value === "refresh_failed";

export const parseProviderHealthObservation = (value: unknown): ProviderHealthObservation | null => {
  const providerRequestId = isRecord(value) ? value.provider_request_id : undefined;
  if (
    !isRecord(value) ||
    !isProviderEvent(value.event) ||
    !(value.status === null || (typeof value.status === "number" && Number.isFinite(value.status))) ||
    !(typeof value.observed_at_ms === "number" && Number.isFinite(value.observed_at_ms) && value.observed_at_ms >= 0) ||
    !(providerRequestId === undefined || providerRequestId === null || normalizedProviderRequestId(providerRequestId))
  ) {
    return null;
  }
  return value as ProviderHealthObservation;
};

const stateForObservation = (observation: ProviderHealthObservation): Exclude<ProviderHealthState, "unknown"> => {
  const { event } = observation;
  if (event === "success") return "healthy";
  if (event === "auth_invalid") return "invalid";
  if (event === "refresh_failed") {
    return observation.status === 400 || observation.status === 401 || observation.status === 403 ? "invalid" : "degraded";
  }
  if (event === "quota_exhausted") return "exhausted";
  return "degraded";
};

const shouldThrottleObservation = (
  provider: RecordProvider,
  identity: string,
  event: ProviderHealthEvent,
  status: number | null,
  providerRequestId: string | null,
  nowMs: number
): boolean => {
  const throttleKey = `${provider}:${identity}`;
  const previousObservation = lastObservation.get(throttleKey);
  // A recovery is a transition, never a heartbeat. Persist it immediately
  // even when the previous healthy heartbeat was recent. The first available
  // correlation ID is also new evidence. Later IDs are sampled at the normal
  // heartbeat interval so per-request IDs do not turn this into a write path.
  const previousProviderRequestId = normalizedProviderRequestId(previousObservation?.provider_request_id);
  if (!previousObservation) return false;
  if (previousObservation.event !== event || previousObservation.status !== status || (previousProviderRequestId === null && providerRequestId !== null)) {
    return false;
  }
  const previous = lastHeartbeatWriteAtMs.get(throttleKey);
  return previous !== undefined && nowMs - previous < PROVIDER_HEALTH_HEARTBEAT_WRITE_INTERVAL_MS;
};

const historyRecordFor = (event: ProviderHealthEvent): (typeof PROVIDER_RECORDS)[number] => {
  if (event === "success") return "success";
  if (event === "reachable") return "reachable";
  if (event === "auth_invalid") return "auth_invalid";
  if (event === "quota_exhausted") return "quota_exhausted";
  if (event === "upstream_error") return "upstream_error";
  return "refresh";
};

const persistProviderHealth = async (
  provider: RecordProvider,
  identity: string,
  event: ProviderHealthEvent,
  status: number | null,
  observedAtMs: number,
  providerRequestId: string | null
): Promise<void> => {
  try {
    if (shouldThrottleObservation(provider, identity, event, status, providerRequestId, observedAtMs)) return;
    const kv = await getKv();
    if (!kv) return;
    const observation = {
      event,
      status,
      observed_at_ms: observedAtMs,
      provider_request_id: providerRequestId,
    } satisfies ProviderHealthObservation;
    const commit = await kv
      .atomic()
      .set(providerHealthKey(provider, identity, "current"), observation)
      .set(providerHealthKey(provider, identity, historyRecordFor(event)), observation)
      .commit();
    if (!commit.ok) return;
    const throttleKey = `${provider}:${identity}`;
    lastObservation.set(throttleKey, observation);
    lastHeartbeatWriteAtMs.set(throttleKey, observedAtMs);
  } catch {
    // Health observations are optional telemetry and must never affect routing.
  }
};

const recordProviderHealth = (
  provider: RecordProvider,
  identity: string,
  event: ProviderHealthEvent,
  status: number | null,
  now: () => number,
  providerRequestId: string | null
): Promise<void> => {
  const queueKey = `${provider}:${identity}`;
  const current = providerTransitionQueues.get(queueKey);
  const normalizedRequestId = normalizedProviderRequestId(providerRequestId);
  if (current?.event === event && current.status === status && (current.providerRequestId !== null || normalizedRequestId === null)) return current.tail;
  let observedAtMs: number;
  try {
    observedAtMs = Math.trunc(now());
  } catch {
    return Promise.resolve();
  }

  // Reserve the transition synchronously, before getKv() or any other await.
  // Identical cold-burst observations share this promise, while a different
  // event or the first available correlation ID is appended in arrival order.
  const tail = (current?.tail ?? Promise.resolve())
    .catch(() => {})
    .then(() => persistProviderHealth(provider, identity, event, status, observedAtMs, normalizedRequestId));
  providerTransitionQueues.set(queueKey, { event, status, providerRequestId: normalizedRequestId, tail });
  void tail.finally(() => {
    if (providerTransitionQueues.get(queueKey)?.tail === tail) providerTransitionQueues.delete(queueKey);
  });
  return tail;
};

export const recordCodexProviderHealth = (
  accountId: string,
  event: ProviderHealthEvent,
  status: number | null = null,
  now: () => number = Date.now,
  providerRequestId: string | null = null
): Promise<void> => recordProviderHealth("codex", accountId, event, status, now, providerRequestId);

export const recordMeteredProviderHealth = (
  event: ProviderHealthEvent,
  status: number | null = null,
  now: () => number = Date.now,
  providerRequestId: string | null = null
): Promise<void> => recordProviderHealth("metered", "default", event, status, now, providerRequestId);

export const recordSurplusProviderHealth = (
  event: ProviderHealthEvent,
  status: number | null = null,
  now: () => number = Date.now,
  providerRequestId: string | null = null
): Promise<void> => recordProviderHealth("surplus", "default", event, status, now, providerRequestId);

export const recordCerebrasProviderHealth = (
  event: ProviderHealthEvent,
  status: number | null = null,
  now: () => number = Date.now,
  providerRequestId: string | null = null
): Promise<void> => recordProviderHealth("cerebras", "default", event, status, now, providerRequestId);

export const recordDeepSeekProviderHealth = (
  event: ProviderHealthEvent,
  status: number | null = null,
  now: () => number = Date.now,
  providerRequestId: string | null = null
): Promise<void> => recordProviderHealth("deepseek", "default", event, status, now, providerRequestId);

export const recordLithosProviderHealth = (
  event: ProviderHealthEvent,
  status: number | null = null,
  now: () => number = Date.now,
  providerRequestId: string | null = null
): Promise<void> => recordProviderHealth("lithos", "default", event, status, now, providerRequestId);

const unknownView = (): ProviderHealthView => ({
  state: "unknown",
  stale: null,
  last_event: null,
  last_status: null,
  last_observed_at_ms: null,
  last_provider_request_id: null,
  last_provider_request_id_at_ms: null,
  last_success_at_ms: null,
  last_401_at_ms: null,
  last_429_at_ms: null,
  last_error_at_ms: null,
  last_refresh_at_ms: null,
  last_refresh_succeeded: null,
});

export const unknownProviderHealthView = (): ProviderHealthView => unknownView();

const latestObservation = (observations: readonly ProviderHealthObservation[], events?: readonly ProviderHealthEvent[]): ProviderHealthObservation | null => {
  const allowed = events ? new Set(events) : null;
  return observations
    .filter((observation) => !allowed || allowed.has(observation.event))
    .reduce<ProviderHealthObservation | null>(
      (latest, observation) => (!latest || observation.observed_at_ms > latest.observed_at_ms ? observation : latest),
      null
    );
};

const toView = (observations: readonly ProviderHealthObservation[], nowMs: number): ProviderHealthView => {
  const latest = latestObservation(observations);
  if (!latest) return unknownView();
  const latestState = latestObservation(observations, [
    "success",
    "reachable",
    "auth_invalid",
    "quota_exhausted",
    "upstream_error",
    "refresh_success",
    "refresh_failed",
  ]);
  const success = latestObservation(observations, ["success"]);
  const authInvalid = latestObservation(observations, ["auth_invalid"]);
  const exhausted = latestObservation(observations, ["quota_exhausted"]);
  const error = latestObservation(observations, ["upstream_error", "refresh_failed"]);
  const refresh = latestObservation(observations, ["refresh_success", "refresh_failed"]);
  const providerRequest = latestObservation(observations.filter((observation) => normalizedProviderRequestId(observation.provider_request_id) !== null));
  return {
    state: latestState ? stateForObservation(latestState) : "unknown",
    stale: nowMs - latest.observed_at_ms > PROVIDER_HEALTH_STALE_AFTER_MS,
    last_event: latest.event,
    last_status: latest.status,
    last_observed_at_ms: latest.observed_at_ms,
    last_provider_request_id: normalizedProviderRequestId(providerRequest?.provider_request_id),
    last_provider_request_id_at_ms: providerRequest?.observed_at_ms ?? null,
    last_success_at_ms: success?.observed_at_ms ?? null,
    last_401_at_ms: authInvalid?.observed_at_ms ?? null,
    last_429_at_ms: exhausted?.observed_at_ms ?? null,
    last_error_at_ms: error?.observed_at_ms ?? null,
    last_refresh_at_ms: refresh?.observed_at_ms ?? null,
    last_refresh_succeeded: refresh ? refresh.event === "refresh_success" : null,
  };
};

const readProviderHealth = async (provider: RecordProvider, identity: string, now: () => number): Promise<ProviderHealthView> => {
  const kv = await getKv();
  if (!kv) return unknownView();
  const entries = await Promise.all(PROVIDER_RECORDS.map((record) => kv.get<ProviderHealthObservation>(providerHealthKey(provider, identity, record))));
  const observations = entries
    .map((entry) => parseProviderHealthObservation(entry.value))
    .filter((value): value is ProviderHealthObservation => value !== null);
  return toView(observations, Math.trunc(now()));
};

export const getCodexProviderHealth = (accountId: string, now: () => number = Date.now): Promise<ProviderHealthView> =>
  readProviderHealth("codex", accountId, now);

export const getMeteredProviderHealth = (now: () => number = Date.now): Promise<ProviderHealthView> => readProviderHealth("metered", "default", now);

export const getSurplusProviderHealth = (now: () => number = Date.now): Promise<ProviderHealthView> => readProviderHealth("surplus", "default", now);

export const getCerebrasProviderHealth = (now: () => number = Date.now): Promise<ProviderHealthView> => readProviderHealth("cerebras", "default", now);

export const getDeepSeekProviderHealth = (now: () => number = Date.now): Promise<ProviderHealthView> => readProviderHealth("deepseek", "default", now);

export const getLithosProviderHealth = (now: () => number = Date.now): Promise<ProviderHealthView> => readProviderHealth("lithos", "default", now);

export const resetProviderHealthThrottleForTest = (): void => {
  lastHeartbeatWriteAtMs.clear();
  lastObservation.clear();
  providerTransitionQueues.clear();
};
