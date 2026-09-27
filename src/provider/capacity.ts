import { type CodexCapacityAccount, getCodexCapacityAccounts } from "../codex/index.ts";
import { json } from "../http.ts";
import { getKv } from "../kv.ts";
import { type CodexCapacityRoutingObservationInput, recordCodexCapacityRoutingObservations } from "../codex/account-routing.ts";
import {
  listProviderCapacityDowntimeEvents,
  listProviderCapacityRateLimitResetEvents,
  listProviderCapacityResetEvents,
  PROVIDER_CAPACITY_RESET_EVENT_RETENTION_MS,
  type ProviderCapacityDowntimeEvent,
  type ProviderCapacityRateLimitResetEvent,
  providerCapacityRateLimitResetEventKey,
  type ProviderCapacityResetEvent,
} from "./capacity-events.ts";
import { readPromptCacheAnalytics } from "../cache/prompt-analytics.ts";
import { getConfiguredMeteredQuotaSnapshot, METERED_QUOTA_FRESH_MS, type MeteredQuotaSnapshot } from "../metered-quota.ts";
import { sha256Hex } from "../utils.ts";
import {
  PROVIDER_CAPACITY_CODEX_TIMEOUT_MS,
  PROVIDER_CAPACITY_COLD_WAIT_MS,
  PROVIDER_CAPACITY_HISTORY_RETENTION_MS,
  PROVIDER_CAPACITY_LEASE_KEY,
  PROVIDER_CAPACITY_LEASE_MS,
  PROVIDER_CAPACITY_SNAPSHOT_KEY,
  PROVIDER_CAPACITY_SOURCE_STALE_MS,
} from "./capacity-contract.ts";
import type {
  CapacityLease,
  ProviderCapacityCodexSource,
  ProviderCapacityFetch,
  ProviderCapacityHistoryPoint,
  ProviderCapacitySnapshot,
  ProviderCapacitySnapshotOptions,
  ProviderCapacitySource,
  ProviderCapacityView,
  ProviderCapacityViewState,
} from "./capacity-contract.ts";
import {
  additionalRateLimitsForRouting,
  codexAccountLabel,
  codexUsageUrl,
  fillMissingCodexSparkLimitForAdmin,
  parseCodexUsage,
  providerCapacityLastAvailableKey,
  readCapacitySnapshot,
  readStoredHistoryPoint,
  readStoredSnapshot,
  safeNow,
  unavailableCodexSource,
  unavailableMeteredSource,
} from "./capacity-parse.ts";
import {
  codexAvailabilityTransitionObserved,
  codexResetTransitionObserved,
  historyPointForSnapshot,
  lastAvailableCapacityEntries,
  mergeHistoricalRateLimitResetEvents,
  mergeHistoryPoints,
  observedRateLimitResetEvents,
  providerCapacityHistoryKey,
  providerCapacityHistoryTransitionKey,
  readCapacityHistory,
  readStoredRateLimitObservation,
  recoverRateLimitObservations,
  recoverySlotsForSnapshot,
} from "./capacity-history.ts";

export const PROVIDER_CAPACITY_SNAPSHOT_RETENTION_MS = PROVIDER_CAPACITY_HISTORY_RETENTION_MS;

export {
  PROVIDER_CAPACITY_CODEX_TIMEOUT_MS,
  PROVIDER_CAPACITY_COLD_WAIT_MS,
  PROVIDER_CAPACITY_HISTORY_BUCKET_MS,
  PROVIDER_CAPACITY_HISTORY_KEY_PREFIX,
  PROVIDER_CAPACITY_HISTORY_RETENTION_MS,
  PROVIDER_CAPACITY_LEASE_KEY,
  PROVIDER_CAPACITY_LEASE_MS,
  PROVIDER_CAPACITY_RATE_LIMIT_RESET_MIN_GAIN_PERCENTAGE_POINTS,
  PROVIDER_CAPACITY_SNAPSHOT_KEY,
  PROVIDER_CAPACITY_SOURCE_STALE_MS,
} from "./capacity-contract.ts";
export type {
  ProviderCapacityAdditionalRateLimit,
  ProviderCapacityCodexSource,
  ProviderCapacityFailureKind,
  ProviderCapacityFetch,
  ProviderCapacityHistoryPoint,
  ProviderCapacityMeteredSource,
  ProviderCapacitySnapshot,
  ProviderCapacitySnapshotOptions,
  ProviderCapacitySource,
  ProviderCapacityView,
  ProviderCapacityViewState,
  ProviderCapacityWindow,
} from "./capacity-contract.ts";
export { codexAccountLabel } from "./capacity-parse.ts";
export { providerCapacityHistoryKey } from "./capacity-history.ts";

const cancelResponseBody = (response: Response): void => {
  try {
    const cancellation = response.body?.cancel();
    if (cancellation) void Promise.resolve(cancellation).catch(() => {});
  } catch {
    // The capacity response is already classified as unavailable.
  }
};

const fetchCodexCapacitySource = async (
  account: CodexCapacityAccount,
  accountCohortId: string,
  snapshotAtMs: number,
  fetcher: ProviderCapacityFetch,
  signal: AbortSignal
): Promise<ProviderCapacityCodexSource> => {
  const headers = new Headers({
    Accept: "application/json",
    Authorization: "Bearer " + account.access_token,
    "ChatGPT-Account-ID": account.account_id,
    "User-Agent": "codex_cli_rs/0.100.0 (ai.ubq.fi)",
  });
  try {
    const response = await fetcher(codexUsageUrl(), {
      method: "GET",
      headers,
      redirect: "manual",
      signal,
    });
    if (!response.ok) {
      cancelResponseBody(response);
      return unavailableCodexSource(
        account.slot as 1 | 2,
        snapshotAtMs,
        response.status >= 500 ? "upstream_error" : "http_error",
        response.status,
        true,
        codexAccountLabel(account.slot, account.email),
        accountCohortId
      );
    }
    let payload: unknown;
    try {
      payload = await response.json();
    } catch {
      return unavailableCodexSource(
        account.slot as 1 | 2,
        snapshotAtMs,
        "invalid_response",
        response.status,
        true,
        codexAccountLabel(account.slot, account.email),
        accountCohortId
      );
    }
    const windows = parseCodexUsage(payload);
    if (!windows) {
      return unavailableCodexSource(
        account.slot as 1 | 2,
        snapshotAtMs,
        "invalid_response",
        response.status,
        true,
        codexAccountLabel(account.slot, account.email),
        accountCohortId
      );
    }
    return {
      source: "codex",
      label: codexAccountLabel(account.slot, account.email),
      slot: account.slot as 1 | 2,
      account_cohort_id: accountCohortId,
      state: "available",
      source_observed_at_ms: snapshotAtMs,
      snapshot_at_ms: snapshotAtMs,
      failure_kind: null,
      failure_status: null,
      windows: {
        primary: windows.primary,
        secondary: windows.secondary,
      },
      additional_rate_limits: windows.additional_rate_limits,
    };
  } catch {
    return unavailableCodexSource(
      account.slot as 1 | 2,
      snapshotAtMs,
      "unreachable",
      null,
      true,
      codexAccountLabel(account.slot, account.email),
      accountCohortId
    );
  }
};

const meteredCapacitySource = (snapshot: MeteredQuotaSnapshot | null, snapshotAtMs: number): ProviderCapacitySource => {
  if (!snapshot) return unavailableMeteredSource(snapshotAtMs);
  const sourceObservedAtMs = snapshot.state.observed_at_ms;
  const stale = snapshot.cache_state === "stale" || snapshotAtMs - sourceObservedAtMs >= METERED_QUOTA_FRESH_MS;
  const tokenUsage = snapshot.unlimited_quota || snapshot.total_available !== null || snapshot.total_used !== null;
  return {
    source: "metered",
    label: "Metered fallback",
    state: stale ? "stale" : "available",
    source_observed_at_ms: sourceObservedAtMs,
    snapshot_at_ms: snapshotAtMs,
    wallet: {
      balance_credits: snapshot.balance_credits,
      baseline_credits: snapshot.baseline_credits,
      refill_cycle_remaining_percent: snapshot.remaining_percent,
      refill_cycle_used_percent: snapshot.used_percent,
      unlimited_quota: snapshot.unlimited_quota,
      total_available: snapshot.total_available,
      total_granted: snapshot.total_granted,
      total_used: snapshot.total_used,
      cycle_started_at_ms: tokenUsage ? null : snapshot.state.cycle_started_at_ms,
      last_credit_at_ms: tokenUsage ? null : snapshot.state.last_credit_at_ms,
      confidence: tokenUsage ? null : snapshot.state.confidence,
      cache_state: snapshot.cache_state,
      // Metered exposes a refill cycle, not a scheduled reset window.
      reset_at_ms: null,
    },
  };
};

const captureProviderCapacitySnapshot = async (
  options: ProviderCapacitySnapshotOptions,
  snapshotAtMs: number,
  kv: Deno.Kv | null
): Promise<ProviderCapacitySnapshot> => {
  let accounts: readonly CodexCapacityAccount[] = [];
  try {
    accounts = await getCodexCapacityAccounts();
  } catch {
    // Missing or malformed auth is represented by redacted unavailable slots.
  }

  const fetcher = options.fetcher ?? ((input: RequestInfo | URL, init?: RequestInit) => globalThis.fetch(input, init));
  const timeout = AbortSignal.timeout(PROVIDER_CAPACITY_CODEX_TIMEOUT_MS);
  const signal = options.signal ? AbortSignal.any([options.signal, timeout]) : timeout;
  const codexPromise = Promise.all(
    ([1, 2] as const).map(async (slot) => {
      const account = accounts.find((candidate) => candidate.slot === slot);
      try {
        return account
          ? await fetchCodexCapacitySource(
              account,
              await sha256Hex(`uos-prompt-cache-account-cohort-v1\u0000${account.account_id}`),
              snapshotAtMs,
              fetcher,
              signal
            )
          : unavailableCodexSource(slot, snapshotAtMs);
      } catch {
        // A configured slot that throws here is unreachable, not unconfigured; only a missing account keeps the default kind.
        return account ? unavailableCodexSource(slot, snapshotAtMs, "unreachable", null, true) : unavailableCodexSource(slot, snapshotAtMs);
      }
    })
  );
  const meteredPromise = getConfiguredMeteredQuotaSnapshot({
    kv,
    fetcher,
    now: () => snapshotAtMs,
    signal,
    forceRefresh: true,
    createLeaseOwner: options.createLeaseOwner,
  }).catch(() => null);
  const [codexSources, meteredSnapshot] = await Promise.all([codexPromise, meteredPromise]);

  const routingObservations: CodexCapacityRoutingObservationInput[] = [];
  for (const account of accounts) {
    const source = codexSources.find((candidate) => candidate.slot === account.slot);
    if (!source) continue;
    routingObservations.push({
      slot: account.slot - 1,
      account_id: account.account_id,
      state: source.state,
      source_observed_at_ms: source.source_observed_at_ms,
      snapshot_at_ms: source.snapshot_at_ms,
      windows: source.windows,
      additional_rate_limits: additionalRateLimitsForRouting(source.additional_rate_limits, snapshotAtMs),
    });
  }
  await recordCodexCapacityRoutingObservations(routingObservations, snapshotAtMs);

  return {
    snapshot_at_ms: snapshotAtMs,
    stale_after_ms: PROVIDER_CAPACITY_SOURCE_STALE_MS,
    sources: [codexSources[0], codexSources[1], meteredCapacitySource(meteredSnapshot, snapshotAtMs)],
  };
};

const staleProviderCapacitySource = (source: ProviderCapacitySource, snapshot: ProviderCapacitySnapshot, nowMs: number): ProviderCapacitySource => {
  if (source.state !== "available") return source;
  if (source.source === "codex") {
    return nowMs >= source.snapshot_at_ms + snapshot.stale_after_ms ? { ...source, state: "stale" as const } : source;
  }
  const meteredStale =
    source.wallet.cache_state === "stale" || source.source_observed_at_ms === null || nowMs - source.source_observed_at_ms >= METERED_QUOTA_FRESH_MS;
  return meteredStale ? { ...source, state: "stale" as const } : source;
};

const staleProviderSnapshot = (snapshot: ProviderCapacitySnapshot, nowMs: number): ProviderCapacitySnapshot => {
  return {
    ...snapshot,
    sources: snapshot.sources.map((source) => staleProviderCapacitySource(source, snapshot, nowMs)) as [
      ProviderCapacitySource,
      ProviderCapacitySource,
      ProviderCapacitySource,
    ],
  };
};

const toCapacityView = (
  snapshot: ProviderCapacitySnapshot,
  requestedState: Exclude<ProviderCapacityViewState, "unavailable">,
  history: readonly ProviderCapacityHistoryPoint[],
  resetEvents: readonly ProviderCapacityResetEvent[],
  rateLimitResetEvents: readonly ProviderCapacityRateLimitResetEvent[],
  downtimeEvents: readonly ProviderCapacityDowntimeEvent[],
  nowMs: number
): ProviderCapacityView => {
  const current = staleProviderSnapshot(snapshot, nowMs);
  const codexSources = fillMissingCodexSparkLimitForAdmin(current.sources.filter((source): source is ProviderCapacityCodexSource => source.source === "codex"));
  const projected = {
    ...current,
    sources: [codexSources[0] ?? current.sources[0], codexSources[1] ?? current.sources[1], current.sources[2]],
  } as ProviderCapacitySnapshot;
  const stale = projected.sources.some((source) => source.state === "stale");
  return {
    ...projected,
    cache_state: stale ? "stale" : requestedState,
    history,
    reset_events: resetEvents,
    rate_limit_reset_events: rateLimitResetEvents,
    downtime_events: downtimeEvents,
  };
};

const unavailableSnapshot = (snapshotAtMs: number): ProviderCapacitySnapshot => ({
  snapshot_at_ms: snapshotAtMs,
  stale_after_ms: PROVIDER_CAPACITY_SOURCE_STALE_MS,
  sources: [unavailableCodexSource(1, snapshotAtMs), unavailableCodexSource(2, snapshotAtMs), unavailableMeteredSource(snapshotAtMs)],
});

const unavailableView = (
  snapshotAtMs: number,
  history: readonly ProviderCapacityHistoryPoint[],
  resetEvents: readonly ProviderCapacityResetEvent[],
  rateLimitResetEvents: readonly ProviderCapacityRateLimitResetEvent[],
  downtimeEvents: readonly ProviderCapacityDowntimeEvent[]
): ProviderCapacityView => ({
  ...unavailableSnapshot(snapshotAtMs),
  cache_state: "unavailable",
  history,
  reset_events: resetEvents,
  rate_limit_reset_events: rateLimitResetEvents,
  downtime_events: downtimeEvents,
});

const acquireCapacityLease = async (kv: Deno.Kv, owner: string, nowMs: number): Promise<{ acquired: boolean; entry: Deno.KvEntryMaybe<CapacityLease> }> => {
  const entry = await kv.get<CapacityLease>(PROVIDER_CAPACITY_LEASE_KEY);
  if (entry.value && entry.value.lease_until_ms > nowMs) return { acquired: false, entry };
  const lease: CapacityLease = { owner, lease_until_ms: nowMs + PROVIDER_CAPACITY_LEASE_MS };
  const committed = await kv
    .atomic()
    .check(entry)
    .set(PROVIDER_CAPACITY_LEASE_KEY, lease, { expireIn: PROVIDER_CAPACITY_LEASE_MS * 2 })
    .commit();
  if (!committed.ok) return { acquired: false, entry };
  const acquiredEntry = await kv.get<CapacityLease>(PROVIDER_CAPACITY_LEASE_KEY);
  return acquiredEntry.value?.owner === owner
    ? { acquired: true, entry: acquiredEntry }
    : {
        acquired: false,
        entry: acquiredEntry,
      };
};

const releaseCapacityLease = async (kv: Deno.Kv, owner: string): Promise<void> => {
  try {
    const entry = await kv.get<CapacityLease>(PROVIDER_CAPACITY_LEASE_KEY);
    if (entry.value?.owner !== owner) return;
    await kv.atomic().check(entry).delete(PROVIDER_CAPACITY_LEASE_KEY).commit();
  } catch {
    // A short-lived lease can expire without affecting the redacted snapshot.
  }
};

const persistCapacitySnapshot = async (kv: Deno.Kv, leaseEntry: Deno.KvEntryMaybe<CapacityLease>, snapshot: ProviderCapacitySnapshot): Promise<boolean> => {
  const history = historyPointForSnapshot(snapshot);
  const comparisonReads = await Promise.allSettled([
    kv.get(PROVIDER_CAPACITY_SNAPSHOT_KEY, { consistency: "strong" }),
    kv.get(providerCapacityHistoryKey(history.bucket_start_at_ms), { consistency: "strong" }),
    ...([1, 2] as const).map((slot) => kv.get(providerCapacityLastAvailableKey(slot), { consistency: "strong" })),
  ]);
  const storedEntries: Deno.KvEntryMaybe<unknown>[] = [];
  for (const result of comparisonReads) {
    if (result.status === "rejected") return false;
    storedEntries.push(result.value);
  }
  const previousSnapshot = readStoredSnapshot(storedEntries[0]?.value);
  const previousHistory = readStoredHistoryPoint(storedEntries[1]?.value);
  const lastAvailableObservations = storedEntries.slice(2).flatMap((entry) => {
    const observation = readStoredRateLimitObservation(entry.value);
    return observation ? [observation] : [];
  });
  const recoverySlots = recoverySlotsForSnapshot(snapshot, previousSnapshot, lastAvailableObservations);
  const recoveredObservations = await recoverRateLimitObservations(kv, snapshot, recoverySlots);
  const rateLimitResetEvents = observedRateLimitResetEvents(previousSnapshot, snapshot, [...lastAvailableObservations, ...recoveredObservations]);
  const preserveTransition =
    rateLimitResetEvents.length > 0 || codexResetTransitionObserved(previousHistory, history) || codexAvailabilityTransitionObserved(previousHistory, history);
  let operation = kv.atomic().check(leaseEntry).set(PROVIDER_CAPACITY_SNAPSHOT_KEY, snapshot, { expireIn: PROVIDER_CAPACITY_SNAPSHOT_RETENTION_MS });
  for (const event of rateLimitResetEvents) {
    operation = operation.set(providerCapacityRateLimitResetEventKey(event.event_id), event, {
      expireIn: PROVIDER_CAPACITY_RESET_EVENT_RETENTION_MS,
    });
  }
  for (const entry of lastAvailableCapacityEntries(snapshot)) {
    operation = operation.set(providerCapacityLastAvailableKey(entry.slot), entry.value, { expireIn: PROVIDER_CAPACITY_RESET_EVENT_RETENTION_MS });
  }
  if (preserveTransition && previousHistory) {
    operation = operation.set(providerCapacityHistoryTransitionKey(history.bucket_start_at_ms, previousHistory.sampled_at_ms), previousHistory, {
      expireIn: PROVIDER_CAPACITY_HISTORY_RETENTION_MS,
    });
  }
  const committed = await operation
    .set(providerCapacityHistoryKey(history.bucket_start_at_ms), history, {
      expireIn: PROVIDER_CAPACITY_HISTORY_RETENTION_MS,
    })
    .delete(PROVIDER_CAPACITY_LEASE_KEY)
    .commit();
  return committed.ok;
};

const waitForCapacitySnapshot = async (kv: Deno.Kv, previousSnapshotAtMs: number | null): Promise<ProviderCapacitySnapshot | null> => {
  const deadline = Date.now() + PROVIDER_CAPACITY_COLD_WAIT_MS;
  while (Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 50));
    const [snapshot, lease] = await Promise.all([readCapacitySnapshot(kv), kv.get<CapacityLease>(PROVIDER_CAPACITY_LEASE_KEY).catch(() => null)]);
    const leaseFinished = !lease?.value || lease.value.lease_until_ms <= Date.now();
    if (snapshot && (previousSnapshotAtMs === null || snapshot.snapshot_at_ms !== previousSnapshotAtMs || leaseFinished)) {
      return snapshot;
    }
    if (!snapshot && leaseFinished) return null;
  }
  return null;
};

export const getPersistedProviderCapacityView = async (options: Pick<ProviderCapacitySnapshotOptions, "kv" | "now"> = {}): Promise<ProviderCapacityView> => {
  const nowMs = safeNow(options.now ?? Date.now);
  const kv = options.kv === undefined ? await getKv() : options.kv;
  if (!kv) return unavailableView(nowMs, [], [], [], []);
  const [snapshot, history, resetEvents, rateLimitResetEvents, downtimeEvents] = await Promise.all([
    readCapacitySnapshot(kv),
    readCapacityHistory(kv, nowMs),
    listProviderCapacityResetEvents({ kv, now: () => nowMs }),
    listProviderCapacityRateLimitResetEvents({ kv, now: () => nowMs }),
    listProviderCapacityDowntimeEvents({ kv, now: () => nowMs }),
  ]);
  const mergedRateLimitResetEvents = await mergeHistoricalRateLimitResetEvents(kv, history, rateLimitResetEvents);
  return snapshot
    ? toCapacityView(snapshot, "persisted", history, resetEvents, mergedRateLimitResetEvents, downtimeEvents, nowMs)
    : unavailableView(nowMs, history, resetEvents, mergedRateLimitResetEvents, downtimeEvents);
};

export const refreshProviderCapacity = async (options: ProviderCapacitySnapshotOptions = {}): Promise<ProviderCapacityView> => {
  const nowMs = safeNow(options.now ?? Date.now);
  const kv = options.kv === undefined ? await getKv() : options.kv;
  if (!kv) {
    const snapshot = await captureProviderCapacitySnapshot(options, nowMs, null);
    return toCapacityView(snapshot, "live", [historyPointForSnapshot(snapshot)], [], [], [], nowMs);
  }

  const [cached, historyBefore, resetEventsBefore, rateLimitResetEventsBefore, downtimeEventsBefore] = await Promise.all([
    readCapacitySnapshot(kv),
    readCapacityHistory(kv, nowMs),
    listProviderCapacityResetEvents({ kv, now: () => nowMs }),
    listProviderCapacityRateLimitResetEvents({ kv, now: () => nowMs }),
    listProviderCapacityDowntimeEvents({ kv, now: () => nowMs }),
  ]);
  const owner = (options.createLeaseOwner ?? (() => crypto.randomUUID()))();
  const lease = await acquireCapacityLease(kv, owner, nowMs).catch(
    () =>
      ({
        acquired: false,
        entry: { key: PROVIDER_CAPACITY_LEASE_KEY, value: null, versionstamp: null },
      }) as { acquired: boolean; entry: Deno.KvEntryMaybe<CapacityLease> }
  );
  if (!lease.acquired) {
    const coalesced = await waitForCapacitySnapshot(kv, cached?.snapshot_at_ms ?? null).catch(() => null);
    const snapshot = coalesced ?? cached;
    const history = await readCapacityHistory(kv, nowMs).catch(() => historyBefore);
    const resetEvents = await listProviderCapacityResetEvents({ kv, now: () => nowMs }).catch(() => resetEventsBefore);
    const rateLimitResetEvents = await listProviderCapacityRateLimitResetEvents({ kv, now: () => nowMs }).catch(() => rateLimitResetEventsBefore);
    const downtimeEvents = await listProviderCapacityDowntimeEvents({ kv, now: () => nowMs }).catch(() => downtimeEventsBefore);
    const mergedRateLimitResetEvents = await mergeHistoricalRateLimitResetEvents(kv, history, rateLimitResetEvents);
    return snapshot
      ? toCapacityView(snapshot, "persisted", history, resetEvents, mergedRateLimitResetEvents, downtimeEvents, nowMs)
      : unavailableView(nowMs, history, resetEvents, mergedRateLimitResetEvents, downtimeEvents);
  }

  try {
    const snapshot = await captureProviderCapacitySnapshot(options, nowMs, kv);
    const persisted = await persistCapacitySnapshot(kv, lease.entry, snapshot).catch(() => false);
    const history = await readCapacityHistory(kv, nowMs).catch(() => historyBefore);
    const resetEvents = await listProviderCapacityResetEvents({ kv, now: () => nowMs }).catch(() => resetEventsBefore);
    const rateLimitResetEvents = await listProviderCapacityRateLimitResetEvents({ kv, now: () => nowMs }).catch(() => rateLimitResetEventsBefore);
    const downtimeEvents = await listProviderCapacityDowntimeEvents({ kv, now: () => nowMs }).catch(() => downtimeEventsBefore);
    const mergedRateLimitResetEvents = await mergeHistoricalRateLimitResetEvents(kv, history, rateLimitResetEvents);
    return toCapacityView(
      snapshot,
      "live",
      persisted ? history : mergeHistoryPoints(history, historyPointForSnapshot(snapshot)),
      resetEvents,
      mergedRateLimitResetEvents,
      downtimeEvents,
      nowMs
    );
  } finally {
    await releaseCapacityLease(kv, owner);
  }
};

/**
 * Persist one capacity sample for an observed event, without building the admin
 * projection.
 *
 * Replaces the retired fifteen-minute deploy cron. The trigger is a capacity observation
 * - a Codex rate-limit reset, an upstream downtime - or an operator opening the
 * capacity view; `triggerProviderCapacitySample` is the fire-and-forget entry
 * point and debounces to one probe per history bucket. Nothing here runs because
 * time passed.
 *
 * The caller does not consume a ProviderCapacityView, so scanning the seven-day
 * history and reset-event ledgers before and after every sample only pays to
 * construct a discarded response. Keep the capture, routing observations, lease,
 * snapshot/history write, and same-bucket reset transition exactly on the
 * durable sampler path. Admin callers continue to use refreshProviderCapacity()
 * when they need the full projection.
 */
export const sampleProviderCapacityOnEvent = async (options: ProviderCapacitySnapshotOptions = {}): Promise<void> => {
  const nowMs = safeNow(options.now ?? Date.now);
  const kv = options.kv === undefined ? await getKv() : options.kv;
  // A scheduled sample without durable storage would only create provider
  // traffic while leaving routing and history state stale.
  if (!kv) return;

  const owner = (options.createLeaseOwner ?? (() => crypto.randomUUID()))();
  const lease = await acquireCapacityLease(kv, owner, nowMs).catch(
    () =>
      ({
        acquired: false,
        entry: { key: PROVIDER_CAPACITY_LEASE_KEY, value: null, versionstamp: null },
      }) as { acquired: boolean; entry: Deno.KvEntryMaybe<CapacityLease> }
  );
  // The event caller has no view to return. A competing live refresh owns the
  // only probe, so avoid both a duplicate probe and the old coalesced-view
  // polling loop.
  if (!lease.acquired) return;

  let persisted = false;
  try {
    const snapshot = await captureProviderCapacitySnapshot(options, nowMs, kv);
    persisted = await persistCapacitySnapshot(kv, lease.entry, snapshot).catch(() => false);
  } finally {
    // A successful atomic persist deletes the checked lease. Only failed or
    // interrupted persistence needs the conservative owner-checked release.
    if (!persisted) await releaseCapacityLease(kv, owner);
  }
};

export const handleProviderCapacity = async (
  request: Request = new Request("https://ai.ubq.fi/admin/providers/capacity"),
  options: ProviderCapacitySnapshotOptions = {}
): Promise<Response> => {
  const promptCache = readPromptCacheAnalytics({ kv: options.kv, now: options.now });
  try {
    const live = new URL(request.url).searchParams.get("refresh") === "live";
    const view = live ? await refreshProviderCapacity(options) : await getPersistedProviderCapacityView(options);
    return json(200, { ...view, prompt_cache: await promptCache }, { "Cache-Control": "no-store" });
  } catch {
    return json(200, { ...unavailableView(Date.now(), [], [], [], []), prompt_cache: await promptCache }, { "Cache-Control": "no-store" });
  }
};
