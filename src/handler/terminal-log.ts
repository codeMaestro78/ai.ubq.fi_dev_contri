// Terminal request logging and delivery decoration, split out of src/handler.ts.

import type { BodyOutcome, DeliveryOutcome, RequestDeliveryInfo, SentinelBackgroundTaskRegistrar } from "./http.ts";
import { scheduleSentinelBackgroundTask, shouldSignalSentinelProviderDegradation } from "./http.ts";

import { recordAdminError } from "../admin/error-log.ts";
import { runtimeDeploymentId, runtimeGitSha } from "../config.ts";
import { getResponseAccountCohortId, getResponseTelemetry } from "../openai-telemetry.ts";
import { enqueuePromptCacheAnalytics, recordPromptCacheAnalytics } from "../cache/prompt-analytics.ts";
import { recordPromptCacheTelemetry } from "../cache/telemetry-gate.ts";
import {
  type AcceptedSentinelReplayInput,
  createSentinelSseInspector,
  disposeSentinelUpstreamRecorder,
  inspectSentinelBufferedResponseBody,
  persistSentinelReplayFromEnvironment,
  recordSentinelReplayOmissionFromEnvironment,
  resolveSentinelClientFailureObservation,
  type SentinelClientBodyObservation,
  type SentinelFailureObservation,
  type SentinelReplayCaptureOmissionReason,
  shouldPersistSentinelReplay,
  snapshotSentinelReplayInput,
  zeroSentinelReplayInput,
} from "../sentinel/replay-capture.ts";
import type { recordSentinelProviderDegradationFromEnvironment } from "../sentinel/incident-outbox.ts";

// A sink invoked while building the terminal batch can throw synchronously
// (injected test seams included), which would bypass allSettled below and
// abort sibling writes. Capturing the call converts that throw into a
// rejection so every sibling still settles.
const settleTerminalWrite = (write: () => unknown): Promise<unknown> => {
  try {
    return Promise.resolve(write());
  } catch (error) {
    return Promise.reject(error);
  }
};

const logTerminalRequest = async (
  input: Readonly<{
    route: string;
    response: Response;
    telemetryResponse?: Response;
    startedAtMonotonicMs: number;
    downstreamDrainedAtMonotonicMs?: number;
    deliveryOutcome: DeliveryOutcome;
    requestId: string;
    recordCacheAnalytics?: typeof recordPromptCacheAnalytics;
    recordTelemetry?: typeof recordPromptCacheTelemetry;
    sentinelReplayInput?: AcceptedSentinelReplayInput | null;
    persistSentinelReplay?: typeof persistSentinelReplayFromEnvironment;
    recordSentinelDegradation?: typeof recordSentinelProviderDegradationFromEnvironment;
    recordAdminError?: typeof recordAdminError;
    streamReadFailure?: boolean;
    suppressSentinelReplay?: boolean;
    /** Time this request spent waiting for a process-resource permit, if it queued. */
    admissionWaitMs?: number | null;
    resolveClientBodyObservation?: () => SentinelClientBodyObservation | null | Promise<SentinelClientBodyObservation | null>;
  }>
): Promise<void> => {
  const telemetry = getResponseTelemetry(input.telemetryResponse ?? input.response);
  const accountCohortId = getResponseAccountCohortId(input.telemetryResponse ?? input.response);
  const latencyMs = Math.max(0, Math.round(performance.now() - input.startedAtMonotonicMs));
  const downstreamDrainMs =
    telemetry?.stream === true &&
    telemetry.firstSemanticCommitmentMs !== null &&
    telemetry.streamTerminalMs !== null &&
    input.downstreamDrainedAtMonotonicMs !== undefined
      ? Math.max(0, Math.round(input.downstreamDrainedAtMonotonicMs - input.startedAtMonotonicMs) - telemetry.streamTerminalMs)
      : null;
  const terminal = {
    request_id: input.requestId,
    route: input.route,
    status: input.response.status,
    provider: telemetry?.provider ?? input.response.headers.get("x-uos-upstream") ?? "gateway",
    latency_ms: latencyMs,
    admission_wait_ms: input.admissionWaitMs ?? null,
    first_provider_dispatch_ms: telemetry?.firstProviderDispatchMs ?? null,
    first_provider_headers_ms: telemetry?.firstProviderHeadersMs ?? null,
    first_codex_dispatch_ms: telemetry?.firstCodexDispatchMs ?? null,
    first_codex_headers_ms: telemetry?.firstCodexHeadersMs ?? null,
    first_upstream_sse_event_ms: telemetry?.firstUpstreamSseEventMs ?? null,
    first_semantic_commitment_ms: telemetry?.firstSemanticCommitmentMs ?? null,
    stream_terminal_ms: telemetry?.streamTerminalMs ?? null,
    downstream_drain_ms: downstreamDrainMs,
    delivery_outcome: input.deliveryOutcome,
    model: telemetry?.model ?? null,
    reasoning: telemetry?.reasoning ?? null,
    output_token_allowance: telemetry?.outputTokenAllowance ?? null,
    rate_limit_wait_ms: telemetry?.rateLimitWaitMs ?? null,
    rate_limit_failover_model: telemetry?.rateLimitFailoverModel ?? null,
    provider_request_id: telemetry?.providerRequestId ?? null,
    input_tokens: telemetry?.inputTokens ?? null,
    cached_input_tokens: telemetry?.cachedInputTokens ?? null,
    cache_write_input_tokens: telemetry?.cacheWriteInputTokens ?? null,
    output_tokens: telemetry?.outputTokens ?? null,
    total_tokens: telemetry?.totalTokens ?? null,
    usage_observed: telemetry?.usageObserved ?? false,
    usage_telemetry_status: telemetry?.usageTelemetryStatus ?? "missing",
    prompt_cache_key_present: telemetry?.promptCacheKeyPresent ?? false,
    prompt_cache_mode: telemetry?.promptCacheMode ?? "unspecified",
    explicit_breakpoint_count: telemetry?.explicitBreakpointCount ?? 0,
    account_slot: telemetry?.accountSlot ?? null,
    account_cohort_id: accountCohortId,
    active_generation: telemetry?.activeGeneration ?? null,
    active_transition_reason: telemetry?.activeTransitionReason ?? null,
    fallback_reason: telemetry?.fallbackReason ?? null,
    semantic_output_observed: telemetry?.semanticOutputObserved ?? null,
    upstream_event_kinds: telemetry?.upstreamEventKinds ?? [],
    stream: input.streamReadFailure ? (telemetry?.stream ?? true) : (telemetry?.stream ?? null),
    stream_terminal_type: input.streamReadFailure ? (telemetry?.streamTerminalType ?? "error") : (telemetry?.streamTerminalType ?? null),
    failure_kind: input.streamReadFailure ? (telemetry?.failureKind ?? "gateway_stream_read_error") : (telemetry?.failureKind ?? null),
    response_created_observed: telemetry?.responseCreatedObserved ?? false,
    synthetic_terminal_type: telemetry?.syntheticTerminalType ?? null,
    attempted_providers: telemetry?.attemptedProviders ?? [],
    removed_provider_trigger_class: telemetry?.removedProviderTriggerClass ?? null,
    removed_provider_circuit_transition: telemetry?.removedProviderCircuitTransition ?? null,
    removed_provider_selected_model: telemetry?.removedProviderSelectedModel ?? null,
    removed_provider_task_type: telemetry?.removedProviderTaskType ?? null,
    removed_provider_latency_ms: telemetry?.removedProviderLatencyMs ?? null,
    removed_provider_terminal_status: telemetry?.removedProviderTerminalStatus ?? null,
    removed_provider_semantic_commitment: telemetry?.removedProviderSemanticCommitment ?? null,
    git_sha: runtimeGitSha(),
    deno_revision: runtimeDeploymentId(),
    router_revision: input.response.headers.get("x-uos-router-revision"),
  };
  console.info("[ai.ubq.fi] request_terminal", JSON.stringify(terminal));
  const telemetryWrite = settleTerminalWrite(() => (input.recordTelemetry ?? recordPromptCacheTelemetry)({
    provider: terminal.provider,
    model: terminal.model,
    route: terminal.route,
    status: terminal.status,
    completed: input.streamReadFailure ? false : (telemetry?.completed ?? false),
    usageTelemetryStatus: terminal.usage_telemetry_status,
    cacheWriteTokensPresent: terminal.cache_write_input_tokens !== null,
  }));
  // Optional aggregate analytics is enqueued on the bounded best-effort queue
  // by default, so a slow analytics sink cannot extend the terminal handoff.
  // The explicit test seam still injects the direct writer. Every other write
  // in this batch stays awaited but best-effort: a rejected sink is warned
  // below and never fails the terminal handoff or erases sibling evidence.
  const cacheAnalyticsEvent = {
    provider: terminal.provider,
    model: terminal.model,
    route: terminal.route,
    status: terminal.status,
    completed: input.streamReadFailure ? false : (telemetry?.completed ?? false),
    usageTelemetryStatus: terminal.usage_telemetry_status,
    inputTokens: terminal.input_tokens,
    cachedInputTokens: terminal.cached_input_tokens,
    cacheWriteInputTokens: terminal.cache_write_input_tokens,
    promptCacheKeyPresent: terminal.prompt_cache_key_present,
    promptCacheMode: terminal.prompt_cache_mode,
    fallbackReason: terminal.fallback_reason,
  };
  const cacheAnalyticsWrite = settleTerminalWrite(() =>
    input.recordCacheAnalytics ? input.recordCacheAnalytics(cacheAnalyticsEvent) : enqueuePromptCacheAnalytics(cacheAnalyticsEvent)
  );
  const replayObservation: SentinelFailureObservation = {
    status: terminal.status,
    stream: terminal.stream,
    completed: input.streamReadFailure ? false : (telemetry?.completed ?? false),
    terminal_type: terminal.stream_terminal_type,
    failure_kind: terminal.failure_kind,
    synthetic_terminal_type: terminal.synthetic_terminal_type,
    provider_route: terminal.provider,
  };
  try {
    const clientBodyObservation = (await input.resolveClientBodyObservation?.()) ?? null;
    const clientObservation = resolveSentinelClientFailureObservation(replayObservation, clientBodyObservation);
    const replayWrite =
      input.sentinelReplayInput && !input.suppressSentinelReplay && shouldPersistSentinelReplay(replayObservation, clientObservation)
        ? settleTerminalWrite(() =>
            (input.persistSentinelReplay ?? persistSentinelReplayFromEnvironment)(input.sentinelReplayInput, replayObservation, clientObservation)
          )
        : Promise.resolve();
    const degradationWrite = shouldSignalSentinelProviderDegradation({
      status: terminal.status,
      completed: telemetry?.completed ?? false,
      removedProviderTriggerClass: terminal.removed_provider_trigger_class,
    })
      ? settleTerminalWrite(() => input.recordSentinelDegradation?.(Date.now()))
      : Promise.resolve();
    const adminErrorWrite = settleTerminalWrite(() => (input.recordAdminError ?? recordAdminError)({
      request_id: terminal.request_id,
      route: terminal.route,
      status: terminal.status,
      provider: terminal.provider,
      model: terminal.model,
      reasoning: terminal.reasoning,
      stream: terminal.stream,
      terminal_type: clientObservation.terminal_type,
      failure_kind: clientObservation.failure_kind,
      delivery_outcome: terminal.delivery_outcome,
      created_at_ms: Date.now(),
      latency_ms: terminal.latency_ms,
      git_sha: terminal.git_sha,
      deno_revision: terminal.deno_revision,
    }));
    const settled = await Promise.allSettled([telemetryWrite, cacheAnalyticsWrite, replayWrite, degradationWrite, adminErrorWrite]);
    const rejected = settled.filter((result): result is PromiseRejectedResult => result.status === "rejected");
    if (rejected.length > 0) {
      const classes = rejected.map((item) => (item.reason instanceof Error ? item.reason.name : typeof item.reason));
      console.warn("[ai.ubq.fi] terminal_writes_failed", JSON.stringify({ request_id: terminal.request_id, classes }));
    }
  } finally {
    zeroSentinelReplayInput(input.sentinelReplayInput);
  }
};

export const warnQuotaAccountingFailure = (input: Readonly<{ route: string; requestId: string }>, error: unknown): void => {
  const errors = error instanceof AggregateError ? error.errors : [error];
  try {
    console.warn(
      "[ai.ubq.fi] quota_accounting_failed",
      JSON.stringify({
        request_id: input.requestId,
        route: input.route,
        errors: errors.map((item) => ({
          class: item instanceof Error ? item.name : typeof item,
        })),
      })
    );
  } catch {
    // Accounting and its warning are both best-effort after completion. Neither
    // may replace an upstream response that is already ready for the client.
  }
};

/**
 * Runs one best-effort Sentinel replay write.  A synchronous throw from the
 * persistence call and a rejected write both settle as success, so capture
 * persistence can never replace a response that is already ready for the
 * client.
 */
const persistSentinelReplayBestEffort = async (
  persist: typeof persistSentinelReplayFromEnvironment,
  input: AcceptedSentinelReplayInput,
  observation: SentinelFailureObservation,
  clientObservation: ReturnType<typeof resolveSentinelClientFailureObservation>
): Promise<void> => {
  try {
    await persist(input, observation, clientObservation);
  } catch {
    // Capture persistence is best effort and must not replace the response.
  }
};

export const withTerminalRequestLog = (
  response: Response,
  input: Readonly<{
    route: string;
    telemetryResponse?: Response;
    startedAtMonotonicMs: number;
    requestId: string;
    onTerminal?: (outcome: "completed" | "incomplete", reason?: string) => Promise<void>;
    /**
     * Runs exactly once, when the wrapped response's body and delivery have
     * settled (or the application terminal has been observed for a buffered
     * response). It owns the process-resource permit release, so it must never
     * await accounting or replace the response.
     */
    onSettled?: () => void;
    deliveryCompleted?: Promise<void>;
    deliverySignal?: AbortSignal;
    /** Time this request spent waiting for a process-resource permit, if it queued. */
    admissionWaitMs?: number | null;
    /** Test seam for proving aggregate cache analytics remains best effort. */
    recordCacheAnalytics?: typeof recordPromptCacheAnalytics;
    /** Test seam for proving terminal telemetry remains best effort. */
    recordTelemetry?: typeof recordPromptCacheTelemetry;
    /** Test seam for proving failed replay persistence and successful-request exclusion. */
    sentinelReplayInput?: AcceptedSentinelReplayInput | null;
    persistSentinelReplay?: typeof persistSentinelReplayFromEnvironment;
    /** Why this request carries no captured body; published for persistable failures only. */
    sentinelReplayOmission?: SentinelReplayCaptureOmissionReason | null;
    /** Test seam for proving a body-less failure still publishes its capture status. */
    recordSentinelReplayOmission?: typeof recordSentinelReplayOmissionFromEnvironment;
    recordSentinelDegradation?: typeof recordSentinelProviderDegradationFromEnvironment;
    recordAdminError?: typeof recordAdminError;
    waitUntil?: SentinelBackgroundTaskRegistrar;
  }>
): Promise<Response> => {
  const contentType = response.headers.get("Content-Type")?.toLowerCase() ?? "";
  const isSse = contentType.includes("text/event-stream");
  const initialTelemetry = getResponseTelemetry(input.telemetryResponse ?? response);
  let clientBodyObservation: SentinelClientBodyObservation | null = null;
  let bufferedObservation: Promise<SentinelClientBodyObservation | null> | null = null;
  if (
    !isSse &&
    response.body &&
    (response.status >= 400 ||
      response.status === 202 ||
      input.route.startsWith("embeddings.jobs.") ||
      (initialTelemetry?.completed === false &&
        (initialTelemetry.streamTerminalType !== null || initialTelemetry.failureKind !== null || initialTelemetry.syntheticTerminalType !== null)))
  ) {
    bufferedObservation = inspectSentinelBufferedResponseBody(response);
  }
  let replayFinalization: Promise<void> | null = null;
  /** The same internal failure observation feeds capture persistence and omission status. */
  const replayObservationFor = (streamReadFailure: boolean): SentinelFailureObservation => {
    const telemetry = getResponseTelemetry(input.telemetryResponse ?? response);
    return {
      status: response.status,
      stream: streamReadFailure ? (telemetry?.stream ?? true) : (telemetry?.stream ?? null),
      completed: streamReadFailure ? false : (telemetry?.completed ?? false),
      terminal_type: streamReadFailure ? (telemetry?.streamTerminalType ?? "error") : (telemetry?.streamTerminalType ?? null),
      failure_kind: streamReadFailure ? (telemetry?.failureKind ?? "gateway_stream_read_error") : (telemetry?.failureKind ?? null),
      synthetic_terminal_type: telemetry?.syntheticTerminalType ?? null,
      provider_route: telemetry?.provider ?? response.headers.get("x-uos-upstream") ?? "gateway",
    };
  };
  const persistReplayAtApplicationTerminal = (streamReadFailure = false): Promise<void> => {
    if (replayFinalization) return replayFinalization;
    const originalReplayInput = input.sentinelReplayInput;
    // One snapshot copies the body and seals the request-owned upstream
    // recorder together, before the original is zeroed or any await. The
    // recorder is disposed inside the snapshot: zeroing the original can
    // never erase captured upstream evidence, and HMAC/encryption use the
    // same immutable trace.
    const backgroundReplayInput = originalReplayInput ? snapshotSentinelReplayInput(originalReplayInput) : null;
    // The background task owns the independent snapshot. Release the
    // request-owned bytes before any inspection or persistence await so a
    // stalled clone, crypto operation, or KV write cannot retain both copies.
    zeroSentinelReplayInput(originalReplayInput);
    replayFinalization = (async () => {
      const observation = replayObservationFor(streamReadFailure);
      if (!backgroundReplayInput) {
        // Nothing was carried into a capture. A persistable failure still
        // publishes its explicit omission status so a missing key, an omitted
        // body or a rejected request is never an empty replay history.
        if (input.sentinelReplayOmission) {
          const bodyObservation = clientBodyObservation ?? (await bufferedObservation);
          const clientObservation = resolveSentinelClientFailureObservation(observation, bodyObservation);
          if (shouldPersistSentinelReplay(observation, clientObservation)) {
            await (input.recordSentinelReplayOmission ?? recordSentinelReplayOmissionFromEnvironment)(
              input.requestId,
              input.sentinelReplayOmission,
              Date.now()
            );
          }
        }
        return;
      }
      const startReplayPersistence = (clientObservation: ReturnType<typeof resolveSentinelClientFailureObservation>): Promise<void> => {
        if (!shouldPersistSentinelReplay(observation, clientObservation)) return Promise.resolve();
        return persistSentinelReplayBestEffort(
          input.persistSentinelReplay ?? persistSentinelReplayFromEnvironment,
          backgroundReplayInput,
          observation,
          clientObservation
        );
      };
      // Never persist from a status-only fallback when an observed downstream
      // body is available: the client-visible error code/param and the bounded
      // terminal body are required replay evidence. The bounded clone was
      // started as soon as the response was known, so waiting for it keeps the
      // real HTTP failure capture as rich as the embedded-call one.
      const bodyObservation = clientBodyObservation ?? (await bufferedObservation);
      const clientObservation = resolveSentinelClientFailureObservation(observation, bodyObservation);
      await startReplayPersistence(clientObservation);
    })()
      .catch(() => {
        // Capture persistence is best effort and must not replace the response.
      })
      .finally(() => {
        zeroSentinelReplayInput(backgroundReplayInput);
        zeroSentinelReplayInput(originalReplayInput);
      });
    return replayFinalization;
  };
  let terminalLog: Promise<void> | null = null;
  let terminalFinalization: Promise<void> | null = null;
  let terminalIntent: Readonly<{ outcome: "completed" | "incomplete"; reason?: string }> | null = null;
  let terminalSettled = false;
  const log = (
    downstreamDrainedAtMonotonicMs?: number,
    deliveryOutcome: DeliveryOutcome = "unobserved",
    streamReadFailure = false,
    suppressSentinelReplay = false
  ): Promise<void> => {
    if (terminalLog) return terminalLog;
    // The response body and its delivery are settled here, so the process
    // resources behind this request are released before the durable terminal
    // writes start. Accounting has already been scheduled by the wrapper and is
    // never cancelled, refunded or duplicated by this release.
    try {
      input.onSettled?.();
    } catch {
      // A release fault cannot replace a response that is already delivered.
    }
    terminalLog = logTerminalRequest({
      ...input,
      // Replay persistence owns cleanup after it has taken its snapshot. Do
      // not let terminal logging clear the original body first.
      sentinelReplayInput: replayFinalization === null ? input.sentinelReplayInput : null,
      response,
      downstreamDrainedAtMonotonicMs,
      deliveryOutcome,
      streamReadFailure,
      suppressSentinelReplay: suppressSentinelReplay || replayFinalization !== null,
      resolveClientBodyObservation: async () => clientBodyObservation ?? (await bufferedObservation),
    }).catch(() => {
      // Terminal logging and its durable baseline counters are best effort;
      // neither may replace a response that is already ready for the client.
    });
    return terminalLog;
  };
  const deliveryOutcome = input.deliveryCompleted
    ? input.deliveryCompleted.then(
        () => (input.deliverySignal?.aborted ? ("interrupted" as const) : ("delivered" as const)),
        () => "interrupted" as const
      )
    : null;
  const finalizeTerminal = (outcome: "completed" | "incomplete", reason?: string): Promise<void> => {
    const onTerminal = input.onTerminal;
    if (!onTerminal) return Promise.resolve();
    if (outcome === "completed" && !terminalSettled) terminalIntent = { outcome, reason };
    else terminalIntent ??= { outcome, reason };
    if (terminalSettled) return Promise.resolve();
    if (terminalFinalization) {
      const pending = terminalFinalization;
      return pending.then(() => (terminalSettled ? undefined : finalizeTerminal(outcome, reason)));
    }
    const intended = terminalIntent;
    const current = (async () => {
      let lastError: unknown = null;
      for (let attempt = 0; attempt < 3; attempt += 1) {
        try {
          await onTerminal(intended.outcome, intended.reason);
          terminalSettled = true;
          return;
        } catch (error) {
          lastError = error;
        }
      }
      warnQuotaAccountingFailure(input, lastError);
    })().finally(() => {
      if (terminalFinalization === current) terminalFinalization = null;
    });
    terminalFinalization = current;
    return current;
  };
  const finalizeObservedCompletion = (): Promise<void> => {
    if (!response.ok) return Promise.resolve();
    const telemetry = getResponseTelemetry(input.telemetryResponse ?? response);
    if (!telemetry?.completed) return Promise.resolve();
    return finalizeTerminal("completed");
  };
  const finalizeFromTelemetry = (reason: string): Promise<void> => {
    const telemetry = getResponseTelemetry(input.telemetryResponse ?? response);
    return response.ok && telemetry?.completed ? finalizeTerminal("completed") : finalizeTerminal("incomplete", reason);
  };
  if (!response.body || !isSse) {
    return (async () => {
      try {
        await finalizeFromTelemetry(response.ok ? "response_incomplete" : "response_error");
        // Deno can return the already-computed response while this best-effort
        // capture continues in the background. In particular, buffered replay
        // inspection, compression, encryption, and KV writes must not extend
        // client-visible gateway error latency.
        const replayTask = persistReplayAtApplicationTerminal();
        if (!scheduleSentinelBackgroundTask(replayTask, input.waitUntil)) await replayTask;
        return response;
      } finally {
        if (deliveryOutcome) {
          void deliveryOutcome.then((outcome) => log(undefined, outcome, false, true));
        } else {
          await log(undefined, "unobserved", false, true);
        }
      }
    })();
  }

  const reader = response.body.getReader();
  const sseInspector = createSentinelSseInspector();
  let sseInspectionFailed = false;
  const inspectSseChunk = (value: Uint8Array): void => {
    if (sseInspectionFailed) return;
    try {
      sseInspector.push(value);
    } catch {
      sseInspectionFailed = true;
    }
  };
  const finishSseInspection = (termination: "eof" | "read_error" = "eof"): SentinelClientBodyObservation | null => {
    if (sseInspectionFailed) return null;
    try {
      return sseInspector.finish(termination);
    } catch {
      return null;
    }
  };
  let downstreamDrainedAtMonotonicMs: number | undefined;
  let settleBody: ((outcome: BodyOutcome) => void) | null = null;
  let bodyDidSettle = false;
  let downstreamCancelled = false;
  const bodyOutcome = deliveryOutcome
    ? new Promise<BodyOutcome>((resolve) => {
        settleBody = (outcome) => {
          if (bodyDidSettle) return;
          bodyDidSettle = true;
          resolve(outcome);
        };
      })
    : null;
  if (bodyOutcome && deliveryOutcome) {
    void Promise.all([bodyOutcome, deliveryOutcome]).then(([bodyResult, deliveryResult]) =>
      log(downstreamDrainedAtMonotonicMs, bodyResult === "drained" ? deliveryResult : "interrupted", bodyResult === "failed", bodyResult === "interrupted")
    );
  }
  /** Releases the request-owned replay capture before terminal cleanup runs. */
  const releaseReplayOwnership = (): void => {
    zeroSentinelReplayInput(input.sentinelReplayInput);
    disposeSentinelUpstreamRecorder(input.sentinelReplayInput);
  };
  /** Records an interrupted wrapper body and settles the body-outcome observer. */
  const settleInterruptedBody = async (): Promise<void> => {
    if (!deliveryOutcome) await log(undefined, "interrupted", false, true);
    settleBody?.("interrupted");
  };
  /** Cleans up after a failed provider read, then surfaces that error downstream. */
  const handleProviderReadFailure = async (error: unknown, controller: ReadableStreamDefaultController<Uint8Array>): Promise<void> => {
    clientBodyObservation = finishSseInspection("read_error");
    const downstreamAborted = downstreamCancelled || input.deliverySignal?.aborted === true;
    await finalizeFromTelemetry(downstreamAborted ? "downstream_cancelled" : "stream_read_error");
    if (downstreamAborted) {
      releaseReplayOwnership();
    } else {
      await persistReplayAtApplicationTerminal(true);
    }
    if (!deliveryOutcome) await log(undefined, "interrupted", !downstreamAborted, true);
    try {
      controller.error(error);
    } catch {
      // The downstream may have cancelled while the provider read failed.
    }
    settleBody?.(downstreamAborted ? "interrupted" : "failed");
  };
  /** Finishes the wrapper body once the provider stream reports EOF. */
  const finishProviderStream = async (controller: ReadableStreamDefaultController<Uint8Array>): Promise<void> => {
    clientBodyObservation = finishSseInspection();
    const downstreamAborted = downstreamCancelled || input.deliverySignal?.aborted === true;
    if (downstreamAborted) {
      await finalizeFromTelemetry("downstream_cancelled");
      releaseReplayOwnership();
      try {
        controller.close();
      } catch {
        // The downstream cancellation may already have closed the wrapper.
      }
      await settleInterruptedBody();
      return;
    }
    // Snapshot the downstream drain before finalization. Accounting can
    // wait on KV and belongs in total latency, not drain telemetry.
    downstreamDrainedAtMonotonicMs = performance.now();
    // The application stream has drained, but Deno still owns delivery.
    // Finish one-shot usage accounting before closing this wrapper, then
    // let `completed` classify the separate delivery outcome.
    await finalizeFromTelemetry("stream_eof_without_completion");
    await persistReplayAtApplicationTerminal();
    try {
      controller.close();
    } catch {
      await settleInterruptedBody();
      return;
    }
    if (!deliveryOutcome) await log(downstreamDrainedAtMonotonicMs, "unobserved", false, true);
    settleBody?.("drained");
  };
  const body = new ReadableStream<Uint8Array>({
    async pull(controller) {
      let result: ReadableStreamReadResult<Uint8Array>;
      try {
        result = await reader.read();
      } catch (error) {
        await handleProviderReadFailure(error, controller);
        return;
      }
      if (result.done) {
        await finishProviderStream(controller);
        return;
      }
      // The OpenAI stream observer marks response.completed before yielding
      // the chunk that contains it. Schedule accounting, but never hold back
      // the provider bytes that are already ready for the client.
      void finalizeObservedCompletion();
      inspectSseChunk(result.value);
      try {
        controller.enqueue(result.value);
      } catch {
        downstreamCancelled = true;
        void reader.cancel().catch(() => {});
        void finalizeFromTelemetry("downstream_enqueue_failed");
        releaseReplayOwnership();
        await settleInterruptedBody();
      }
    },
    cancel(reason) {
      // Cancellation must not await a concurrently pending provider pull;
      // that pull observes the cancellation and performs layered cleanup.
      downstreamCancelled = true;
      void reader.cancel(reason).catch(() => {});
      void finalizeFromTelemetry("downstream_cancelled");
      releaseReplayOwnership();
      if (!deliveryOutcome) void log(undefined, "interrupted", false, true);
      settleBody?.("interrupted");
    },
  });
  return Promise.resolve(
    new Response(body, {
      status: response.status,
      statusText: response.statusText,
      headers: response.headers,
    })
  );
};

const withRejectionTerminalLog = (
  response: Response,
  route: string | null,
  input: Readonly<{
    requestId: string;
    startedAtMonotonicMs: number;
    delivery?: RequestDeliveryInfo;
    onSettled?: () => void;
    admissionWaitMs?: number | null;
  }>
): Promise<Response> => {
  if (!route) {
    input.onSettled?.();
    return Promise.resolve(response);
  }
  return withTerminalRequestLog(response, {
    route,
    startedAtMonotonicMs: input.startedAtMonotonicMs,
    requestId: input.requestId,
    deliveryCompleted: input.delivery?.completed,
    deliverySignal: input.delivery?.downstreamSignal,
    onSettled: input.onSettled,
    admissionWaitMs: input.admissionWaitMs,
  });
};

/** The API-key id that usage telemetry attributes this request to, if any. */

export { withRejectionTerminalLog };
