// Embeddings HTTP handlers and job runner, extracted from src/openai.ts.

import { ApiKeyQuotaDispatchError } from "../api-key-policy.ts";
import { json, openaiError } from "../http.ts";
import { getKv } from "../kv.ts";
import { readJsonBody } from "../request.ts";
import { isRecord, sha256Hex } from "../utils.ts";
import { downstreamSignalFor } from "../openai.ts";
import { apiKeyQuotaDispatchErrorResponse } from "../upstream-wire.ts";
import {
  UsageContext,
  UsageTokens,
  recordCompletionUsage,
  recordErrorUsage,
  recordRequestUsage,
  recordStreamTerminalType,
  runWithResponseTelemetry,
} from "../openai-telemetry.ts";
import { formatErrorSnippet, logRedactedUpstreamError } from "../upstream-wire.ts";
import {
  EMBEDDINGS_IDEMPOTENCY_MAX_KEY_CHARS,
  EMBEDDINGS_MAX_INPUTS_PER_REQUEST,
  EMBEDDINGS_RETRYABLE_UPSTREAM_STATUSES,
  EMBEDDINGS_TIMEOUT_MS,
  EmbeddingsIdempotencyLease,
  VOYAGE_RATE_LIMIT_TPM,
  acquireEmbeddingsIdempotencyLease,
  buildEmbeddingsIdempotencyFingerprint,
  embeddingsCacheKey,
  embeddingsIdempotencyIndeterminateResponse,
  embeddingsIdempotencyUnavailableResponse,
  hasAsciiControlCharacter,
  markEmbeddingsIdempotencyDispatched,
  markEmbeddingsIdempotencyIndeterminate,
  releaseEmbeddingsIdempotencyReservation,
  storeEmbeddingsIdempotencySuccess,
} from "./ledger.ts";
import {
  applyVoyageRateLimit,
  chunkByTokenBudget,
  estimateTokenCount,
  fetchVoyageEmbeddings,
  floatEmbeddingToBase64,
  isValidEmbeddingVector,
  parseUosEmbeddingsRequest,
  readVoyageApiKey,
  sleepUnlessAborted,
  writeEmbeddingsCacheEntryBestEffort,
} from "./voyage.ts";

export const withVoyageUpstreamHeader = (response: Response): Response => {
  const headers = new Headers(response.headers);
  headers.set("x-uos-upstream", "voyage");
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
};

const handleEmbeddingsRequest = async (req: Request, usageContext?: UsageContext, options: Readonly<{ kv?: Deno.Kv | null }> = {}): Promise<Response> => {
  const requestId = crypto.randomUUID();
  const startedAtMs = Date.now();
  const downstreamSignal = downstreamSignalFor(req, usageContext);

  const rawBody = (await readJsonBody(req)) as Record<string, unknown> | null;
  if (!rawBody || !isRecord(rawBody)) {
    return openaiError(400, "Invalid JSON body", "invalid_request_error");
  }

  const parsed = parseUosEmbeddingsRequest(rawBody);
  if (!parsed.ok) return parsed.response;
  const { model, inputs, profile } = parsed.value;

  const kv = Object.prototype.hasOwnProperty.call(options, "kv") ? (options.kv ?? null) : await getKv();
  const hashes = await Promise.all(inputs.map((text) => sha256Hex(text)));
  let idempotencyLease: EmbeddingsIdempotencyLease | null = null;
  let idempotencyDispatched = false;
  let idempotencyHasConfirmedSuccess = false;

  const acquireIdempotencyLease = async (): Promise<Response | null> => {
    const idempotencyKey = req.headers.get("Idempotency-Key");
    if (idempotencyKey === null) return null;
    if (!idempotencyKey || idempotencyKey.length > EMBEDDINGS_IDEMPOTENCY_MAX_KEY_CHARS || hasAsciiControlCharacter(idempotencyKey)) {
      return openaiError(400, `Idempotency-Key must contain 1-${EMBEDDINGS_IDEMPOTENCY_MAX_KEY_CHARS} non-control characters.`, "invalid_request_error", {
        param: null,
      });
    }
    const principal = usageContext?.idempotencyPrincipal?.trim() ?? "";
    if (!kv || !principal) return embeddingsIdempotencyUnavailableResponse();
    const fingerprint = await buildEmbeddingsIdempotencyFingerprint(profile, hashes);
    const acquired = await acquireEmbeddingsIdempotencyLease({
      kv,
      principal,
      idempotencyKey,
      fingerprint,
      requestId,
    });
    if (acquired.kind === "replay") return acquired.response;
    if (acquired.kind === "error") return acquired.response;
    idempotencyLease = acquired.lease;
    return null;
  };

  const leaseFailure = await acquireIdempotencyLease();
  if (leaseFailure) return leaseFailure;

  const releaseBeforeDispatch = async (response: Response): Promise<Response> => {
    if (!idempotencyLease) return response;
    const released = await releaseEmbeddingsIdempotencyReservation(idempotencyLease, false);
    return released ? response : embeddingsIdempotencyUnavailableResponse();
  };
  const releaseAfterExplicitUpstreamFailure = async (response: Response): Promise<Response> => {
    if (!idempotencyLease) return response;
    if (idempotencyHasConfirmedSuccess) return await failIndeterminate();
    const released = await releaseEmbeddingsIdempotencyReservation(idempotencyLease, true);
    if (released) return response;
    await markEmbeddingsIdempotencyIndeterminate(idempotencyLease);
    return embeddingsIdempotencyIndeterminateResponse();
  };
  const failIndeterminate = async (): Promise<Response> => {
    if (idempotencyLease) await markEmbeddingsIdempotencyIndeterminate(idempotencyLease);
    return embeddingsIdempotencyIndeterminateResponse();
  };
  const cancelledResponse = async (): Promise<Response> => {
    recordStreamTerminalType(usageContext, "cancelled");
    await recordErrorUsage(usageContext);
    if (idempotencyLease && idempotencyDispatched) return await failIndeterminate();
    return await releaseBeforeDispatch(openaiError(499, "Request was cancelled.", "request_cancelled", { type: "server_error", param: null }));
  };

  await recordRequestUsage(usageContext, { model, route: "embeddings", stream: false, reasoning: null });

  const deadlineMs = startedAtMs + EMBEDDINGS_TIMEOUT_MS;
  const apiKey = await readVoyageApiKey(kv);
  if (!apiKey) {
    await recordErrorUsage(usageContext);
    return await releaseBeforeDispatch(
      openaiError(503, "Embeddings provider is not configured: set VOYAGEAI_API_KEY (or store it in Deno KV)", "server_error", {
        type: "server_error",
        param: null,
      })
    );
  }
  const shouldCache = Boolean(kv);

  // Dedupe within a request (hash collisions are astronomically unlikely).
  const buckets = new Map<string, { text: string; indices: number[] }>();
  for (let i = 0; i < inputs.length; i += 1) {
    const hash = hashes[i];
    const existing = buckets.get(hash);
    if (existing) {
      existing.indices.push(i);
    } else {
      buckets.set(hash, { text: inputs[i], indices: [i] });
    }
  }

  const cacheProfileKey = profile.cache_profile_key;
  const cacheKeyFor = (hash: string): Deno.KvKey => embeddingsCacheKey(cacheProfileKey, hash);
  const vectorsByIndex: (number[] | null)[] = Array.from({ length: inputs.length }, () => null);

  let voyageTotalTokens = 0;
  let sawVoyageTokenUsage = false;

  const missing: { hash: string; text: string; indices: number[] }[] = [];
  const loadCachedVectorsAndMissing = async (): Promise<void> => {
    if (!shouldCache || !kv) {
      for (const [hash, bucket] of buckets.entries()) {
        missing.push({ hash, text: bucket.text, indices: bucket.indices });
      }
      return;
    }
    const unique = Array.from(buckets.entries()).map(([hash, bucket]) => ({ hash, ...bucket }));
    const entries = await Promise.all(unique.map((item) => kv.get<{ embedding?: unknown }>(cacheKeyFor(item.hash)).catch(() => null)));
    for (let i = 0; i < unique.length; i += 1) {
      const item = unique[i];
      const entry = entries[i];
      const cached = entry?.value?.embedding;
      if (isValidEmbeddingVector(cached, profile.dimensions)) {
        for (const idx of item.indices) vectorsByIndex[idx] = cached;
      } else {
        missing.push(item);
      }
    }
  };
  const releasedFailureResponse = async (response: Response): Promise<Response> => {
    await recordErrorUsage(usageContext);
    if (idempotencyLease && idempotencyDispatched) return await failIndeterminate();
    return await releaseBeforeDispatch(response);
  };
  const unreleasedFailureResponse = async (response: Response): Promise<Response> => {
    await recordErrorUsage(usageContext);
    if (idempotencyLease && idempotencyDispatched) return await failIndeterminate();
    return response;
  };
  const timeoutResponse = async (): Promise<Response> =>
    await releasedFailureResponse(openaiError(502, "Embeddings request timed out.", "timeout", { type: "server_error", param: null }));
  const rateLimitResponse = async (waitMs: number): Promise<Response> => {
    const retryAfterSeconds = Math.max(1, Math.ceil(waitMs / 1000));
    const body = {
      error: {
        message: `Rate limit exceeded; retry after ~${retryAfterSeconds}s`,
        type: "rate_limit_error",
        code: "rate_limit_exceeded",
        param: null,
      },
    };
    return await releasedFailureResponse(json(429, body, { "Retry-After": String(retryAfterSeconds) }));
  };
  const sizeMismatchResponse = async (): Promise<Response> =>
    await unreleasedFailureResponse(
      openaiError(502, "Embeddings upstream returned a size mismatch.", "upstream_error", {
        type: "server_error",
        param: null,
      })
    );
  const dimensionMismatchResponse = async (actualLength: number): Promise<Response> =>
    await unreleasedFailureResponse(
      openaiError(502, `Embeddings upstream returned vector length ${actualLength}; expected ${profile.dimensions}.`, "upstream_dimension_mismatch", {
        type: "server_error",
        param: null,
      })
    );
  const incompleteResponseFailure = async (): Promise<Response> =>
    await unreleasedFailureResponse(
      openaiError(502, "Embeddings gateway failed to construct a complete response.", "server_error", {
        type: "server_error",
        param: null,
      })
    );
  const ensureIdempotencyDispatched = async (): Promise<Response | null> => {
    if (!idempotencyLease || idempotencyDispatched) return null;
    const markedDispatched = await markEmbeddingsIdempotencyDispatched(idempotencyLease);
    if (!markedDispatched) {
      await recordErrorUsage(usageContext);
      return embeddingsIdempotencyUnavailableResponse();
    }
    idempotencyDispatched = true;
    return null;
  };
  const handleQuotaDispatchFailure = async (error: ApiKeyQuotaDispatchError): Promise<Response> => {
    await recordErrorUsage(usageContext);
    if (idempotencyLease) {
      const released = await releaseEmbeddingsIdempotencyReservation(idempotencyLease, idempotencyDispatched);
      if (!released) return embeddingsIdempotencyUnavailableResponse();
      idempotencyDispatched = false;
    }
    return apiKeyQuotaDispatchErrorResponse(error);
  };
  const upstreamFailureResponse = async (status: number, message: string, waitMs: number): Promise<Response> => {
    if (status === 429) {
      const retryAfterSeconds = Math.max(1, Math.ceil(waitMs / 1000));
      const body = {
        error: {
          message,
          type: "rate_limit_error",
          code: "rate_limit_exceeded",
          param: null,
        },
      };
      return await releaseAfterExplicitUpstreamFailure(json(429, body, { "Retry-After": String(retryAfterSeconds) }));
    }
    return await releaseAfterExplicitUpstreamFailure(openaiError(502, message, "upstream_error", { type: "server_error", param: null }));
  };
  const resolveUpstreamFailure = async (error: unknown, attempt: number, backoffMs: number): Promise<Response | null> => {
    if (downstreamSignal.aborted) return await cancelledResponse();
    if (error instanceof ApiKeyQuotaDispatchError) return await handleQuotaDispatchFailure(error);
    const status = (error as { status?: number }).status;
    const retryAfterMs = (error as { retry_after_ms?: number | null }).retry_after_ms ?? null;
    const snippet = formatErrorSnippet(error);
    const message = snippet ? `Embeddings upstream request failed: ${snippet}` : "Embeddings upstream request failed.";

    if (!status || !EMBEDDINGS_RETRYABLE_UPSTREAM_STATUSES.has(status)) {
      logRedactedUpstreamError(`[ai.ubq.fi] embeddings request_id=${requestId} upstream_error:`, error);
      await recordErrorUsage(usageContext);
      if (!status) return await failIndeterminate();
      return await releaseAfterExplicitUpstreamFailure(openaiError(502, message, "upstream_error", { type: "server_error", param: null }));
    }

    const waitMs = Math.max(0, retryAfterMs ?? backoffMs);
    if (attempt >= 2) {
      logRedactedUpstreamError(`[ai.ubq.fi] embeddings request_id=${requestId} upstream_error:`, error);
      await recordErrorUsage(usageContext);
      return await upstreamFailureResponse(status, message, waitMs);
    }

    const now = Date.now();
    if (now + waitMs >= deadlineMs) {
      await recordErrorUsage(usageContext);
      return await upstreamFailureResponse(status, message, waitMs);
    }

    if (!(await sleepUnlessAborted(waitMs, downstreamSignal))) return await cancelledResponse();
    return null;
  };
  const recordChunkUpstreamSuccess = (totalTokens: number | null): void => {
    if (idempotencyLease) idempotencyHasConfirmedSuccess = true;
    if (typeof totalTokens === "number") {
      sawVoyageTokenUsage = true;
      voyageTotalTokens += totalTokens;
    }
  };
  const fetchChunkVectors = async (
    texts: string[]
  ): Promise<{ kind: "ok"; vectors: number[][]; totalTokens: number | null } | { kind: "response"; response: Response }> => {
    let attempt = 0;
    let backoffMs = 250;
    for (;;) {
      if (downstreamSignal.aborted) return { kind: "response", response: await cancelledResponse() };
      const dispatchFailure = await ensureIdempotencyDispatched();
      if (dispatchFailure) return { kind: "response", response: dispatchFailure };
      try {
        const upstream = await fetchVoyageEmbeddings({
          apiKey,
          model: profile.upstream_model,
          inputs: texts,
          inputType: profile.input_type,
          dimensions: profile.dimensions,
          outputDtype: profile.output_dtype,
          truncation: profile.truncation,
          deadlineMs,
          downstreamSignal,
          beforeProviderDispatch: usageContext?.beforeProviderDispatch,
        });
        recordChunkUpstreamSuccess(upstream.totalTokens);
        return { kind: "ok", vectors: upstream.vectors, totalTokens: upstream.totalTokens };
      } catch (error) {
        const failure = await resolveUpstreamFailure(error, attempt, backoffMs);
        if (failure) return { kind: "response", response: failure };
        backoffMs = Math.min(2000, backoffMs * 2);
        attempt += 1;
      }
    }
  };
  const writeChunkCacheEntries = async (chunkItems: { hash: string; text: string; indices: number[] }[], chunkVectors: number[][]): Promise<void> => {
    for (let i = 0; i < chunkItems.length; i += 1) {
      const item = chunkItems[i];
      const vec = chunkVectors[i];
      for (const idx of item.indices) vectorsByIndex[idx] = vec;
      if (shouldCache && kv) {
        await writeEmbeddingsCacheEntryBestEffort(kv, cacheProfileKey, item.hash, vec, Date.now(), deadlineMs);
      }
    }
  };
  const processChunk = async (chunkItems: { hash: string; text: string; indices: number[] }[]): Promise<Response | null> => {
    const texts = chunkItems.map((item) => item.text);
    const tokenEstimate = estimateTokenCount(texts);

    if (kv) {
      const reserved = await applyVoyageRateLimit(kv, tokenEstimate, deadlineMs);
      if (!reserved.ok) return await rateLimitResponse(reserved.wait_ms);
    }

    const fetched = await fetchChunkVectors(texts);
    if (fetched.kind === "response") return fetched.response;
    const vectors = fetched.vectors;

    if (vectors.length !== chunkItems.length) return await sizeMismatchResponse();

    const wrongLengthIndex = vectors.findIndex((vector) => vector.length !== profile.dimensions);
    if (wrongLengthIndex >= 0) return await dimensionMismatchResponse(vectors[wrongLengthIndex]?.length ?? 0);

    await writeChunkCacheEntries(chunkItems, vectors);
    return null;
  };
  const fillMissingVectors = async (): Promise<Response | null> => {
    const chunks = chunkByTokenBudget(
      missing.map((item) => ({ hash: item.hash, text: item.text })),
      EMBEDDINGS_MAX_INPUTS_PER_REQUEST,
      VOYAGE_RATE_LIMIT_TPM
    );

    let offset = 0;
    for (const chunk of chunks) {
      const now = Date.now();
      if (now >= deadlineMs) return await timeoutResponse();
      const chunkItems = missing.slice(offset, offset + chunk.length);
      offset += chunk.length;
      const failure = await processChunk(chunkItems);
      if (failure) return failure;
    }
    return null;
  };
  const storeSuccessOrFailure = async (response: Response): Promise<Response | null> => {
    if (!idempotencyLease) return null;
    const stored = await storeEmbeddingsIdempotencySuccess(idempotencyLease, response);
    if (stored) return null;
    if (idempotencyDispatched) return await failIndeterminate();
    return await releaseBeforeDispatch(embeddingsIdempotencyUnavailableResponse());
  };
  const completeEmbeddingsResponse = async (): Promise<Response> => {
    const data: { object: "embedding"; index: number; embedding: number[] | string }[] = [];
    for (let i = 0; i < vectorsByIndex.length; i += 1) {
      const vec = vectorsByIndex[i];
      if (!vec) return await incompleteResponseFailure();
      data.push({
        object: "embedding",
        index: i,
        embedding: profile.encoding_format === "base64" ? floatEmbeddingToBase64(vec) : vec,
      });
    }

    const usageTokens: UsageTokens | null = sawVoyageTokenUsage
      ? {
          inputTokens: voyageTotalTokens,
          cachedInputTokens: null,
          cacheWriteInputTokens: null,
          outputTokens: 0,
          totalTokens: voyageTotalTokens,
          status: "reported",
        }
      : null;
    const response = json(200, {
      object: "list",
      data,
      model,
      usage: {
        prompt_tokens: usageTokens?.inputTokens ?? 0,
        total_tokens: usageTokens?.totalTokens ?? 0,
      },
    });
    const storeFailure = await storeSuccessOrFailure(response);
    if (storeFailure) return storeFailure;
    await recordCompletionUsage(usageContext, usageTokens);
    return response;
  };

  await loadCachedVectorsAndMissing();

  if (missing.length > 0) {
    const failure = await fillMissingVectors();
    if (failure) return failure;
  }

  return await completeEmbeddingsResponse();
};

export const handleUosEmbeddings = async (req: Request, usageContext?: UsageContext, options: Readonly<{ kv?: Deno.Kv | null }> = {}): Promise<Response> =>
  await runWithResponseTelemetry(usageContext, async (context) => withVoyageUpstreamHeader(await handleEmbeddingsRequest(req, context, options)));
