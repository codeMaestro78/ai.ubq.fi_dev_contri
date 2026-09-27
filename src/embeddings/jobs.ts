// Embeddings job API and runner, extracted from src/openai.ts.

// Embeddings HTTP handlers and job runner, extracted from src/openai.ts.

import { ApiKeyQuotaDispatchError } from "../api-key-policy.ts";
import { json, openaiError } from "../http.ts";
import { getKv } from "../kv.ts";
import { readJsonBody } from "../request.ts";
import { getString, isRecord, sha256Hex } from "../utils.ts";
import { UsageContext, UsageTokens, recordCompletionUsage, recordErrorUsage, recordRequestUsage, runWithResponseTelemetry } from "../openai-telemetry.ts";
import { formatErrorSnippet } from "../upstream-wire.ts";
import {
  EMBEDDINGS_MAX_INPUTS_PER_REQUEST,
  EMBEDDINGS_RETRYABLE_UPSTREAM_STATUSES,
  EMBEDDINGS_TIMEOUT_MS,
  VOYAGE_RATE_LIMIT_TPM,
  embeddingsCacheKey,
} from "./ledger.ts";
import {
  chunkByTokenBudget,
  estimateTokenCount,
  fetchVoyageEmbeddings,
  floatEmbeddingToBase64,
  isValidEmbeddingVector,
  readVoyageApiKey,
  writeEmbeddingsCacheEntryBestEffort,
} from "./voyage.ts";
import {
  EMBEDDINGS_JOB_LOCK_MS,
  EMBEDDINGS_JOB_TTL_MS,
  EmbeddingsEncodingFormat,
  EmbeddingsJobInputRecord,
  EmbeddingsJobLookupRecord,
  EmbeddingsJobRecord,
  VoyageEmbeddingsDimension,
  embeddingsJobInputKey,
  embeddingsJobKey,
  embeddingsJobLookupKey,
} from "./ledger.ts";
import {
  decryptEmbeddingsJobInput,
  encryptEmbeddingsJobInput,
  normalizeEmbeddingsJobInputRecord,
  parseEmbeddingsJobRequest,
  resolveEmbeddingsJobTokenSeed,
  sleep,
  tryReserveVoyageBudget,
} from "./voyage.ts";
import { withVoyageUpstreamHeader } from "./handlers.ts";

const buildEmbeddingsJobBody = (job: EmbeddingsJobRecord, result: Record<string, unknown> | null): Record<string, unknown> => ({
  id: job.id,
  object: "embeddings.job",
  status: job.status,
  created_at_ms: job.created_at_ms,
  updated_at_ms: job.updated_at_ms,
  model: job.model,
  upstream: job.upstream,
  upstream_model: job.upstream_model,
  input_type: job.input_type,
  dimensions: job.dimensions,
  output_dtype: job.output_dtype,
  encoding_format: job.encoding_format,
  truncation: job.truncation,
  input_count: job.input_count,
  total_chars: job.total_chars,
  retry_after_seconds: job.retry_after_seconds,
  error: job.error,
  result,
});

const loadEmbeddingsVectorsFromCache = async (
  kv: Deno.Kv,
  cacheProfileKey: string,
  hashesByIndex: string[],
  dimensions: VoyageEmbeddingsDimension
): Promise<(number[] | null)[]> => {
  const uniqueHashes = Array.from(new Set(hashesByIndex));
  const cacheKeyFor = (hash: string): Deno.KvKey => embeddingsCacheKey(cacheProfileKey, hash);
  const entries = await Promise.all(uniqueHashes.map((hash) => kv.get<{ embedding?: unknown }>(cacheKeyFor(hash)).catch(() => null)));
  const vectorsByHash = new Map<string, number[]>();
  for (const [i, hash] of uniqueHashes.entries()) {
    const cached = entries[i]?.value?.embedding;
    if (isValidEmbeddingVector(cached, dimensions)) {
      vectorsByHash.set(hash, cached);
    }
  }
  return hashesByIndex.map((hash) => vectorsByHash.get(hash) ?? null);
};

const buildOpenAiEmbeddingsResult = (
  model: string,
  vectorsByIndex: (number[] | null)[],
  usageTotalTokens: number,
  encodingFormat: EmbeddingsEncodingFormat
): Record<string, unknown> => ({
  object: "list",
  data: vectorsByIndex.map((vec, index) => ({
    object: "embedding",
    index,
    embedding: vec && encodingFormat === "base64" ? floatEmbeddingToBase64(vec) : (vec ?? []),
  })),
  model,
  usage: { prompt_tokens: usageTotalTokens, total_tokens: usageTotalTokens },
});

const reserveVoyageBudgetForJob = async (kv: Deno.Kv, tokens: number): Promise<{ ok: true } | { ok: false; wait_ms: number }> => {
  for (let attempt = 0; attempt < 6; attempt += 1) {
    const reserved = await tryReserveVoyageBudget(kv, tokens);
    if (reserved.ok) return reserved;
    if (reserved.wait_ms > 0) return reserved;
    await sleep(5 + attempt * 5);
  }
  return { ok: false, wait_ms: 1000 };
};

const updateEmbeddingsJobRecord = async (kv: Deno.Kv, jobKey: Deno.KvKey, lookupKey: Deno.KvKey, job: EmbeddingsJobRecord): Promise<void> => {
  await kv
    .atomic()
    .set(jobKey, job, { expireIn: EMBEDDINGS_JOB_TTL_MS })
    .set(lookupKey, { cache_profile_key: job.cache_profile_key } satisfies EmbeddingsJobLookupRecord, { expireIn: EMBEDDINGS_JOB_TTL_MS })
    .commit();
};

const deleteEmbeddingsJobInputs = async (kv: Deno.Kv, tokenHash: string, cacheProfileKey: string, jobId: string, uniqueHashes: string[]): Promise<void> => {
  await Promise.allSettled(uniqueHashes.map((hash) => kv.delete(embeddingsJobInputKey(tokenHash, cacheProfileKey, jobId, hash))));
};

export const runEmbeddingsJobAttempt = async (params: {
  reqId: string;
  kv: Deno.Kv;
  apiKey: string;
  tokenSeed: string;
  tokenHash: string;
  jobKey: Deno.KvKey;
  jobLookupKey: Deno.KvKey;
  jobEntry: Deno.KvEntryMaybe<EmbeddingsJobRecord>;
  job: EmbeddingsJobRecord;
  deadlineMs: number;
  usageContext?: UsageContext;
}): Promise<Response> => {
  const now = Date.now();
  if (params.job.locked_until_ms && params.job.locked_until_ms > now) {
    const retryAfterSeconds = Math.max(1, Math.ceil((params.job.locked_until_ms - now) / 1000));
    const body = buildEmbeddingsJobBody(params.job, null);
    return json(202, body, {
      "Retry-After": String(retryAfterSeconds),
      "x-uos-upstream": params.job.upstream,
    });
  }

  const lockedUntilMs = now + EMBEDDINGS_JOB_LOCK_MS;
  const locked: EmbeddingsJobRecord = {
    ...params.job,
    status: "running",
    locked_until_ms: lockedUntilMs,
    updated_at_ms: now,
    retry_after_seconds: null,
  };

  const lockCommit = await params.kv
    .atomic()
    .check(params.jobEntry)
    .set(params.jobKey, locked, { expireIn: EMBEDDINGS_JOB_TTL_MS })
    .set(params.jobLookupKey, { cache_profile_key: locked.cache_profile_key } satisfies EmbeddingsJobLookupRecord, { expireIn: EMBEDDINGS_JOB_TTL_MS })
    .commit();
  if (!lockCommit.ok) {
    const body = buildEmbeddingsJobBody(params.job, null);
    return json(202, body, { "Retry-After": "1", "x-uos-upstream": params.job.upstream });
  }

  const cacheProfileKey = locked.cache_profile_key;
  const hashesByIndex = locked.input_hashes;
  const uniqueHashes = Array.from(new Set(hashesByIndex));
  const cacheKeyFor = (hash: string): Deno.KvKey => embeddingsCacheKey(cacheProfileKey, hash);

  let currentJob: EmbeddingsJobRecord = locked;
  let queueRetryAfterMs: number | null = null;
  let queueFailureKind: string | null = null;

  const computeMissing = async (): Promise<string[]> => {
    const entries = await Promise.all(uniqueHashes.map((hash) => params.kv.get<{ embedding?: unknown }>(cacheKeyFor(hash)).catch(() => null)));
    const missing: string[] = [];
    for (let i = 0; i < uniqueHashes.length; i += 1) {
      const hash = uniqueHashes[i];
      const cached = entries[i]?.value?.embedding;
      if (isValidEmbeddingVector(cached, locked.dimensions)) continue;
      missing.push(hash);
    }
    return missing;
  };

  const failJob = async (message: string, code: string): Promise<Response> => {
    const failed: EmbeddingsJobRecord = {
      ...currentJob,
      status: "failed",
      updated_at_ms: Date.now(),
      locked_until_ms: null,
      retry_after_seconds: null,
      error: { message, type: "server_error", code },
    };
    currentJob = failed;
    await updateEmbeddingsJobRecord(params.kv, params.jobKey, params.jobLookupKey, failed);
    await deleteEmbeddingsJobInputs(params.kv, params.tokenHash, failed.cache_profile_key, failed.id, uniqueHashes);
    await recordErrorUsage(params.usageContext);
    if (params.usageContext?.responseTelemetry) {
      params.usageContext.responseTelemetry.stream = false;
      params.usageContext.responseTelemetry.completed = false;
      params.usageContext.responseTelemetry.streamTerminalType = "error";
      params.usageContext.responseTelemetry.failureKind = code;
    }
    return json(200, buildEmbeddingsJobBody(failed, null), { "x-uos-upstream": failed.upstream });
  };

  const queueJob = async (waitMs: number, failureKind: string | null = null): Promise<Response> => {
    const retryAfterSeconds = Math.max(1, Math.ceil(waitMs / 1000));
    const queued: EmbeddingsJobRecord = {
      ...currentJob,
      status: "queued",
      updated_at_ms: Date.now(),
      locked_until_ms: null,
      retry_after_seconds: retryAfterSeconds,
      error: null,
    };
    currentJob = queued;
    await updateEmbeddingsJobRecord(params.kv, params.jobKey, params.jobLookupKey, queued);
    if (failureKind && params.usageContext?.responseTelemetry) {
      params.usageContext.responseTelemetry.stream = false;
      params.usageContext.responseTelemetry.completed = false;
      params.usageContext.responseTelemetry.streamTerminalType = "deadline";
      params.usageContext.responseTelemetry.failureKind = failureKind;
    }
    const body = buildEmbeddingsJobBody(queued, null);
    return json(202, body, { "Retry-After": String(retryAfterSeconds), "x-uos-upstream": queued.upstream });
  };

  const succeedJob = async (): Promise<Response> => {
    const vectorsByIndex = await loadEmbeddingsVectorsFromCache(params.kv, cacheProfileKey, hashesByIndex, currentJob.dimensions);
    if (vectorsByIndex.some((vec) => !vec)) {
      return await failJob("Embeddings job completed but cache entries were missing.", "embeddings_job_cache_miss");
    }
    const succeeded: EmbeddingsJobRecord = {
      ...currentJob,
      status: "succeeded",
      updated_at_ms: Date.now(),
      locked_until_ms: null,
      retry_after_seconds: null,
      error: null,
    };
    currentJob = succeeded;
    await updateEmbeddingsJobRecord(params.kv, params.jobKey, params.jobLookupKey, succeeded);
    await deleteEmbeddingsJobInputs(params.kv, params.tokenHash, succeeded.cache_profile_key, succeeded.id, uniqueHashes);
    const result = buildOpenAiEmbeddingsResult(succeeded.model, vectorsByIndex, succeeded.usage_total_tokens, succeeded.encoding_format);
    const usageTokens: UsageTokens | null =
      succeeded.usage_total_tokens > 0
        ? {
            inputTokens: succeeded.usage_total_tokens,
            cachedInputTokens: null,
            cacheWriteInputTokens: null,
            outputTokens: 0,
            totalTokens: succeeded.usage_total_tokens,
            status: "reported",
          }
        : null;
    await recordCompletionUsage(params.usageContext, usageTokens);
    return json(200, buildEmbeddingsJobBody(succeeded, result), { "x-uos-upstream": succeeded.upstream });
  };

  const loadInputItems = async (
    hashes: string[]
  ): Promise<{ kind: "ok"; items: { hash: string; text: string }[] } | { kind: "response"; response: Response }> => {
    const inputEntries = await Promise.all(
      hashes.map((hash) =>
        params.kv.get<EmbeddingsJobInputRecord>(embeddingsJobInputKey(params.tokenHash, locked.cache_profile_key, locked.id, hash)).catch(() => null)
      )
    );
    const items: { hash: string; text: string }[] = [];
    for (let i = 0; i < hashes.length; i += 1) {
      const hash = hashes[i];
      const entry = inputEntries[i];
      const normalized = normalizeEmbeddingsJobInputRecord(entry?.value);
      if (!normalized) {
        return { kind: "response", response: await failJob("Embeddings job input expired or was unavailable.", "embeddings_job_input_missing") };
      }
      const text = await decryptEmbeddingsJobInput(params.tokenSeed, normalized);
      if (text === null) {
        return { kind: "response", response: await failJob("Embeddings job input could not be decrypted.", "embeddings_job_input_decrypt_failed") };
      }
      items.push({ hash, text });
    }
    return { kind: "ok", items };
  };

  const fetchChunkVectors = async (
    texts: string[]
  ): Promise<
    | { kind: "ok"; vectors: number[][]; totalTokens: number | null }
    | { kind: "response"; response: Response }
    | { kind: "queued"; waitMs: number; failureKind: string }
  > => {
    try {
      const upstream = await fetchVoyageEmbeddings({
        apiKey: params.apiKey,
        model: currentJob.upstream_model,
        inputs: texts,
        inputType: currentJob.input_type,
        dimensions: currentJob.dimensions,
        outputDtype: currentJob.output_dtype,
        truncation: currentJob.truncation,
        deadlineMs: params.deadlineMs,
        beforeProviderDispatch: params.usageContext?.beforeProviderDispatch,
      });
      return { kind: "ok", vectors: upstream.vectors, totalTokens: upstream.totalTokens };
    } catch (error) {
      if (error instanceof ApiKeyQuotaDispatchError) {
        return { kind: "response", response: await queueJob(1_000) };
      }
      const status = (error as { status?: number }).status;
      const retryAfterMs = (error as { retry_after_ms?: number | null }).retry_after_ms ?? null;
      if (status && EMBEDDINGS_RETRYABLE_UPSTREAM_STATUSES.has(status)) {
        return {
          kind: "queued",
          waitMs: retryAfterMs ?? (status === 429 ? 60_000 : 1_000),
          failureKind: `embeddings_job_upstream_http_${status}`,
        };
      }
      const snippet = formatErrorSnippet(error);
      const message = snippet ? `Embeddings upstream request failed: ${snippet}` : "Embeddings upstream request failed.";
      return { kind: "response", response: await failJob(message, "embeddings_job_upstream_error") };
    }
  };

  const processChunk = async (
    chunk: { hash: string; text: string }[]
  ): Promise<{ kind: "continue" } | { kind: "stop"; waitMs: number; failureKind: string | null } | { kind: "response"; response: Response }> => {
    if (Date.now() >= params.deadlineMs) {
      return { kind: "stop", waitMs: 1000, failureKind: "embeddings_job_deadline" };
    }

    const texts = chunk.map((item) => item.text);
    const tokenEstimate = estimateTokenCount(texts);

    const reserved = await reserveVoyageBudgetForJob(params.kv, tokenEstimate);
    if (!reserved.ok) {
      return { kind: "stop", waitMs: reserved.wait_ms > 0 ? reserved.wait_ms : 1000, failureKind: null };
    }

    const fetched = await fetchChunkVectors(texts);
    if (fetched.kind === "response") return { kind: "response", response: fetched.response };
    if (fetched.kind === "queued") return { kind: "stop", waitMs: fetched.waitMs, failureKind: fetched.failureKind };

    const vectors = fetched.vectors;
    if (vectors.length !== chunk.length) {
      return { kind: "response", response: await failJob("Embeddings upstream returned a size mismatch.", "embeddings_job_upstream_mismatch") };
    }

    const wrongLengthIndex = vectors.findIndex((vector) => vector.length !== currentJob.dimensions);
    if (wrongLengthIndex >= 0) {
      const actualLength = vectors[wrongLengthIndex]?.length ?? 0;
      return {
        kind: "response",
        response: await failJob(
          `Embeddings upstream returned vector length ${actualLength}; expected ${currentJob.dimensions}.`,
          "embeddings_job_upstream_dimension_mismatch"
        ),
      };
    }

    if (typeof fetched.totalTokens === "number") {
      currentJob = { ...currentJob, usage_total_tokens: currentJob.usage_total_tokens + fetched.totalTokens };
    }

    for (let i = 0; i < chunk.length; i += 1) {
      const item = chunk[i];
      const vec = vectors[i];
      await writeEmbeddingsCacheEntryBestEffort(params.kv, currentJob.cache_profile_key, item.hash, vec, Date.now(), params.deadlineMs);
    }
    return { kind: "continue" };
  };

  const missingBefore = await computeMissing();
  if (missingBefore.length === 0) return await succeedJob();

  const loadedInputs = await loadInputItems(missingBefore);
  if (loadedInputs.kind === "response") return loadedInputs.response;
  const items = loadedInputs.items;

  const chunks = chunkByTokenBudget(items, EMBEDDINGS_MAX_INPUTS_PER_REQUEST, VOYAGE_RATE_LIMIT_TPM);
  for (const chunk of chunks) {
    const outcome = await processChunk(chunk);
    if (outcome.kind === "response") return outcome.response;
    if (outcome.kind === "stop") {
      queueRetryAfterMs = outcome.waitMs;
      queueFailureKind = outcome.failureKind;
      break;
    }
  }

  const missingAfter = await computeMissing();
  if (missingAfter.length === 0) return await succeedJob();

  const waitMs = queueRetryAfterMs ?? 60_000;
  return await queueJob(waitMs, queueFailureKind);
};

const handleEmbeddingsJobCreateInternal = async (req: Request, authToken: string | null, usageContext?: UsageContext): Promise<Response> => {
  const requestId = crypto.randomUUID();
  const startedAtMs = Date.now();

  const rawBody = (await readJsonBody(req)) as Record<string, unknown> | null;
  if (!rawBody || !isRecord(rawBody)) {
    await recordErrorUsage(usageContext);
    return openaiError(400, "Invalid JSON body", "invalid_request_error");
  }

  const parsed = parseEmbeddingsJobRequest(rawBody);
  if (!parsed.ok) {
    await recordErrorUsage(usageContext);
    return parsed.response;
  }
  const { model, inputs, total_chars: totalChars, profile } = parsed.value;

  await recordRequestUsage(usageContext, { model, route: "embeddings.jobs.create", stream: false, reasoning: null });

  const kv = await getKv();
  if (!kv) {
    await recordErrorUsage(usageContext);
    return openaiError(503, "Embeddings jobs require Deno KV", "server_error", { type: "server_error", param: null });
  }

  const apiKey = await readVoyageApiKey(kv);
  if (!apiKey) {
    await recordErrorUsage(usageContext);
    return openaiError(503, "Embeddings provider is not configured: set VOYAGEAI_API_KEY (or store it in Deno KV)", "server_error", {
      type: "server_error",
      param: null,
    });
  }

  const hashesByIndex = await Promise.all(inputs.map((text) => sha256Hex(text)));
  const uniqueTextsByHash = new Map<string, string>();
  for (const [i, text] of inputs.entries()) uniqueTextsByHash.set(hashesByIndex[i], text);
  const uniqueHashes = Array.from(uniqueTextsByHash.keys());

  const jobId = `embjob_${crypto.randomUUID().replace(/-/g, "")}`;
  const tokenSeed = resolveEmbeddingsJobTokenSeed(jobId, authToken, usageContext);
  const tokenHash = await sha256Hex(tokenSeed);
  const now = Date.now();

  // Store encrypted inputs (no raw text) so queued jobs can be processed later without the client resending inputs.
  const inputWrites = uniqueHashes.map(async (hash) => {
    const record = await encryptEmbeddingsJobInput(tokenSeed, uniqueTextsByHash.get(hash) ?? "");
    await kv.set(embeddingsJobInputKey(tokenHash, profile.cache_profile_key, jobId, hash), record, { expireIn: EMBEDDINGS_JOB_TTL_MS });
  });
  await Promise.all(inputWrites);

  const job: EmbeddingsJobRecord = {
    id: jobId,
    status: "queued",
    created_at_ms: now,
    updated_at_ms: now,
    model,
    cache_profile_key: profile.cache_profile_key,
    upstream: profile.upstream,
    upstream_model: profile.upstream_model,
    input_type: profile.input_type,
    dimensions: profile.dimensions,
    output_dtype: profile.output_dtype,
    encoding_format: profile.encoding_format,
    truncation: profile.truncation,
    input_hashes: hashesByIndex,
    input_count: inputs.length,
    total_chars: totalChars,
    usage_total_tokens: 0,
    retry_after_seconds: null,
    locked_until_ms: null,
    error: null,
  };
  const jobKey = embeddingsJobKey(tokenHash, profile.cache_profile_key, jobId);
  const jobLookupKey = embeddingsJobLookupKey(tokenHash, jobId);
  const persisted = await kv
    .atomic()
    .set(jobKey, job, { expireIn: EMBEDDINGS_JOB_TTL_MS })
    .set(jobLookupKey, { cache_profile_key: profile.cache_profile_key } satisfies EmbeddingsJobLookupRecord, { expireIn: EMBEDDINGS_JOB_TTL_MS })
    .commit();
  if (!persisted.ok) {
    await recordErrorUsage(usageContext);
    return openaiError(502, "Embeddings job could not be persisted.", "server_error", {
      type: "server_error",
      param: null,
    });
  }

  const deadlineMs = startedAtMs + EMBEDDINGS_TIMEOUT_MS;
  const entry = await kv.get<EmbeddingsJobRecord>(jobKey);
  const value = entry.value;
  if (!value) {
    await recordErrorUsage(usageContext);
    return openaiError(502, "Embeddings job could not be persisted.", "server_error", {
      type: "server_error",
      param: null,
    });
  }

  return await runEmbeddingsJobAttempt({
    reqId: requestId,
    kv,
    apiKey,
    tokenSeed,
    tokenHash,
    jobKey,
    jobLookupKey,
    jobEntry: entry,
    job: value,
    deadlineMs,
    usageContext,
  });
};

export const handleEmbeddingsJobCreate = async (req: Request, authToken: string | null, usageContext?: UsageContext): Promise<Response> =>
  await runWithResponseTelemetry(usageContext, async (context) => withVoyageUpstreamHeader(await handleEmbeddingsJobCreateInternal(req, authToken, context)));

const handleEmbeddingsJobGetInternal = async (_req: Request, authToken: string | null, jobId: string, usageContext?: UsageContext): Promise<Response> => {
  const requestId = crypto.randomUUID();
  const startedAtMs = Date.now();

  const kv = await getKv();
  if (!kv) {
    await recordErrorUsage(usageContext);
    return openaiError(503, "Embeddings jobs require Deno KV", "server_error", { type: "server_error", param: null });
  }

  const preferredSeed = resolveEmbeddingsJobTokenSeed(jobId, authToken, usageContext);
  const preferredHash = await sha256Hex(preferredSeed);
  const tokenSeed = preferredSeed;
  const tokenHash = preferredHash;
  const jobLookupKey = embeddingsJobLookupKey(preferredHash, jobId);
  const lookupEntry = await kv.get<EmbeddingsJobLookupRecord>(jobLookupKey);
  const cacheProfileKey = isRecord(lookupEntry.value) ? getString(lookupEntry.value.cache_profile_key) : null;
  if (!cacheProfileKey) {
    await recordErrorUsage(usageContext);
    return openaiError(404, "Embeddings job not found", "not_found", {
      type: "invalid_request_error",
      param: null,
    });
  }
  const jobKey = embeddingsJobKey(preferredHash, cacheProfileKey, jobId);
  const entry = await kv.get<EmbeddingsJobRecord>(jobKey);

  const job = entry.value;
  if (job?.cache_profile_key !== cacheProfileKey) {
    await recordErrorUsage(usageContext);
    return openaiError(404, "Embeddings job not found", "not_found", { type: "invalid_request_error", param: null });
  }

  await recordRequestUsage(usageContext, {
    model: job.model,
    route: "embeddings.jobs.get",
    stream: false,
    reasoning: null,
  });

  if (job.status === "succeeded") {
    const vectorsByIndex = await loadEmbeddingsVectorsFromCache(kv, job.cache_profile_key, job.input_hashes, job.dimensions);
    const result = vectorsByIndex.some((vec) => !vec)
      ? null
      : buildOpenAiEmbeddingsResult(job.model, vectorsByIndex, job.usage_total_tokens, job.encoding_format);
    if (!result) {
      // Cache misses are unexpected (cache TTL is longer than job TTL), but if it happens
      // there's nothing the client can do besides resubmitting the job.
      const failed: EmbeddingsJobRecord = {
        ...job,
        status: "failed",
        updated_at_ms: Date.now(),
        locked_until_ms: null,
        retry_after_seconds: null,
        error: {
          message: "Embeddings job result was unavailable; please resubmit.",
          type: "server_error",
          code: "embeddings_job_result_missing",
        },
      };
      await updateEmbeddingsJobRecord(kv, jobKey, jobLookupKey, failed);
      return json(200, buildEmbeddingsJobBody(failed, null), { "x-uos-upstream": failed.upstream });
    }
    return json(200, buildEmbeddingsJobBody(job, result), { "x-uos-upstream": job.upstream });
  }

  if (job.status === "failed") {
    return json(200, buildEmbeddingsJobBody(job, null), { "x-uos-upstream": job.upstream });
  }

  const apiKey = await readVoyageApiKey(kv);
  if (!apiKey) {
    await recordErrorUsage(usageContext);
    return openaiError(503, "Embeddings provider is not configured: set VOYAGEAI_API_KEY (or store it in Deno KV)", "server_error", {
      type: "server_error",
      param: null,
    });
  }

  const deadlineMs = startedAtMs + EMBEDDINGS_TIMEOUT_MS;
  const response = await runEmbeddingsJobAttempt({
    reqId: requestId,
    kv,
    apiKey,
    tokenSeed,
    tokenHash,
    jobKey,
    jobLookupKey,
    jobEntry: entry,
    job,
    deadlineMs,
    usageContext,
  });

  return response;
};

export const handleEmbeddingsJobGet = async (req: Request, authToken: string | null, jobId: string, usageContext?: UsageContext): Promise<Response> =>
  await runWithResponseTelemetry(usageContext, async (context) =>
    withVoyageUpstreamHeader(await handleEmbeddingsJobGetInternal(req, authToken, jobId, context))
  );
