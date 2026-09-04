import JSONBigFactory from "json-bigint";
import { ApiError, TransportError } from "./errors.js";
import type {
  BatchUpsert,
  Collection,
  CreateCollection,
  DocumentUpsert,
  EmbeddingMigration,
  Job,
  Mutation,
  QueryRequest,
  QueryResponse,
  RecordId,
  RecordUpsert,
  StartEmbeddingMigration,
} from "./models.js";

const JSONBig = JSONBigFactory({ useNativeBigInt: true });

export interface ClientOptions {
  baseUrl: string;
  timeoutMs?: number;
  maxRetries?: number;
  retryBackoffMs?: number;
  fetchImpl?: typeof fetch;
}

export class Client {
  private readonly baseUrl: string;
  private readonly timeoutMs: number;
  private readonly maxRetries: number;
  private readonly retryBackoffMs: number;
  private readonly fetchImpl: typeof fetch;

  constructor(options: ClientOptions) {
    this.baseUrl = options.baseUrl.replace(/\/+$/, "");
    this.timeoutMs = options.timeoutMs ?? 10_000;
    this.maxRetries = options.maxRetries ?? 2;
    this.retryBackoffMs = options.retryBackoffMs ?? 50;
    this.fetchImpl = options.fetchImpl ?? fetch;
  }

  async listCollections(): Promise<Collection[]> {
    const body = await this.request<{ collections: Collection[] }>("GET", "/v0/collections", undefined, true);
    return body.collections;
  }

  createCollection(request: CreateCollection): Promise<Collection> {
    return this.request("POST", "/v0/collections", request, false);
  }

  getCollection(id: string): Promise<Collection> {
    return this.request("GET", `/v0/collections/${encodeURIComponent(id)}`, undefined, true);
  }

  async deleteCollection(id: string): Promise<void> {
    await this.request("DELETE", `/v0/collections/${encodeURIComponent(id)}`, undefined, true, true);
  }

  upsertRecord(collection: string, id: RecordId, request: RecordUpsert): Promise<Mutation> {
    return this.request("PUT", `/v0/collections/${encodeURIComponent(collection)}/records/${this.recordIdPath(id)}`, request, true);
  }

  deleteRecord(collection: string, id: RecordId): Promise<Mutation> {
    return this.request("DELETE", `/v0/collections/${encodeURIComponent(collection)}/records/${this.recordIdPath(id)}`, undefined, true);
  }

  batchUpsertRecords(collection: string, request: BatchUpsert): Promise<unknown> {
    return this.request("POST", `/v0/collections/${encodeURIComponent(collection)}/records:batchUpsert`, request, true);
  }

  upsertDocument(collection: string, id: RecordId, request: DocumentUpsert): Promise<unknown> {
    return this.request("PUT", `/v0/collections/${encodeURIComponent(collection)}/documents/${this.recordIdPath(id)}`, request, true);
  }

  deleteDocument(collection: string, id: RecordId): Promise<unknown> {
    return this.request("DELETE", `/v0/collections/${encodeURIComponent(collection)}/documents/${this.recordIdPath(id)}`, undefined, true);
  }

  query(collection: string, request: QueryRequest): Promise<QueryResponse> {
    return this.request("POST", `/v1/collections/${encodeURIComponent(collection)}/query`, request, true);
  }

  getJob(jobId: string): Promise<Job> {
    return this.request("GET", `/v0/jobs/${encodeURIComponent(jobId)}`, undefined, true);
  }

  cancelJob(jobId: string): Promise<Job> {
    return this.request("POST", `/v0/jobs/${encodeURIComponent(jobId)}/cancel`, undefined, false);
  }

  getEmbeddingMigration(collection: string): Promise<EmbeddingMigration> {
    return this.request("GET", `/v0/collections/${encodeURIComponent(collection)}/embedding-migration`, undefined, true);
  }

  startEmbeddingMigration(collection: string, request: StartEmbeddingMigration): Promise<EmbeddingMigration> {
    return this.request("POST", `/v0/collections/${encodeURIComponent(collection)}/embedding-migration`, request, false);
  }

  catchUpEmbeddingMigration(collection: string): Promise<EmbeddingMigration> {
    return this.request("POST", `/v0/collections/${encodeURIComponent(collection)}/embedding-migration/catch-up`, undefined, false);
  }

  startEmbeddingMigrationCatchUpJob(collection: string): Promise<Job> {
    return this.request("POST", `/v0/collections/${encodeURIComponent(collection)}/embedding-migration/catch-up-job`, undefined, false);
  }

  activateEmbeddingMigration(collection: string): Promise<EmbeddingMigration> {
    return this.request("POST", `/v0/collections/${encodeURIComponent(collection)}/embedding-migration/activate`, undefined, false);
  }

  private recordIdPath(id: RecordId): string {
    return encodeURIComponent(id.value.toString());
  }

  private async request<T>(
    method: string,
    path: string,
    body: unknown,
    idempotent: boolean,
    allowEmpty = false,
  ): Promise<T> {
    const attempts = idempotent ? this.maxRetries + 1 : 1;
    for (let attempt = 0; attempt < attempts; attempt += 1) {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), this.timeoutMs);
      try {
        const init: RequestInit = { method, signal: controller.signal };
        if (body !== undefined) {
          init.headers = { "content-type": "application/json" };
          init.body = JSONBig.stringify(body);
        }
        const response = await this.fetchImpl(`${this.baseUrl}${path}`, init);
        const text = await response.text();
        if (response.ok) {
          if (allowEmpty && text.length === 0) return undefined as T;
          return (text.length === 0 ? undefined : JSONBig.parse(text)) as T;
        }
        if (idempotent && attempt + 1 < attempts && (response.status === 429 || response.status >= 500)) {
          await this.sleep();
          continue;
        }
        throw this.toApiError(response.status, text);
      } catch (error) {
        if (error instanceof ApiError) throw error;
        if (idempotent && attempt + 1 < attempts) {
          await this.sleep();
          continue;
        }
        throw new TransportError("Ketebe request failed", error);
      } finally {
        clearTimeout(timer);
      }
    }
    throw new TransportError("Ketebe request exhausted retries");
  }

  private toApiError(status: number, text: string): ApiError {
    try {
      const envelope = JSONBig.parse(text) as { error?: { code?: string; message?: string } };
      return new ApiError(status, envelope.error?.code ?? "http_error", envelope.error?.message ?? `HTTP ${status}`);
    } catch {
      return new ApiError(status, "http_error", `HTTP ${status}`);
    }
  }

  private sleep(): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, this.retryBackoffMs));
  }
}
