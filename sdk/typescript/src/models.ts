export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };

export type RecordId =
  | { type: "string"; value: string }
  | { type: "u64"; value: bigint };

export interface CreateCollection {
  id: string;
  dimension: number;
  metric: string;
  lexical_fields?: string[][];
}

export interface Collection {
  id: string;
  dimension: number;
  metric: string;
  [key: string]: unknown;
}

export interface RecordUpsert {
  vector: number[];
  metadata?: JsonValue;
}

export interface BatchRecordUpsert extends RecordUpsert {
  id: RecordId;
}

export interface BatchUpsert {
  records: BatchRecordUpsert[];
}

export interface Mutation { sequence_number: bigint }

export interface DocumentUpsert {
  text: string;
  metadata?: JsonValue;
  source?: JsonValue;
  chunking?: JsonValue;
}

export interface QueryRequest {
  vector?: number[];
  text?: string;
  top_k?: number;
  predicate?: JsonValue;
  execution?: string;
  dense_candidates?: number;
  lexical_candidates?: number;
  search_profile?: string;
  timeout_ms?: number;
  explain?: boolean;
}

export interface QueryHit {
  id: RecordId;
  score: number;
  sequence_number: bigint;
  metadata?: JsonValue;
  [key: string]: unknown;
}

export interface QueryResponse {
  api_version: string;
  hits: QueryHit[];
  explain?: JsonValue;
}

export interface Job {
  id: string;
  kind: string;
  state: string;
  [key: string]: unknown;
}

export interface StartEmbeddingMigration { target_profile: string }
export type EmbeddingMigration = Record<string, unknown>;

export type ResourceLifecycle = "active" | "suspended" | "deleting";

export interface Organization {
  id: string;
  name: string;
  slug: string;
  lifecycle: ResourceLifecycle;
  created_at_unix: bigint;
  updated_at_unix: bigint;
}

export interface Project {
  id: string;
  organization_id: string;
  name: string;
  slug: string;
  lifecycle: ResourceLifecycle;
  created_at_unix: bigint;
  updated_at_unix: bigint;
}

export interface CreateOrganization { id: string; name: string; slug: string }
export interface UpdateOrganization { name: string; slug: string; lifecycle: ResourceLifecycle }
export interface CreateProject { id: string; name: string; slug: string }
export interface UpdateProject { name: string; slug: string; lifecycle: ResourceLifecycle }
