package ketebe

import "encoding/json"

type Collection map[string]any

type CreateCollection struct {
	ID            string     `json:"id"`
	Dimension     int        `json:"dimension"`
	Metric        string     `json:"metric"`
	LexicalFields [][]string `json:"lexical_fields,omitempty"`
}

type RecordUpsert struct {
	Vector   []float32 `json:"vector"`
	Metadata any       `json:"metadata,omitempty"`
}

type BatchRecordUpsert struct {
	ID       RecordID  `json:"id"`
	Vector   []float32 `json:"vector"`
	Metadata any       `json:"metadata,omitempty"`
}

type BatchUpsert struct {
	Records []BatchRecordUpsert `json:"records"`
}

type Mutation struct {
	SequenceNumber uint64 `json:"sequence_number"`
}

type DocumentUpsert struct {
	Text     string `json:"text"`
	Metadata any    `json:"metadata,omitempty"`
	Source   any    `json:"source,omitempty"`
	Chunking any    `json:"chunking,omitempty"`
}

type QueryRequest struct {
	Vector            []float32 `json:"vector,omitempty"`
	Text              string    `json:"text,omitempty"`
	TopK              int       `json:"top_k,omitempty"`
	Predicate         any       `json:"predicate,omitempty"`
	Execution         string    `json:"execution,omitempty"`
	DenseCandidates   int       `json:"dense_candidates,omitempty"`
	LexicalCandidates int       `json:"lexical_candidates,omitempty"`
	SearchProfile     string    `json:"search_profile,omitempty"`
	TimeoutMS         uint64    `json:"timeout_ms,omitempty"`
	Explain           bool      `json:"explain,omitempty"`
}

type QueryHit struct {
	ID             RecordID `json:"id"`
	Score          float64  `json:"score"`
	SequenceNumber uint64   `json:"sequence_number"`
	Metadata       any      `json:"metadata,omitempty"`
}

type QueryResponse struct {
	APIVersion string          `json:"api_version"`
	Hits       []QueryHit      `json:"hits"`
	Explain    json.RawMessage `json:"explain,omitempty"`
}

type Job map[string]any
type EmbeddingMigration map[string]any

type StartEmbeddingMigration struct {
	TargetProfile string `json:"target_profile"`
}
