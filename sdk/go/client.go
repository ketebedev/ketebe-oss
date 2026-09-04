package ketebe

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type ClientOptions struct {
	BaseURL      string
	HTTPClient   *http.Client
	Timeout      time.Duration
	MaxRetries   int
	RetryBackoff time.Duration
}

type Client struct {
	baseURL      string
	httpClient   *http.Client
	timeout      time.Duration
	maxRetries   int
	retryBackoff time.Duration
}

func NewClient(options ClientOptions) (*Client, error) {
	base := strings.TrimRight(options.BaseURL, "/")
	if base == "" {
		return nil, errors.New("BaseURL is required")
	}
	if _, err := url.ParseRequestURI(base); err != nil {
		return nil, fmt.Errorf("invalid BaseURL: %w", err)
	}
	timeout := options.Timeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	backoff := options.RetryBackoff
	if backoff == 0 {
		backoff = 50 * time.Millisecond
	}
	hc := options.HTTPClient
	if hc == nil {
		hc = &http.Client{}
	}
	return &Client{baseURL: base, httpClient: hc, timeout: timeout, maxRetries: options.MaxRetries, retryBackoff: backoff}, nil
}

func (c *Client) ListCollections(ctx context.Context) ([]Collection, error) {
	var out struct {
		Collections []Collection `json:"collections"`
	}
	if err := c.request(ctx, http.MethodGet, "/v0/collections", nil, true, &out); err != nil {
		return nil, err
	}
	return out.Collections, nil
}
func (c *Client) CreateCollection(ctx context.Context, in CreateCollection) (Collection, error) {
	var out Collection
	err := c.request(ctx, http.MethodPost, "/v0/collections", in, false, &out)
	return out, err
}
func (c *Client) GetCollection(ctx context.Context, id string) (Collection, error) {
	var out Collection
	err := c.request(ctx, http.MethodGet, "/v0/collections/"+segment(id), nil, true, &out)
	return out, err
}
func (c *Client) DeleteCollection(ctx context.Context, id string) error {
	return c.request(ctx, http.MethodDelete, "/v0/collections/"+segment(id), nil, true, nil)
}
func (c *Client) UpsertRecord(ctx context.Context, collection string, id RecordID, in RecordUpsert) (Mutation, error) {
	var out Mutation
	err := c.request(ctx, http.MethodPut, recordPath(collection, id), in, true, &out)
	return out, err
}
func (c *Client) DeleteRecord(ctx context.Context, collection string, id RecordID) (Mutation, error) {
	var out Mutation
	err := c.request(ctx, http.MethodDelete, recordPath(collection, id), nil, true, &out)
	return out, err
}
func (c *Client) BatchUpsertRecords(ctx context.Context, collection string, in BatchUpsert) (map[string]any, error) {
	var out map[string]any
	err := c.request(ctx, http.MethodPost, "/v0/collections/"+segment(collection)+"/records:batchUpsert", in, true, &out)
	return out, err
}
func (c *Client) UpsertDocument(ctx context.Context, collection string, id RecordID, in DocumentUpsert) (map[string]any, error) {
	var out map[string]any
	err := c.request(ctx, http.MethodPut, "/v0/collections/"+segment(collection)+"/documents/"+segment(id.PathValue()), in, true, &out)
	return out, err
}
func (c *Client) DeleteDocument(ctx context.Context, collection string, id RecordID) (map[string]any, error) {
	var out map[string]any
	err := c.request(ctx, http.MethodDelete, "/v0/collections/"+segment(collection)+"/documents/"+segment(id.PathValue()), nil, true, &out)
	return out, err
}
func (c *Client) Query(ctx context.Context, collection string, in QueryRequest) (QueryResponse, error) {
	var out QueryResponse
	err := c.request(ctx, http.MethodPost, "/v1/collections/"+segment(collection)+"/query", in, true, &out)
	return out, err
}
func (c *Client) GetJob(ctx context.Context, jobID string) (Job, error) {
	var out Job
	err := c.request(ctx, http.MethodGet, "/v0/jobs/"+segment(jobID), nil, true, &out)
	return out, err
}
func (c *Client) CancelJob(ctx context.Context, jobID string) (Job, error) {
	var out Job
	err := c.request(ctx, http.MethodPost, "/v0/jobs/"+segment(jobID)+"/cancel", nil, false, &out)
	return out, err
}
func (c *Client) GetEmbeddingMigration(ctx context.Context, collection string) (EmbeddingMigration, error) {
	var out EmbeddingMigration
	err := c.request(ctx, http.MethodGet, migrationPath(collection), nil, true, &out)
	return out, err
}
func (c *Client) StartEmbeddingMigration(ctx context.Context, collection string, in StartEmbeddingMigration) (EmbeddingMigration, error) {
	var out EmbeddingMigration
	err := c.request(ctx, http.MethodPost, migrationPath(collection), in, false, &out)
	return out, err
}
func (c *Client) CatchUpEmbeddingMigration(ctx context.Context, collection string) (EmbeddingMigration, error) {
	var out EmbeddingMigration
	err := c.request(ctx, http.MethodPost, migrationPath(collection)+"/catch-up", nil, false, &out)
	return out, err
}
func (c *Client) StartEmbeddingMigrationCatchUpJob(ctx context.Context, collection string) (Job, error) {
	var out Job
	err := c.request(ctx, http.MethodPost, migrationPath(collection)+"/catch-up-job", nil, false, &out)
	return out, err
}
func (c *Client) ActivateEmbeddingMigration(ctx context.Context, collection string) (EmbeddingMigration, error) {
	var out EmbeddingMigration
	err := c.request(ctx, http.MethodPost, migrationPath(collection)+"/activate", nil, false, &out)
	return out, err
}

func recordPath(collection string, id RecordID) string {
	return "/v0/collections/" + segment(collection) + "/records/" + segment(id.PathValue())
}
func migrationPath(collection string) string {
	return "/v0/collections/" + segment(collection) + "/embedding-migration"
}
func segment(s string) string { return url.PathEscape(s) }

func (c *Client) request(parent context.Context, method, path string, body any, idempotent bool, out any) error {
	attempts := 1
	if idempotent {
		attempts += c.maxRetries
	}
	var last error
	for attempt := 0; attempt < attempts; attempt++ {
		ctx, cancel := context.WithTimeout(parent, c.timeout)
		err := c.requestOnce(ctx, method, path, body, out)
		cancel()
		if err == nil {
			return nil
		}
		last = err
		var api *APIError
		retryable := !errors.As(err, &api) || api.StatusCode == 429 || api.StatusCode >= 500
		if !idempotent || !retryable || attempt+1 >= attempts {
			return err
		}
		timer := time.NewTimer(c.retryBackoff)
		select {
		case <-parent.Done():
			timer.Stop()
			return &TransportError{Err: parent.Err()}
		case <-timer.C:
		}
	}
	return last
}

func (c *Client) requestOnce(ctx context.Context, method, path string, body any, out any) error {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return &TransportError{Err: err}
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return &TransportError{Err: err}
	}
	if body != nil {
		req.Header.Set("content-type", "application/json")
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return &TransportError{Err: err}
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return &TransportError{Err: err}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return decodeAPIError(resp.StatusCode, data)
	}
	if out == nil || len(data) == 0 {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(out); err != nil {
		return &TransportError{Err: err}
	}
	return nil
}

func decodeAPIError(status int, data []byte) error {
	var env struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(data, &env) == nil && env.Error.Code != "" {
		return &APIError{StatusCode: status, Code: env.Error.Code, Message: env.Error.Message}
	}
	return &APIError{StatusCode: status, Code: "http_error", Message: fmt.Sprintf("HTTP %d", status)}
}
