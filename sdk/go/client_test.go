package ketebe

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestRecordIDVariantsAreLossless(t *testing.T) {
	cases := []RecordID{StringID("42"), U64ID(^uint64(0))}
	for _, want := range cases {
		data, err := json.Marshal(want)
		if err != nil {
			t.Fatal(err)
		}
		var got RecordID
		if err := json.Unmarshal(data, &got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("round trip mismatch: %#v != %#v", got, want)
		}
	}
	a, _ := json.Marshal(StringID("42"))
	b, _ := json.Marshal(U64ID(42))
	if string(a) == string(b) {
		t.Fatal("typed ids collapsed")
	}
}

func TestRetriesOnlyIdempotentRequests(t *testing.T) {
	var gets atomic.Int32
	var posts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			gets.Add(1)
		} else {
			posts.Add(1)
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":{"code":"busy","message":"busy"}}`))
	}))
	defer srv.Close()
	c, _ := NewClient(ClientOptions{BaseURL: srv.URL, MaxRetries: 2, RetryBackoff: time.Millisecond})
	_, _ = c.ListCollections(context.Background())
	_, _ = c.CreateCollection(context.Background(), CreateCollection{ID: "x", Dimension: 1, Metric: "dot"})
	if gets.Load() != 3 {
		t.Fatalf("GET attempts=%d", gets.Load())
	}
	if posts.Load() != 1 {
		t.Fatalf("POST attempts=%d", posts.Load())
	}
}

func TestContextCancellationStopsRequest(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer srv.Close()
	c, _ := NewClient(ClientOptions{BaseURL: srv.URL, Timeout: 5 * time.Second})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := c.ListCollections(ctx)
	var transport *TransportError
	if !errors.As(err, &transport) {
		t.Fatalf("expected TransportError, got %T", err)
	}
}
