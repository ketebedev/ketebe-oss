package ketebe

import (
	"context"
	"os"
	"testing"
)

func TestRealServerTypedIDsAndQuery(t *testing.T) {
	base := os.Getenv("KETEBE_BASE_URL")
	if base == "" {
		t.Skip("KETEBE_BASE_URL not set")
	}
	c, err := NewClient(ClientOptions{BaseURL: base})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_, err = c.CreateCollection(ctx, CreateCollection{ID: "go-sdk", Dimension: 2, Metric: "dot"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.DeleteCollection(ctx, "go-sdk")

	_, err = c.BatchUpsertRecords(ctx, "go-sdk", BatchUpsert{Records: []BatchRecordUpsert{
		{ID: StringID("42"), Vector: []float32{1, 0}},
		{ID: U64ID(42), Vector: []float32{1, 0}},
	}})
	if err != nil {
		t.Fatal(err)
	}

	out, err := c.Query(ctx, "go-sdk", QueryRequest{Vector: []float32{1, 0}, TopK: 2, Execution: "exact"})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Hits) != 2 {
		t.Fatalf("hits=%d", len(out.Hits))
	}
	seenString, seenU64 := false, false
	for _, hit := range out.Hits {
		if hit.ID == StringID("42") {
			seenString = true
		}
		if hit.ID == U64ID(42) {
			seenU64 = true
		}
	}
	if !seenString || !seenU64 {
		t.Fatalf("typed ids not preserved: %#v", out.Hits)
	}
}
