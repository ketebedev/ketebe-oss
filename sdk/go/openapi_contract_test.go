package ketebe

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestOpenAPIContainsGoSDKSurface(t *testing.T) {
	path := filepath.Join("..", "..", "api", "openapi", "v1.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var spec struct {
		Paths map[string]map[string]any `json:"paths"`
	}
	if err := json.Unmarshal(raw, &spec); err != nil {
		t.Fatal(err)
	}
	operations := [][2]string{
		{"get", "/v0/collections"}, {"post", "/v0/collections"},
		{"put", "/v0/collections/{collection_id}/records/{record_id}"},
		{"post", "/v0/collections/{collection_id}/records:batchUpsert"},
		{"put", "/v0/collections/{collection_id}/documents/{record_id}"},
		{"post", "/v1/collections/{collection_id}/query"},
		{"get", "/v0/jobs/{job_id}"}, {"post", "/v0/jobs/{job_id}/cancel"},
		{"get", "/v0/collections/{collection_id}/embedding-migration"},
		{"post", "/v0/collections/{collection_id}/embedding-migration"},
		{"post", "/v0/collections/{collection_id}/embedding-migration/catch-up"},
		{"post", "/v0/collections/{collection_id}/embedding-migration/catch-up-job"},
		{"post", "/v0/collections/{collection_id}/embedding-migration/activate"},
		{"get", "/v0/organizations"},
		{"post", "/v0/organizations"},
		{"get", "/v0/organizations/{organization_id}"},
		{"patch", "/v0/organizations/{organization_id}"},
		{"delete", "/v0/organizations/{organization_id}"},
		{"get", "/v0/organizations/{organization_id}/projects"},
		{"post", "/v0/organizations/{organization_id}/projects"},
		{"get", "/v0/projects/{project_id}"},
		{"patch", "/v0/projects/{project_id}"},
		{"delete", "/v0/projects/{project_id}"},
	}
	for _, op := range operations {
		if spec.Paths[op[1]][op[0]] == nil {
			t.Fatalf("missing %s %s", op[0], op[1])
		}
	}
}
