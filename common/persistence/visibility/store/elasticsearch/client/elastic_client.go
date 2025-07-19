package client

import (
	"context"
)

// GetResult represents the result of getting a document from Elasticsearch
// This is a generic type that can be used with different ES client libraries
type GetResult struct {
	Id_     string                 `json:"_id"`
	Source_ map[string]interface{} `json:"_source"`
	Found   bool                   `json:"found"`
}

// ElasticClient defines the interface for interacting with Elasticsearch.
type (
	ElasticClient interface {
		IndexExists(ctx context.Context, indexName string) (bool, error)
		Ping(ctx context.Context) error
		CreateIndex(ctx context.Context, index string, body map[string]any) (bool, error)
		DeleteIndex(ctx context.Context, indexName string) (bool, error)
		GetDocument(ctx context.Context, index string, docID string) (*GetResult, error)
	}
)
