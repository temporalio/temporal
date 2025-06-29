package client

import (
	"context"

	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
)

// ElasticClient defines the interface for interacting with Elasticsearch.
type (
	ElasticClient interface {
		IndexExists(ctx context.Context, indexName string) (bool, error)
		Ping(ctx context.Context) error
		CreateIndex(ctx context.Context, index string, body map[string]any) (bool, error)
		DeleteIndex(ctx context.Context, indexName string) (bool, error)
		GetDocument(ctx context.Context, index string, docID string) (*types.GetResult, error)
	}
)
