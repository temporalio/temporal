//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination client_mock.go

package client

import (
	"context"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
)

const (
	versionTypeExternal                 = "external"
	minimumCloseIdleConnectionsInterval = 15 * time.Second
)

type (
	// Client is a wrapper around Elasticsearch client library.
	Client interface {
		Get(ctx context.Context, index string, docID string) (*GetResult, error)
		Search(ctx context.Context, p *SearchParameters) (*SearchResult, error)
		Count(ctx context.Context, index string, query Query) (int64, error)
		CountGroupBy(ctx context.Context, index string, query Query, aggName string, agg Aggregation) (*SearchResult, error)
		RunBulkProcessor(ctx context.Context, p *BulkProcessorParameters) (BulkProcessor, error)

		// TODO (alex): move this to some admin client (and join with IntegrationTestsClient)
		PutMapping(ctx context.Context, index string, mapping map[string]enumspb.IndexedValueType) (bool, error)
		WaitForYellowStatus(ctx context.Context, index string) (string, error)
		GetMapping(ctx context.Context, index string) (map[string]string, error)
		IndexExists(ctx context.Context, indexName string) (bool, error)
		CreateIndex(ctx context.Context, index string, body map[string]any) (bool, error)
		DeleteIndex(ctx context.Context, indexName string) (bool, error)
		CatIndices(ctx context.Context, target string) (CatIndicesResponse, error)

		OpenScroll(ctx context.Context, p *SearchParameters, keepAliveInterval string) (*SearchResult, error)
		Scroll(ctx context.Context, id string, keepAliveInterval string) (*SearchResult, error)
		CloseScroll(ctx context.Context, id string) error

		IsPointInTimeSupported(ctx context.Context) bool
		OpenPointInTime(ctx context.Context, index string, keepAliveInterval string) (string, error)
		ClosePointInTime(ctx context.Context, id string) (bool, error)
	}

	CLIClient interface {
		Client
		Delete(ctx context.Context, indexName string, docID string, version int64) error
	}

	IntegrationTestsClient interface {
		Client
		IndexPutTemplate(ctx context.Context, templateName string, bodyString string) (bool, error)
		IndexPutSettings(ctx context.Context, indexName string, bodyString string) (bool, error)
		IndexGetSettings(ctx context.Context, indexName string) (map[string]*IndicesGetSettingsResponse, error)
		Ping(ctx context.Context) error
	}

	// SearchParameters holds all required and optional parameters for executing a search.
	SearchParameters struct {
		Index    string
		Query    Query
		PageSize int
		Sorter   []Sorter

		SearchAfter []interface{}
		ScrollID    string
		PointInTime *PointInTime
	}

	// Query interface for go-elasticsearch v8 compatibility
	Query interface {
		Source() (interface{}, error)
	}

	// Sorter interface for go-elasticsearch v8 compatibility
	Sorter interface {
		Source() (interface{}, error)
	}

	// Aggregation interface for go-elasticsearch v8 compatibility
	Aggregation interface {
		Source() (interface{}, error)
	}

	// PointInTime for go-elasticsearch v8 compatibility
	PointInTime struct {
		Id        string `json:"id"`
		KeepAlive string `json:"keep_alive"`
	}

	// GetResult represents a get result from Elasticsearch
	GetResult struct {
		Index   string      `json:"_index"`
		Type    string      `json:"_type"`
		Id      string      `json:"_id"`
		Version *int64      `json:"_version"`
		Found   bool        `json:"found"`
		Source  interface{} `json:"_source"`
	}

	// SearchResult represents a search result from Elasticsearch
	SearchResult struct {
		TotalHits     int64                  `json:"total"`
		Hits          []*SearchHit           `json:"hits"`
		ScrollId      string                 `json:"_scroll_id,omitempty"`
		PointInTimeId string                 `json:"pit_id,omitempty"`
		Aggregations  map[string]interface{} `json:"aggregations,omitempty"`
	}

	// SearchHit represents a single search hit
	SearchHit struct {
		Index  string        `json:"_index"`
		Type   string        `json:"_type"`
		Id     string        `json:"_id"`
		Score  *float64      `json:"_score"`
		Source interface{}   `json:"_source"`
		Sort   []interface{} `json:"sort,omitempty"`
	}

	// CatIndicesResponse represents cat indices API response
	CatIndicesResponse []map[string]interface{}

	// IndicesGetSettingsResponse represents indices get settings API response
	IndicesGetSettingsResponse map[string]interface{}
)

// NewPointInTime creates a new PointInTime instance
func NewPointInTime(id string, keepAlive string) *PointInTime {
	return &PointInTime{
		Id:        id,
		KeepAlive: keepAlive,
	}
}
