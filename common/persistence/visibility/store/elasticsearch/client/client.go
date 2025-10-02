//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination client_mock.go

package client

import (
	"context"
	"time"

	"github.com/olivere/elastic/v7"
	enumspb "go.temporal.io/api/enums/v1"
)

const (
	versionTypeExternal                 = "external"
	minimumCloseIdleConnectionsInterval = 15 * time.Second
)

type (
	// Client is a wrapper around Elasticsearch client library.
	Client interface {
		Get(ctx context.Context, index string, docID string) (*elastic.GetResult, error)
		Search(ctx context.Context, p *SearchParameters) (*elastic.SearchResult, error)
		Count(ctx context.Context, index string, query elastic.Query) (int64, error)
		CountGroupBy(ctx context.Context, index string, query elastic.Query, aggName string, agg elastic.Aggregation) (*elastic.SearchResult, error)
		RunBulkProcessor(ctx context.Context, p *BulkProcessorParameters) (BulkProcessor, error)

		// TODO (alex): move this to some admin client (and join with IntegrationTestsClient)
		PutMapping(ctx context.Context, index string, mapping map[string]enumspb.IndexedValueType) (bool, error)
		WaitForYellowStatus(ctx context.Context, index string) (string, error)
		GetMapping(ctx context.Context, index string) (map[string]string, error)
		IndexExists(ctx context.Context, indexName string) (bool, error)
		CreateIndex(ctx context.Context, index string, body map[string]any) (bool, error)
		DeleteIndex(ctx context.Context, indexName string) (bool, error)
		CatIndices(ctx context.Context, target string) (elastic.CatIndicesResponse, error)
	}

	CLIClient interface {
		Client
		Delete(ctx context.Context, indexName string, docID string, version int64) error
		IndexPutTemplate(ctx context.Context, templateName string, bodyString string) (bool, error)
		IndexPutMapping(ctx context.Context, indexName string, bodyString string) (bool, error)
		ClusterPutSettings(ctx context.Context, bodyString string) (bool, error)
		Ping(ctx context.Context) error
	}

	IntegrationTestsClient interface {
		Client
		IndexPutTemplate(ctx context.Context, templateName string, bodyString string) (bool, error)
		IndexPutSettings(ctx context.Context, indexName string, bodyString string) (bool, error)
		IndexGetSettings(ctx context.Context, indexName string) (map[string]*elastic.IndicesGetSettingsResponse, error)
		Ping(ctx context.Context) error
	}

	// SearchParameters holds all required and optional parameters for executing a search.
	SearchParameters struct {
		Index       string
		Query       elastic.Query
		PageSize    int
		Sorter      []elastic.Sorter
		SearchAfter []interface{}
	}
)
