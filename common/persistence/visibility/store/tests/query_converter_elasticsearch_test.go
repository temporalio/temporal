//go:build !temporal_minimal || temporal_with_elasticsearch

package tests

import (
	"encoding/json"
	"testing"

	"github.com/olivere/elastic/v7"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute"
)

func serializeESQuery(q elastic.Query) (string, error) {
	if q == nil {
		return "", nil
	}
	source, err := q.Source()
	if err != nil {
		return "", err
	}
	data, err := json.Marshal(source)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func TestElasticsearchQueryConverter(t *testing.T) {
	t.Parallel()
	runQueryConverterTest(
		t,
		elasticsearch.PersistenceName,
		newESQueryConverter,
		serializeESQuery,
	)
}

func newESQueryConverter() *query.QueryConverter[elastic.Query] {
	return elasticsearch.NewQueryConverter(
		testNamespaceName,
		searchattribute.TestNameTypeMap(),
		&searchattribute.TestMapper{},
		metrics.NoopMetricsHandler,
		log.NewNoopLogger(),
	)
}
