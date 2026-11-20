package elasticsearch

import (
	"context"
	"errors"
	"maps"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/searchattribute"
	"go.uber.org/mock/gomock"
)

func TestCheckPreallocatedFieldMappings(t *testing.T) {
	const index = "test-index"
	withPool := maps.Clone(searchattribute.TestEsNameTypeMap().Custom())
	maps.Copy(withPool, searchattribute.TestNameTypeMap().Custom())

	testCases := []struct {
		name        string
		mapping     map[string]string
		mappingErr  error
		expectGauge *float64
	}{
		{
			name:        "all pre-allocated fields mapped",
			mapping:     mappingOf(withPool),
			expectGauge: new(float64(0)),
		},
		{
			name:        "pre-allocated fields not mapped",
			mapping:     mappingOf(searchattribute.TestEsNameTypeMap().Custom()),
			expectGauge: new(float64(28)),
		},
		{
			name:    "index is an alias, mapping can't be checked",
			mapping: map[string]string{},
		},
		{
			name:       "mapping lookup fails",
			mappingErr: errors.New("boom"),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			saProvider := searchattribute.NewMockProvider(ctrl)
			saProvider.EXPECT().GetSearchAttributes(index, false).Return(searchattribute.NewNameTypeMap(withPool), nil)
			esClient := client.NewMockClient(ctrl)
			esClient.EXPECT().GetMapping(gomock.Any(), index).Return(tc.mapping, tc.mappingErr)

			metricsHandler := metricstest.NewCaptureHandler()
			capture := metricsHandler.StartCapture()
			defer metricsHandler.StopCapture(capture)

			s := &VisibilityStore{
				esClient:                 esClient,
				index:                    index,
				searchAttributesProvider: saProvider,
				metricsHandler:           metricsHandler,
				logger:                   log.NewNoopLogger(),
			}
			s.checkPreallocatedFieldMappings(context.Background())

			recordings := capture.SnapshotMetric(metrics.ElasticsearchMissingPreallocatedFieldMappings.Name())
			if tc.expectGauge == nil {
				require.Empty(t, recordings)
				return
			}
			require.Len(t, recordings, 1)
			require.InDelta(t, *tc.expectGauge, recordings[0].Value, 0)
		})
	}
}

func TestCheckPreallocatedFieldMappingsSkipsWithoutPreallocatedFields(t *testing.T) {
	ctrl := gomock.NewController(t)
	saProvider := searchattribute.NewMockProvider(ctrl)
	saProvider.EXPECT().GetSearchAttributes("test-index", false).Return(searchattribute.TestEsNameTypeMap(), nil)
	// No GetMapping call is expected.
	s := &VisibilityStore{
		esClient:                 client.NewMockClient(ctrl),
		index:                    "test-index",
		searchAttributesProvider: saProvider,
		metricsHandler:           metrics.NoopMetricsHandler,
		logger:                   log.NewNoopLogger(),
	}
	s.checkPreallocatedFieldMappings(context.Background())
}

func mappingOf[T any](fields map[string]T) map[string]string {
	result := make(map[string]string, len(fields))
	for name := range fields {
		result[name] = "keyword"
	}
	return result
}
