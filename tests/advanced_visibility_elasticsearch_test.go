//go:build !temporal_minimal || temporal_with_elasticsearch

package tests

import (
	"fmt"
	"strconv"

	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/tests/testcore"
)

func (s *AdvancedVisibilitySuite) updateMaxResultWindow(env *testcore.TestEnv) {
	esConfig := env.GetTestClusterConfig().ESConfig

	esClient, err := esclient.NewFunctionalTestsClient(esConfig, env.Logger)
	s.NoError(err)

	acknowledged, err := esClient.IndexPutSettings(
		s.Context(),
		esConfig.GetVisibilityIndex(),
		fmt.Sprintf(`{"max_result_window" : %d}`, testcore.DefaultPageSize))
	s.NoError(err)
	s.True(acknowledged)

	s.Awaitf(
		func(s *AdvancedVisibilitySuite) {
			settings, err := esClient.IndexGetSettings(s.Context(), esConfig.GetVisibilityIndex())
			s.NoError(err)
			indexSettings, ok := settings[esConfig.GetVisibilityIndex()].Settings["index"].(map[string]any)
			s.True(ok)
			maxResultWindow, ok := indexSettings["max_result_window"].(string)
			s.True(ok)
			s.Equal(strconv.Itoa(testcore.DefaultPageSize), maxResultWindow)
		},
		testcore.WaitForESToSettle,
		esPollInterval,
		"ES max result window size hasn't reach target size within %v",
		testcore.WaitForESToSettle,
	)
}
