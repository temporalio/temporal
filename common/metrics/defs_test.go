package metrics

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var IsMetric = regexp.MustCompile(`^[a-z][a-z_]*$`).MatchString

func TestScopeDefsMapped(t *testing.T) {
	for i := PersistenceCreateShardScope; i < NumCommonScopes; i++ {
		key, ok := ScopeDefs[Common][i]
		require.True(t, ok)
		require.NotEmpty(t, key)
		for tag := range key.tags {
			assert.True(t, IsMetric(tag), "metric tags should conform to regex")
		}
	}
	for i := FrontendStartWorkflowExecutionScope; i < NumFrontendScopes; i++ {
		key, ok := ScopeDefs[Frontend][i]
		require.True(t, ok)
		require.NotEmpty(t, key)
		for tag := range key.tags {
			assert.True(t, IsMetric(tag), "metric tags should conform to regex")
		}
	}
	for i := HistoryStartWorkflowExecutionScope; i < NumHistoryScopes; i++ {
		key, ok := ScopeDefs[History][i]
		require.True(t, ok)
		require.NotEmpty(t, key)
		for tag := range key.tags {
			assert.True(t, IsMetric(tag), "metric tags should conform to regex")
		}
	}
	for i := MatchingPollForDecisionTaskScope; i < NumMatchingScopes; i++ {
		key, ok := ScopeDefs[Matching][i]
		require.True(t, ok)
		require.NotEmpty(t, key)
		for tag := range key.tags {
			assert.True(t, IsMetric(tag), "metric tags should conform to regex")
		}
	}
	for i := ReplicatorScope; i < NumWorkerScopes; i++ {
		key, ok := ScopeDefs[Worker][i]
		require.True(t, ok)
		require.NotEmpty(t, key)
		for tag := range key.tags {
			assert.True(t, IsMetric(tag), "metric tags should conform to regex")
		}
	}
}

func TestMetricDefsMapped(t *testing.T) {
	for i := ServiceRequests; i < NumCommonMetrics; i++ {
		key, ok := MetricDefs[Common][i]
		require.True(t, ok)
		require.NotEmpty(t, key)
	}
	for i := TaskRequests; i < NumHistoryMetrics; i++ {
		key, ok := MetricDefs[History][i]
		require.True(t, ok)
		require.NotEmpty(t, key)
	}
	for i := PollSuccessCounter; i < NumMatchingMetrics; i++ {
		key, ok := MetricDefs[Matching][i]
		require.True(t, ok)
		require.NotEmpty(t, key)
	}
	for i := ReplicatorMessages; i < NumWorkerMetrics; i++ {
		key, ok := MetricDefs[Worker][i]
		require.True(t, ok)
		require.NotEmpty(t, key)
	}
}

func TestMetricDefs(t *testing.T) {
	for service, metrics := range MetricDefs {
		for _, metricDef := range metrics {
			matched := IsMetric(string(metricDef.metricName))
			assert.True(t, matched, fmt.Sprintf("Service: %v, metric_name: %v", service, metricDef.metricName))
		}
	}
}
