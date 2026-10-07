package health

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/primitives"
)

func TestSourceString(t *testing.T) {
	source := Source{
		Service:   primitives.HistoryService,
		Component: ComponentPersistence,
	}

	actual := source.String()
	expected := "history.persistence"

	require.Equal(t, expected, actual)
}

func TestOverallCheckType(t *testing.T) {
	source := Source{
		Service:   primitives.HistoryService,
		Component: ComponentPersistence,
	}

	actual := source.overallCheckType("latency", 0.99)
	expected := "history.persistence.overall.latency.p99"

	require.Equal(t, expected, actual)
}

func TestGroupCheckType(t *testing.T) {
	source := Source{
		Service:   primitives.HistoryService,
		Component: ComponentGRPC,
	}

	actual := source.groupCheckType("critical", "latency", 0.99)
	expected := "history.grpc.group.critical.latency.p99"

	require.Equal(t, expected, actual)
}

func TestCheckType(t *testing.T) {
	source := Source{
		Service:   primitives.HistoryService,
		Component: ComponentGRPC,
	}

	testCases := []struct {
		desc      string
		scope     []string
		checkType string
		quantile  float64
		expected  string
	}{
		{
			desc:      "with quantile",
			scope:     []string{"group", "critical"},
			checkType: "latency",
			quantile:  0.999,
			expected:  "history.grpc.group.critical.latency.p99.9",
		},
		{
			// quantile of 0 means the check is not a quantile, so no suffix
			desc:      "without quantile",
			scope:     []string{"overall"},
			checkType: "error_ratio",
			quantile:  0,
			expected:  "history.grpc.overall.error_ratio",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			actual := source.checkType(tc.scope, tc.checkType, tc.quantile)

			require.Equal(t, tc.expected, actual)
		})
	}
}

func TestFormatQuantile(t *testing.T) {
	actual := formatQuantile(0.999)
	expected := "p99.9"

	require.Equal(t, expected, actual)
}
