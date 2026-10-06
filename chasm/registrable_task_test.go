package chasm

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveTaskCountMetricThreshold(t *testing.T) {
	testCases := []struct {
		name                   string
		task                   *RegistrableTask
		dynamicConfigThreshold int
		expected               int
	}{
		{
			name:     "framework default",
			task:     newTestRegistrableTask(WithTaskCountMetric(0)),
			expected: defaultTaskCountMetricThreshold,
		},
		{
			name:     "registered threshold",
			task:     newTestRegistrableTask(WithTaskCountMetric(50)),
			expected: 50,
		},
		{
			name:     "non-positive registered threshold falls back to default",
			task:     newTestRegistrableTask(WithTaskCountMetric(-1)),
			expected: defaultTaskCountMetricThreshold,
		},
		{
			name:                   "dynamic config overrides default",
			task:                   newTestRegistrableTask(WithTaskCountMetric(0)),
			dynamicConfigThreshold: 20,
			expected:               20,
		},
		{
			name:                   "dynamic config overrides registered threshold",
			task:                   newTestRegistrableTask(WithTaskCountMetric(50)),
			dynamicConfigThreshold: 20,
			expected:               20,
		},
		{
			name:                   "negative dynamic config disables",
			task:                   newTestRegistrableTask(WithTaskCountMetric(50)),
			dynamicConfigThreshold: -1,
			expected:               -1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, tc.task.resolveTaskCountMetricThreshold(tc.dynamicConfigThreshold))
		})
	}
}

func newTestRegistrableTask(opts ...RegistrableTaskOption) *RegistrableTask {
	rt := &RegistrableTask{}
	for _, opt := range opts {
		opt(rt)
	}
	return rt
}
