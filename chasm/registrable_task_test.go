package chasm

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTaskCountMetricEnabledFor(t *testing.T) {
	enabledComponent := newTestRegistrableComponent(WithTaskCountMetric())
	disabledComponent := newTestRegistrableComponent()

	testCases := []struct {
		name      string
		component *RegistrableComponent
		task      *RegistrableTask
		expected  bool
	}{
		{
			name:      "inherits enabled component",
			component: enabledComponent,
			task:      newTestRegistrableTask(),
			expected:  true,
		},
		{
			name:      "inherits disabled component",
			component: disabledComponent,
			task:      newTestRegistrableTask(),
			expected:  false,
		},
		{
			name:      "task opts out of enabled component",
			component: enabledComponent,
			task:      newTestRegistrableTask(WithTaskCountMetricOverride(false)),
			expected:  false,
		},
		{
			name:      "task opts into disabled component",
			component: disabledComponent,
			task:      newTestRegistrableTask(WithTaskCountMetricOverride(true)),
			expected:  true,
		},
		{
			name:      "unregistered component",
			component: nil,
			task:      newTestRegistrableTask(),
			expected:  false,
		},
		{
			name:      "unregistered component with task override",
			component: nil,
			task:      newTestRegistrableTask(WithTaskCountMetricOverride(true)),
			expected:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, tc.task.taskCountMetricEnabledFor(tc.component))
		})
	}
}

func newTestRegistrableComponent(opts ...RegistrableComponentOption) *RegistrableComponent {
	rc := &RegistrableComponent{}
	for _, opt := range opts {
		opt(rc)
	}
	return rc
}

func newTestRegistrableTask(opts ...RegistrableTaskOption) *RegistrableTask {
	rt := &RegistrableTask{}
	for _, opt := range opts {
		opt(rt)
	}
	return rt
}
