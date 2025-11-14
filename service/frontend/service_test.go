package frontend

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfig_IsExperimentAllowed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		namespace  string
		experiment string
		setupFunc  func(namespace string) []string
		expected   bool
	}{
		{
			name:       "no experiments allowed",
			namespace:  "test-namespace",
			experiment: "chasm-scheduler",
			setupFunc:  func(ns string) []string { return []string{} },
			expected:   false,
		},
		{
			name:       "specific experiment match",
			namespace:  "test-namespace",
			experiment: "chasm-scheduler",
			setupFunc:  func(ns string) []string { return []string{"chasm-scheduler"} },
			expected:   true,
		},
		{
			name:       "specific experiment no match",
			namespace:  "test-namespace",
			experiment: "other-experiment",
			setupFunc:  func(ns string) []string { return []string{"chasm-scheduler"} },
			expected:   false,
		},
		{
			name:       "wildcard allows any",
			namespace:  "test-namespace",
			experiment: "any-experiment",
			setupFunc:  func(ns string) []string { return []string{"*"} },
			expected:   true,
		},
		{
			name:       "namespace specific wildcard",
			namespace:  "ns-with-wildcard",
			experiment: "any-experiment",
			setupFunc: func(ns string) []string {
				if ns == "ns-with-wildcard" {
					return []string{"*"}
				}
				return nil
			},
			expected: true,
		},
		{
			name:       "namespace specific match",
			namespace:  "ns-with-specific",
			experiment: "chasm-scheduler",
			setupFunc: func(ns string) []string {
				if ns == "ns-with-specific" {
					return []string{"chasm-scheduler"}
				}
				return nil
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			config := &Config{
				AllowedExperiments: tt.setupFunc,
			}

			result := config.IsExperimentAllowed(tt.experiment, tt.namespace)
			require.Equal(t, tt.expected, result)
		})
	}
}
