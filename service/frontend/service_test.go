package frontend

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfig_IsExperimentEnabled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		allowedExperiments []string
		experiment         string
		expected           bool
	}{
		{
			name:               "no experiments allowed - should not be enabled",
			allowedExperiments: []string{},
			experiment:         "chasm-scheduler",
			expected:           false,
		},
		{
			name:               "specific experiment allowed - exact match",
			allowedExperiments: []string{"chasm-scheduler"},
			experiment:         "chasm-scheduler",
			expected:           true,
		},
		{
			name:               "specific experiment allowed - no match",
			allowedExperiments: []string{"chasm-scheduler"},
			experiment:         "other-experiment",
			expected:           false,
		},
		{
			name:               "wildcard only - any experiment enabled",
			allowedExperiments: []string{"*"},
			experiment:         "chasm-scheduler",
			expected:           true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			config := &Config{
				AllowedExperiments: func(namespace string) []string {
					return tt.allowedExperiments
				},
			}

			result := config.IsExperimentEnabled(tt.experiment, "test-namespace")
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestConfig_IsExperimentEnabled_NamespaceSpecific(t *testing.T) {
	t.Parallel()

	// Test that different namespaces can have different allowed experiments
	config := &Config{
		AllowedExperiments: func(namespace string) []string {
			switch namespace {
			case "namespace-with-wildcard":
				return []string{"*"}
			case "namespace-with-specific":
				return []string{"chasm-scheduler"}
			case "namespace-with-none":
				return []string{}
			default:
				return nil
			}
		},
	}

	tests := []struct {
		name       string
		namespace  string
		experiment string
		expected   bool
	}{
		{
			name:       "wildcard namespace - any experiment",
			namespace:  "namespace-with-wildcard",
			experiment: "any-experiment",
			expected:   true,
		},
		{
			name:       "specific namespace - matching experiment",
			namespace:  "namespace-with-specific",
			experiment: "chasm-scheduler",
			expected:   true,
		},
		{
			name:       "specific namespace - non-matching experiment",
			namespace:  "namespace-with-specific",
			experiment: "other-experiment",
			expected:   false,
		},
		{
			name:       "empty namespace - any experiment",
			namespace:  "namespace-with-none",
			experiment: "chasm-scheduler",
			expected:   false,
		},
		{
			name:       "default namespace - any experiment",
			namespace:  "other-namespace",
			experiment: "chasm-scheduler",
			expected:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := config.IsExperimentEnabled(tt.experiment, tt.namespace)
			require.Equal(t, tt.expected, result)
		})
	}
}
