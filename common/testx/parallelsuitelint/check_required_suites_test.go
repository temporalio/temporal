package parallelsuitelint

import (
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/analysis/analysistest"
)

func TestRequiredSuites(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		allow    string
		runners  string
		packages []string
	}{
		{
			name:     "default",
			packages: []string{"policy/required", "policy/testmain"},
		},
		{
			name:     "exact packages",
			allow:    "policy, policy/allowed, policy/neighbor",
			packages: []string{"policy/allowed", "policy/required", "policy/neighbor_test"},
		},
		{
			name:     "recursive packages",
			allow:    "policy/allowed/...",
			packages: []string{"policy/allowed", "policy/allowed/child", "policy/allowedmore"},
		},
		{
			name:     "all packages",
			allow:    "...",
			packages: []string{"policy/allowed", "policy/allowed/child"},
		},
		{
			name:     "trusted adapter",
			runners:  "other, adapter",
			packages: []string{"policy/adapted"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			analyzer := newAnalyzer()
			require.NoError(t, analyzer.Flags.Set("allow-standard-tests", tc.allow))
			require.NoError(t, analyzer.Flags.Set("runner-packages", tc.runners))
			analysistest.Run(t, analysistest.TestData(), analyzer, tc.packages...)
		})
	}
}
