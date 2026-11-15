package golangci

import (
	"path/filepath"
	"testing"

	"github.com/golangci/plugin-module-register/register"
	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/analysis/analysistest"
)

func TestPlugin(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		settings any
		packages []string
	}{
		{name: "defaults", packages: []string{"policy/required", "policy/adapted"}},
		{
			name:     "adoption exclusions preserve other checks",
			settings: map[string]any{"allow-standard-tests": []string{"policy/allowed/..."}},
			packages: []string{"policy/allowed", "policy/allowed/child"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			constructor, err := register.GetPlugin("parallelsuitelint")
			require.NoError(t, err)
			plugin, err := constructor(tc.settings)
			require.NoError(t, err)
			analyzers, err := plugin.BuildAnalyzers()
			require.NoError(t, err)
			require.Len(t, analyzers, 1)
			analysistest.Run(t, filepath.Join(analysistest.TestData(), "..", "..", "testdata"), analyzers[0], tc.packages...)
		})
	}
}

func TestInvalidSettings(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		settings any
	}{
		{name: "unknown setting", settings: map[string]any{"typo": true}},
		{name: "wrong value type", settings: map[string]any{"allow-standard-tests": true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			constructor, err := register.GetPlugin("parallelsuitelint")
			require.NoError(t, err)
			_, err = constructor(tc.settings)
			require.Error(t, err)
		})
	}
}
