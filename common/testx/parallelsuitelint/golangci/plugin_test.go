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

	constructor, err := register.GetPlugin("parallelsuitelint")
	require.NoError(t, err)
	plugin, err := constructor(nil)
	require.NoError(t, err)
	require.Equal(t, register.LoadModeTypesInfo, plugin.GetLoadMode())

	analyzers, err := plugin.BuildAnalyzers()
	require.NoError(t, err)
	require.Len(t, analyzers, 1)
	require.Equal(t, "parallelsuitelint", analyzers[0].Name)
}

func TestPluginRecognizesConfiguredAwaitPackage(t *testing.T) {
	t.Parallel()

	plugin, err := newPlugin(map[string]any{"await-packages": []string{"example.com/await"}})
	require.NoError(t, err)
	analyzers, err := plugin.BuildAnalyzers()
	require.NoError(t, err)
	require.Len(t, analyzers, 1)

	testdata, err := filepath.Abs("../testdata")
	require.NoError(t, err)
	analysistest.Run(t, testdata, analyzers[0], "configuredawait")
}
