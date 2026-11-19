package golangci

import (
	"testing"

	"github.com/golangci/plugin-module-register/register"
	"github.com/stretchr/testify/require"
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
