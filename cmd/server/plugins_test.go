package main

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

func TestSQLitePlugin(t *testing.T) {
	t.Parallel()

	_, err := sql.GetPluginVisibilityQueryConverter(sqlplugin.PluginNameSQLite)
	require.NoError(t, err)
}
