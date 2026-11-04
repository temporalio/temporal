//go:build !temporal_minimal || temporal_with_mysql

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

func TestMySQLPlugin(t *testing.T) {
	t.Parallel()

	_, err := sql.GetPluginVisibilityQueryConverter(sqlplugin.PluginNameMySQL)
	require.NoError(t, err)
}
