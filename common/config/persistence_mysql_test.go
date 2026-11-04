//go:build !temporal_minimal || temporal_with_mysql

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDataStoreValidateMySQL(t *testing.T) {
	t.Parallel()

	ds := &DataStore{SQL: &SQL{PluginName: "mysql8"}}
	require.NoError(t, ds.Validate())
}
