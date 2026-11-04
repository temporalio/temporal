//go:build !temporal_minimal || temporal_with_postgresql

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDataStoreValidatePostgreSQL(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"postgres12", "postgres12_pgx"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ds := &DataStore{SQL: &SQL{PluginName: name}}
			require.NoError(t, ds.Validate())
		})
	}
}
