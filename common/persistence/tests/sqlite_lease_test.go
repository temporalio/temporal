package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/resolver"
	sqliteschema "go.temporal.io/server/schema/sqlite"
)

func TestSQLiteDatabaseLeasePreservesNamespaceAcrossWrapperChurn(t *testing.T) {
	const namespace = "lease-test"
	cfg := NewSQLiteMemoryConfig()
	lease, err := sql.AcquireDatabaseLease(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, lease.Close()) })

	namespaceConfig, err := sqliteschema.NewNamespaceConfig("active", namespace, false, nil)
	require.NoError(t, err)
	require.NoError(t, sqliteschema.CreateNamespaces(cfg, namespaceConfig))

	for range 3 {
		factory := sql.NewFactory(
			*cfg,
			resolver.NewNoopResolver(),
			testSQLiteClusterName,
			log.NewNoopLogger(),
			metrics.NoopMetricsHandler,
			serialization.NewSerializer(),
		)
		db, err := factory.GetDB()
		require.NoError(t, err)
		namespaceName := namespace
		rows, err := db.SelectFromNamespace(
			context.Background(),
			sqlplugin.NamespaceFilter{Name: &namespaceName},
		)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.NoError(t, db.Close())
		factory.Close()
	}
}
