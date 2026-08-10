package temporalite

import (
	"context"
	gosql "database/sql"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/temporal"
)

func TestNewLiteServerLeasePreservesPrecreatedNamespaces(t *testing.T) {
	const namespace = "lease-test"
	cfg := &LiteServerConfig{
		Ephemeral:  true,
		FrontendIP: "127.0.0.1",
		Namespaces: []string{namespace},
	}
	server, err := NewLiteServer(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Stop()) })

	sqlCfg := cfg.BaseConfig.Persistence.DataStores[cfg.BaseConfig.Persistence.DefaultStore].SQL
	db, err := sql.NewSQLDB(
		sqlplugin.DbKindMain,
		sqlCfg,
		resolver.NewNoopResolver(),
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	namespaceName := namespace
	rows, err := db.SelectFromNamespace(
		context.Background(),
		sqlplugin.NamespaceFilter{Name: &namespaceName},
	)
	require.NoError(t, err)
	require.Len(t, rows, 1)
}

func TestNewLiteServerReleasesDatabaseLeaseOnConstructionPanic(t *testing.T) {
	const (
		expectedPanic = "construction panicked"
		namespace     = "lease-test"
	)
	cfg := &LiteServerConfig{
		Ephemeral:  true,
		FrontendIP: "127.0.0.1",
		Namespaces: []string{namespace},
	}

	func() {
		defer func() {
			require.Equal(t, expectedPanic, recover())
		}()
		_, _ = NewLiteServer(
			cfg,
			temporal.WithClaimMapper(func(*config.Config) authorization.ClaimMapper {
				panic(expectedPanic)
			}),
		)
	}()

	sqlCfg := cfg.BaseConfig.Persistence.DataStores[cfg.BaseConfig.Persistence.DefaultStore].SQL
	db, err := sql.NewSQLDB(
		sqlplugin.DbKindMain,
		sqlCfg,
		resolver.NewNoopResolver(),
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	namespaceName := namespace
	rows, err := db.SelectFromNamespace(
		context.Background(),
		sqlplugin.NamespaceFilter{Name: &namespaceName},
	)
	require.ErrorIs(t, err, gosql.ErrNoRows)
	require.Empty(t, rows)
}
