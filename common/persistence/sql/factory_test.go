package sql

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/resolver"
)

func TestSQLiteFactoryOwnsDatabaseUntilClose(t *testing.T) {
	closeCount := 0
	factory := NewFactory(
		config.SQL{PluginName: "sqlite"},
		resolver.NewNoopResolver(),
		"cluster",
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
		serialization.NewSerializer(),
	)
	factory.mainDBConn.DB = &factoryTestDB{close: func() error {
		closeCount++
		return nil
	}}
	db, err := factory.GetDB()
	require.NoError(t, err)
	require.NoError(t, db.Close())
	require.Equal(t, 0, closeCount)

	factory.Close()
	require.Equal(t, 1, closeCount)
}

type factoryTestDB struct {
	sqlplugin.DB
	close func() error
}

//nolint:staticcheck // Implements sqlplugin.GenericDB.DbName.
func (*factoryTestDB) DbName() string {
	return "factory-test"
}

func (*factoryTestDB) PluginName() string {
	return "factory-test"
}

func (d *factoryTestDB) Close() error {
	return d.close()
}
