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

func TestFactoryOwnsDatabaseUntilClose(t *testing.T) {
	const pluginName = "factory-ownership-test"
	closeCount := 0
	RegisterPlugin(pluginName, &factoryTestPlugin{
		db: &factoryTestDB{close: func() error {
			closeCount++
			return nil
		}},
	})
	t.Cleanup(func() {
		delete(supportedPlugins, pluginName)
	})

	factory := NewFactory(
		config.SQL{PluginName: pluginName},
		resolver.NewNoopResolver(),
		"cluster",
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
		serialization.NewSerializer(),
	)
	db, err := factory.GetDB()
	require.NoError(t, err)
	require.NoError(t, db.Close())
	require.Equal(t, 0, closeCount)

	factory.Close()
	require.Equal(t, 1, closeCount)
}

type factoryTestPlugin struct {
	db sqlplugin.GenericDB
}

func (p *factoryTestPlugin) CreateDB(
	_ sqlplugin.DbKind,
	_ *config.SQL,
	_ resolver.ServiceResolver,
	_ log.Logger,
	_ metrics.Handler,
) (sqlplugin.GenericDB, error) {
	return p.db, nil
}

func (*factoryTestPlugin) GetVisibilityQueryConverter() sqlplugin.VisibilityQueryConverter {
	return nil
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
