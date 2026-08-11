package sql

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/resolver"
)

func TestOpenDatabasesClosesOpenedDatabasesOnFailure(t *testing.T) {
	const pluginName = "database-test-rollback"
	firstCloseCount := 0
	plugin := &databaseTestPlugin{
		create: func(cfg *config.SQL) (sqlplugin.GenericDB, error) {
			if cfg.DatabaseName == "second" {
				return nil, errors.New("second database failed")
			}
			return &databaseTestDB{close: func() error {
				firstCloseCount++
				return nil
			}}, nil
		},
	}
	registerDatabaseTestPlugin(t, pluginName, plugin)
	cfg := config.Persistence{
		DefaultStore:    "a",
		VisibilityStore: "b",
		DataStores: map[string]config.DataStore{
			"a": {SQL: &config.SQL{PluginName: pluginName, DatabaseName: "first"}},
			"b": {SQL: &config.SQL{PluginName: pluginName, DatabaseName: "second"}},
			"c": {},
		},
	}

	databases, err := OpenDatabases(
		cfg,
		resolver.NewNoopResolver(),
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
	)
	require.EqualError(t, err, "second database failed")
	require.Nil(t, databases)
	require.Equal(t, 1, firstCloseCount)
}

func TestCloseDatabasesClosesInReverseOrderAndJoinsErrors(t *testing.T) {
	firstErr := errors.New("first close failed")
	secondErr := errors.New("second close failed")
	var order []string
	databases := []sqlplugin.GenericDB{
		&databaseTestDB{name: "first", close: func() error {
			order = append(order, "first")
			return firstErr
		}},
		&databaseTestDB{name: "second", close: func() error {
			order = append(order, "second")
			return secondErr
		}},
	}

	err := CloseDatabases(databases)
	require.ErrorIs(t, err, firstErr)
	require.ErrorIs(t, err, secondErr)
	require.Equal(t, []string{"second", "first"}, order)
}

func registerDatabaseTestPlugin(t *testing.T, name string, plugin sqlplugin.Plugin) {
	t.Helper()
	RegisterPlugin(name, plugin)
	t.Cleanup(func() {
		delete(supportedPlugins, name)
	})
}

type databaseTestPlugin struct {
	create func(*config.SQL) (sqlplugin.GenericDB, error)
}

func (p *databaseTestPlugin) CreateDB(
	_ sqlplugin.DbKind,
	cfg *config.SQL,
	_ resolver.ServiceResolver,
	_ log.Logger,
	_ metrics.Handler,
) (sqlplugin.GenericDB, error) {
	return p.create(cfg)
}

func (*databaseTestPlugin) GetVisibilityQueryConverter() sqlplugin.VisibilityQueryConverter {
	return nil
}

type databaseTestDB struct {
	name  string
	close func() error
}

//nolint:staticcheck // Implements sqlplugin.GenericDB.DbName.
func (d *databaseTestDB) DbName() string {
	return d.name
}

func (*databaseTestDB) PluginName() string {
	return "database-test"
}

func (d *databaseTestDB) Close() error {
	return d.close()
}
