package client_test

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/resolver"
)

func TestOpenDataStoresClosesOpenedDatabasesOnFailure(t *testing.T) {
	firstCloseCount := 0
	pluginName := registerDataStoreTestPlugin(&dataStoreTestPlugin{
		create: func(cfg *config.SQL) (sqlplugin.GenericDB, error) {
			if cfg.DatabaseName == "second" {
				return nil, errors.New("second database failed")
			}
			return &dataStoreTestDB{close: func() error {
				firstCloseCount++
				return nil
			}}, nil
		},
	})
	cfg := config.Persistence{
		DefaultStore:    "a",
		VisibilityStore: "b",
		DataStores: map[string]config.DataStore{
			"a": {SQL: &config.SQL{PluginName: pluginName, DatabaseName: "first"}},
			"b": {SQL: &config.SQL{PluginName: pluginName, DatabaseName: "second"}},
			"c": {},
		},
	}

	dataStores, err := client.OpenDataStores(
		cfg,
		resolver.NewNoopResolver(),
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
	)
	require.EqualError(t, err, "second database failed")
	require.Nil(t, dataStores)
	require.Equal(t, 1, firstCloseCount)
}

func TestOpenDataStoresClosesDatabasesInReverseOrderAndJoinsErrors(t *testing.T) {
	firstErr := errors.New("first close failed")
	secondErr := errors.New("second close failed")
	var order []string
	pluginName := registerDataStoreTestPlugin(&dataStoreTestPlugin{
		create: func(cfg *config.SQL) (sqlplugin.GenericDB, error) {
			return &dataStoreTestDB{name: cfg.DatabaseName, close: func() error {
				order = append(order, cfg.DatabaseName)
				switch cfg.DatabaseName {
				case "first":
					return firstErr
				case "second":
					return secondErr
				default:
					return nil
				}
			}}, nil
		},
	})
	cfg := config.Persistence{
		DefaultStore:    "a",
		VisibilityStore: "b",
		DataStores: map[string]config.DataStore{
			"a": {SQL: &config.SQL{PluginName: pluginName, DatabaseName: "first"}},
			"b": {SQL: &config.SQL{PluginName: pluginName, DatabaseName: "second"}},
		},
	}

	dataStores, err := client.OpenDataStores(
		cfg,
		resolver.NewNoopResolver(),
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
	)
	require.NoError(t, err)

	err = dataStores.Close()
	require.ErrorIs(t, err, firstErr)
	require.ErrorIs(t, err, secondErr)
	require.Equal(t, []string{"second", "first"}, order)
}

func TestOpenDataStoresSupportsNonSQLDataStores(t *testing.T) {
	dataStores, err := client.OpenDataStores(
		config.Persistence{
			DefaultStore: "cassandra",
			DataStores: map[string]config.DataStore{
				"cassandra": {Cassandra: &config.Cassandra{}},
			},
		},
		resolver.NewNoopResolver(),
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
	)
	require.NoError(t, err)
	require.NotNil(t, dataStores)
	require.NoError(t, dataStores.Close())
}

type dataStoreTestPlugin struct {
	create func(*config.SQL) (sqlplugin.GenericDB, error)
}

var dataStoreTestPluginID atomic.Uint64

func registerDataStoreTestPlugin(plugin sqlplugin.Plugin) string {
	name := fmt.Sprintf("data-store-test-%d", dataStoreTestPluginID.Add(1))
	sql.RegisterPlugin(name, plugin)
	return name
}

func (p *dataStoreTestPlugin) CreateDB(
	_ sqlplugin.DbKind,
	cfg *config.SQL,
	_ resolver.ServiceResolver,
	_ log.Logger,
	_ metrics.Handler,
) (sqlplugin.GenericDB, error) {
	return p.create(cfg)
}

func (*dataStoreTestPlugin) GetVisibilityQueryConverter() sqlplugin.VisibilityQueryConverter {
	return nil
}

type dataStoreTestDB struct {
	sqlplugin.DB
	name  string
	close func() error
}

//nolint:staticcheck // Implements sqlplugin.GenericDB.DbName.
func (d *dataStoreTestDB) DbName() string {
	return d.name
}

func (*dataStoreTestDB) PluginName() string {
	return "data-store-test"
}

func (d *dataStoreTestDB) Close() error {
	return d.close()
}
