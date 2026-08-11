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

func TestAcquireDatabaseLeaseClosesAcquiredLeasesOnFailure(t *testing.T) {
	const pluginName = "lease-test-rollback"
	firstCloseCount := 0
	provider := &leaseTestProvider{
		acquire: func(cfg *config.SQL) (func() error, error) {
			if cfg.DatabaseName == "second" {
				return nil, errors.New("second lease failed")
			}
			return func() error {
				firstCloseCount++
				return nil
			}, nil
		},
	}
	registerLeaseTestPlugin(t, pluginName, provider)
	cfg := config.Persistence{
		DefaultStore:    "a",
		VisibilityStore: "b",
		DataStores: map[string]config.DataStore{
			"a": {SQL: &config.SQL{PluginName: pluginName, DatabaseName: "first"}},
			"b": {SQL: &config.SQL{PluginName: pluginName, DatabaseName: "second"}},
			"c": {},
		},
	}

	release, err := AcquireDatabaseLease(cfg)
	require.EqualError(t, err, "second lease failed")
	require.Nil(t, release)
	require.Equal(t, 1, firstCloseCount)
}

func TestAcquireDatabaseLeaseIgnoresUnusedDataStores(t *testing.T) {
	const pluginName = "lease-test-active"
	registerLeaseTestPlugin(t, pluginName, leaseTestPlugin{})
	cfg := config.Persistence{
		DefaultStore: "active",
		DataStores: map[string]config.DataStore{
			"active": {SQL: &config.SQL{PluginName: pluginName}},
			"unused": {SQL: &config.SQL{PluginName: "not-registered"}},
		},
	}

	release, err := AcquireDatabaseLease(cfg)
	require.NoError(t, err)
	require.NoError(t, release())
}

func TestAcquireDatabaseLeaseClosesInReverseOrderAndJoinsErrors(t *testing.T) {
	const pluginName = "lease-test-close"
	firstErr := errors.New("first close failed")
	secondErr := errors.New("second close failed")
	var order []string
	provider := &leaseTestProvider{
		acquire: func(cfg *config.SQL) (func() error, error) {
			return func() error {
				order = append(order, cfg.DatabaseName)
				switch cfg.DatabaseName {
				case "first":
					return firstErr
				case "second":
					return secondErr
				default:
					return nil
				}
			}, nil
		},
	}
	registerLeaseTestPlugin(t, pluginName, provider)
	cfg := config.Persistence{
		DefaultStore:    "a",
		VisibilityStore: "b",
		DataStores: map[string]config.DataStore{
			"a": {SQL: &config.SQL{PluginName: pluginName, DatabaseName: "first"}},
			"b": {SQL: &config.SQL{PluginName: pluginName, DatabaseName: "second"}},
		},
	}

	release, err := AcquireDatabaseLease(cfg)
	require.NoError(t, err)

	err = release()
	require.ErrorIs(t, err, firstErr)
	require.ErrorIs(t, err, secondErr)
	require.Equal(t, []string{"second", "first"}, order)

	require.Equal(t, err, release())
	require.Equal(t, []string{"second", "first"}, order)
}

func registerLeaseTestPlugin(t *testing.T, name string, plugin sqlplugin.Plugin) {
	t.Helper()
	RegisterPlugin(name, plugin)
	t.Cleanup(func() {
		delete(supportedPlugins, name)
	})
}

type leaseTestPlugin struct{}

func (leaseTestPlugin) CreateDB(
	sqlplugin.DbKind,
	*config.SQL,
	resolver.ServiceResolver,
	log.Logger,
	metrics.Handler,
) (sqlplugin.GenericDB, error) {
	panic("not used")
}

func (leaseTestPlugin) GetVisibilityQueryConverter() sqlplugin.VisibilityQueryConverter {
	return nil
}

type leaseTestProvider struct {
	leaseTestPlugin
	acquire func(*config.SQL) (func() error, error)
}

func (p *leaseTestProvider) AcquireDatabaseLease(cfg *config.SQL) (func() error, error) {
	return p.acquire(cfg)
}
