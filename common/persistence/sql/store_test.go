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

func TestAcquireDatabaseLeasesClosesAcquiredLeasesOnFailure(t *testing.T) {
	const pluginName = "lease-test-rollback"
	firstLease := &leaseTestLease{}
	provider := &leaseTestProvider{
		acquire: func(cfg *config.SQL) (sqlplugin.DatabaseLease, error) {
			if cfg.DatabaseName == "second" {
				return nil, errors.New("second lease failed")
			}
			return firstLease, nil
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

	lease, err := AcquireDatabaseLeases(cfg)
	require.EqualError(t, err, "second lease failed")
	require.Nil(t, lease)
	require.Equal(t, 1, firstLease.closeCount)
}

func TestAcquireDatabaseLeasesIgnoresUnusedDataStores(t *testing.T) {
	const pluginName = "lease-test-active"
	registerLeaseTestPlugin(t, pluginName, leaseTestPlugin{})
	cfg := config.Persistence{
		DefaultStore: "active",
		DataStores: map[string]config.DataStore{
			"active": {SQL: &config.SQL{PluginName: pluginName}},
			"unused": {SQL: &config.SQL{PluginName: "not-registered"}},
		},
	}

	lease, err := AcquireDatabaseLeases(cfg)
	require.NoError(t, err)
	require.NoError(t, lease.Close())
}

func TestDatabaseLeasesCloseInReverseOrderAndJoinErrors(t *testing.T) {
	firstErr := errors.New("first close failed")
	secondErr := errors.New("second close failed")
	var order []string
	leases := &databaseLeases{
		leases: []sqlplugin.DatabaseLease{
			&recordingLease{name: "first", order: &order, err: firstErr},
			&recordingLease{name: "second", order: &order, err: secondErr},
		},
	}

	err := leases.Close()
	require.ErrorIs(t, err, firstErr)
	require.ErrorIs(t, err, secondErr)
	require.Equal(t, []string{"second", "first"}, order)

	require.Equal(t, err, leases.Close())
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
	acquire func(*config.SQL) (sqlplugin.DatabaseLease, error)
}

func (p *leaseTestProvider) AcquireDatabaseLease(cfg *config.SQL) (sqlplugin.DatabaseLease, error) {
	return p.acquire(cfg)
}

type leaseTestLease struct {
	closeCount int
}

func (l *leaseTestLease) Close() error {
	l.closeCount++
	return nil
}

type recordingLease struct {
	name  string
	order *[]string
	err   error
}

func (l *recordingLease) Close() error {
	*l.order = append(*l.order, l.name)
	return l.err
}
