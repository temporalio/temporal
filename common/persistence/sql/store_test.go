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

func TestAcquireDatabaseLeaseReturnsNoopForPluginWithoutProvider(t *testing.T) {
	const pluginName = "lease-test-noop"
	registerLeaseTestPlugin(t, pluginName, leaseTestPlugin{})

	lease, err := AcquireDatabaseLease(&config.SQL{PluginName: pluginName})
	require.NoError(t, err)
	require.NoError(t, lease.Close())
	require.NoError(t, lease.Close())
}

func TestAcquireDatabaseLeaseUsesPluginProvider(t *testing.T) {
	const pluginName = "lease-test-provider"
	wantLease := &leaseTestLease{}
	provider := &leaseTestProvider{lease: wantLease}
	registerLeaseTestPlugin(t, pluginName, provider)
	cfg := &config.SQL{PluginName: pluginName}

	lease, err := AcquireDatabaseLease(cfg)
	require.NoError(t, err)
	require.Same(t, cfg, provider.cfg)
	require.Same(t, wantLease, lease)
}

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
	lease   sqlplugin.DatabaseLease
	cfg     *config.SQL
	acquire func(*config.SQL) (sqlplugin.DatabaseLease, error)
}

func (p *leaseTestProvider) CreateDB(
	sqlplugin.DbKind,
	*config.SQL,
	resolver.ServiceResolver,
	log.Logger,
	metrics.Handler,
) (sqlplugin.GenericDB, error) {
	panic("not used")
}

func (p *leaseTestProvider) GetVisibilityQueryConverter() sqlplugin.VisibilityQueryConverter {
	return nil
}

func (p *leaseTestProvider) AcquireDatabaseLease(cfg *config.SQL) (sqlplugin.DatabaseLease, error) {
	p.cfg = cfg
	if p.acquire != nil {
		return p.acquire(cfg)
	}
	return p.lease, nil
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
