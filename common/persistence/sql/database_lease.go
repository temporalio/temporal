package sql

import (
	"errors"
	"slices"
	"sync"

	"go.temporal.io/server/common/config"
)

type databaseLeaseProvider interface {
	AcquireDatabaseLease(cfg *config.SQL) (func() error, error)
}

// AcquireDatabaseLease keeps every active SQL database available until the returned idempotent release
// function is called.
// Non-SQL stores and SQL plugins without database lease support are ignored.
func AcquireDatabaseLease(cfg config.Persistence) (func() error, error) {
	var releases []func() error
	names := [...]string{cfg.DefaultStore, cfg.VisibilityStore, cfg.SecondaryVisibilityStore}
	for i, name := range names {
		if name == "" || slices.Contains(names[:i], name) {
			continue
		}

		sqlCfg := cfg.DataStores[name].SQL
		if sqlCfg == nil {
			continue
		}
		plugin, err := getPlugin(sqlCfg.PluginName)
		if err != nil {
			return nil, errors.Join(err, releaseDatabaseLeases(releases))
		}
		provider, ok := plugin.(databaseLeaseProvider)
		if !ok {
			continue
		}
		release, err := provider.AcquireDatabaseLease(sqlCfg)
		if err != nil {
			return nil, errors.Join(err, releaseDatabaseLeases(releases))
		}
		releases = append(releases, release)
	}
	return sync.OnceValue(func() error {
		return releaseDatabaseLeases(releases)
	}), nil
}

func releaseDatabaseLeases(releases []func() error) error {
	var err error
	for _, release := range slices.Backward(releases) {
		err = errors.Join(err, release())
	}
	return err
}
