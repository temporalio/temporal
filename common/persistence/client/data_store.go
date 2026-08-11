package client

import (
	"errors"
	"io"
	"slices"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/resolver"
)

// OpenDataStores opens the active persistence data stores. The caller owns the returned handle.
func OpenDataStores(
	cfg config.Persistence,
	r resolver.ServiceResolver,
	logger log.Logger,
	mh metrics.Handler,
) (io.Closer, error) {
	stores := [...]struct {
		name string
		kind sqlplugin.DbKind
	}{
		{cfg.DefaultStore, sqlplugin.DbKindMain},
		{cfg.VisibilityStore, sqlplugin.DbKindVisibility},
		{cfg.SecondaryVisibilityStore, sqlplugin.DbKindVisibility},
	}

	dataStores := make(dataStoreHandles, 0, len(stores))
	seen := make(map[string]struct{}, len(stores))
	for _, store := range stores {
		if store.name == "" {
			continue
		}
		if _, ok := seen[store.name]; ok {
			continue
		}
		seen[store.name] = struct{}{}

		sqlCfg := cfg.DataStores[store.name].SQL
		if sqlCfg == nil {
			continue
		}
		db, err := sql.NewSQLDB(store.kind, sqlCfg, r, logger, mh)
		if err != nil {
			return nil, errors.Join(err, dataStores.Close())
		}
		dataStores = append(dataStores, db)
	}
	return dataStores, nil
}

type dataStoreHandles []io.Closer

func (d dataStoreHandles) Close() error {
	var err error
	for _, dataStore := range slices.Backward(d) {
		err = errors.Join(err, dataStore.Close())
	}
	return err
}
