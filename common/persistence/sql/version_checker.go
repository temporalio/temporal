package sql

import (
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/resolver"
)

// VerifyCompatibleVersion ensures that the installed version of temporal and visibility
// is greater than or equal to the expected version.
func VerifyCompatibleVersion(
	cfg config.Persistence,
	r resolver.ServiceResolver,
) error {

	if err := checkMainDatabase(cfg, r); err != nil {
		return err
	}
	if cfg.VisibilityConfigExist() {
		return checkVisibilityDatabase(cfg, r)
	}
	return nil
}

func checkMainDatabase(
	cfg config.Persistence,
	r resolver.ServiceResolver,
) error {
	ds, ok := cfg.DataStores[cfg.DefaultStore]
	if ok && ds.SQL != nil {
		return checkCompatibleVersion(ds.SQL, r, sqlplugin.DbKindMain)
	}
	return nil
}

func checkVisibilityDatabase(
	cfg config.Persistence,
	r resolver.ServiceResolver,
) error {
	ds, ok := cfg.DataStores[cfg.VisibilityStore]
	if ok && ds.SQL != nil {
		return checkCompatibleVersion(ds.SQL, r, sqlplugin.DbKindVisibility)
	}
	return nil
}

func checkCompatibleVersion(
	cfg *config.SQL,
	r resolver.ServiceResolver,
	dbKind sqlplugin.DbKind,
) error {
	db, err := NewSQLAdminDB(dbKind, cfg, r, log.NewNoopLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	return db.VerifyVersion()
}
