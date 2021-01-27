package sql

import (
	"go.temporal.io/server/common/persistence/schema"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/service/config"
)

// VerifyCompatibleVersion ensures that the installed version of temporal and visibility
// is greater than or equal to the expected version.
func VerifyCompatibleVersion(
	cfg config.Persistence,
	r resolver.ServiceResolver,
) error {

	ds, ok := cfg.DataStores[cfg.DefaultStore]
	if ok && ds.SQL != nil {
		db, err := NewSQLAdminDB(ds.SQL, r)
		if err != nil {
			return err
		}
		defer func() { _ = db.Close() }()

		err = schema.VerifyCompatibleVersion(db, ds.SQL.DatabaseName, db.ExpectedVersion())
		if err != nil {
			return err
		}
	}
	ds, ok = cfg.DataStores[cfg.VisibilityStore]
	if ok && ds.SQL != nil {
		db, err := NewSQLAdminDB(ds.SQL, r)
		if err != nil {
			return err
		}
		defer func() { _ = db.Close() }()

		err = schema.VerifyCompatibleVersion(db, ds.SQL.DatabaseName, db.ExpectedVisibilityVersion())
		if err != nil {
			return err
		}
	}
	return nil
}
