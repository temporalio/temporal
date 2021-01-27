package cassandra

import (
	"context"

	"golang.org/x/sync/errgroup"

	"go.temporal.io/server/common/persistence/schema"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/service/config"
	cassandraschema "go.temporal.io/server/schema/cassandra"
)

// VerifyCompatibleVersion ensures that the installed version of temporal and visibility keyspaces
// is greater than or equal to the expected version.
// In most cases, the versions should match. However if after a schema upgrade there is a code
// rollback, the code version (expected version) would fall lower than the actual version in
// cassandra.
func VerifyCompatibleVersion(
	cfg config.Persistence,
	r resolver.ServiceResolver,
) error {
	g, _ := errgroup.WithContext(context.Background())
	g.Go(func() error {
		return checkTemporalKeyspace(cfg, r)
	})
	g.Go(func() error {
		return checkVisibilityKeyspace(cfg, r)
	})

	return g.Wait()
}

func checkTemporalKeyspace(
	cfg config.Persistence,
	r resolver.ServiceResolver,
) error {
	ds, ok := cfg.DataStores[cfg.DefaultStore]
	if ok && ds.Cassandra != nil {
		err := checkCompatibleVersion(*ds.Cassandra, r, cassandraschema.Version)
		if err != nil {
			return err
		}
	}

	return nil
}

func checkVisibilityKeyspace(
	cfg config.Persistence,
	r resolver.ServiceResolver,
) error {
	ds, ok := cfg.DataStores[cfg.VisibilityStore]
	if ok && ds.Cassandra != nil {
		err := checkCompatibleVersion(*ds.Cassandra, r, cassandraschema.VisibilityVersion)
		if err != nil {
			return err
		}
	}

	return nil
}

// checkCompatibleVersion check the version compatibility
func checkCompatibleVersion(
	cfg config.Cassandra,
	r resolver.ServiceResolver,
	expectedVersion string,
) error {

	session, err := NewSession(cfg, r)
	if err != nil {
		return err
	}
	defer session.Close()

	return schema.VerifyCompatibleVersion(NewSchemaVersionReader(session, cfg.Keyspace), cfg.Keyspace, expectedVersion)
}
