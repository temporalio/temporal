package config

import "fmt"

const (
	// StoreTypeSQL refers to sql based storage as persistence store
	StoreTypeSQL = "sql"
	// StoreTypeCassandra refers to cassandra as persistence store
	StoreTypeCassandra = "cassandra"
)

// DefaultStoreType returns the storeType for the default persistence store
func (c *Persistence) DefaultStoreType() string {
	if c.DataStores[c.DefaultStore].SQL != nil {
		return StoreTypeSQL
	}
	return StoreTypeCassandra
}

// Validate validates the persistence config
func (c *Persistence) Validate() error {
	stores := []string{c.DefaultStore, c.VisibilityStore}
	for _, st := range stores {
		ds, ok := c.DataStores[st]
		if !ok {
			return fmt.Errorf("persistence config: missing config for datastore %v", st)
		}
		if ds.SQL == nil && ds.Cassandra == nil {
			return fmt.Errorf("persistence config: datastore %v: must provide config for one of cassandra or sql stores", st)
		}
		if ds.SQL != nil && ds.Cassandra != nil {
			return fmt.Errorf("persistence config: datastore %v: only one of SQL or cassandra can be specified", st)
		}
		if ds.SQL != nil && ds.SQL.NumShards == 0 {
			ds.SQL.NumShards = 1
		}
	}
	return nil
}

// IsAdvancedVisibilityConfigExist returns whether user specified advancedVisibilityStore in config
func (c *Persistence) IsAdvancedVisibilityConfigExist() bool {
	return len(c.AdvancedVisibilityStore) != 0
}
