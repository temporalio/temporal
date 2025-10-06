package config

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/gocql/gocql"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
)

const (
	// StoreTypeSQL refers to sql based storage as persistence store
	StoreTypeSQL = "sql"
	// StoreTypeNoSQL refers to nosql based storage as persistence store
	StoreTypeNoSQL = "nosql"
)

var ErrPersistenceConfig = errors.New("persistence config error")

// DefaultStoreType returns the storeType for the default persistence store
func (c *Persistence) DefaultStoreType() string {
	if c.DataStores[c.DefaultStore].SQL != nil {
		return StoreTypeSQL
	}
	return StoreTypeNoSQL
}

// Validate validates the persistence config
func (c *Persistence) Validate() error {
	stores := []string{c.DefaultStore}
	if c.VisibilityStore != "" {
		stores = append(stores, c.VisibilityStore)
	}
	if c.SecondaryVisibilityStore != "" {
		stores = append(stores, c.SecondaryVisibilityStore)
	}

	// There are 3 config keys:
	// - visibilityStore: can set any data store
	// - secondaryVisibilityStore: can set any data store
	// If visibilityStore is set, then it's always the primary.
	// If secondaryVisibilityStore is set, it's always the secondary.
	//
	// Valid dual visibility combinations (order: primary, secondary):
	// - visibilityStore (advanced sql),  secondaryVisibilityStore (advanced sql)
	// - visibilityStore (es),            visibilityStore (es) [via elasticsearch.indices config]
	// - visibilityStore (es),            secondaryVisibilityStore (es)
	//
	// Invalid dual visibility combinations:
	// - visibilityStore (advanced sql),  secondaryVisibilityStore (es)
	// - visibilityStore (es),            secondaryVisibilityStore (advanced sql)

	if c.VisibilityStore == "" {
		return fmt.Errorf("%w: visibilityStore must be specified", ErrPersistenceConfig)
	}
	if c.SecondaryVisibilityStore != "" {
		isAnyCustom := c.DataStores[c.VisibilityStore].CustomDataStoreConfig != nil ||
			c.DataStores[c.SecondaryVisibilityStore].CustomDataStoreConfig != nil
		isPrimaryEs := c.DataStores[c.VisibilityStore].Elasticsearch != nil
		isSecondaryEs := c.DataStores[c.SecondaryVisibilityStore].Elasticsearch != nil
		if !isAnyCustom && isPrimaryEs != isSecondaryEs {
			return fmt.Errorf(
				"%w: cannot set visibilityStore and secondaryVisibilityStore with different datastore types",
				ErrPersistenceConfig)
		}
		if c.DataStores[c.VisibilityStore].Elasticsearch.GetSecondaryVisibilityIndex() != "" {
			return fmt.Errorf(
				"%w: cannot set secondaryVisibilityStore "+
					"when visibilityStore is setting Elasticsearch secondary visibility index",
				ErrPersistenceConfig)
		}
		if c.DataStores[c.SecondaryVisibilityStore].Elasticsearch.GetSecondaryVisibilityIndex() != "" {
			return fmt.Errorf(
				"%w: secondary visibility datastore %q cannot set secondary_visibility",
				ErrPersistenceConfig,
				c.SecondaryVisibilityStore)
		}
	}

	for _, st := range stores {
		ds, ok := c.DataStores[st]
		if !ok {
			return fmt.Errorf("%w: missing config for datastore %q", ErrPersistenceConfig, st)
		}
		if err := ds.Validate(); err != nil {
			return fmt.Errorf("%w: datastore %q: %s", ErrPersistenceConfig, st, err.Error())
		}
	}
	return nil
}

// VisibilityConfigExist returns whether user specified visibilityStore in config
func (c *Persistence) VisibilityConfigExist() bool {
	return c.VisibilityStore != ""
}

// SecondaryVisibilityConfigExist returns whether user specified secondaryVisibilityStore in config
func (c *Persistence) SecondaryVisibilityConfigExist() bool {
	return c.SecondaryVisibilityStore != ""
}

func (c *Persistence) IsSQLVisibilityStore() bool {
	return (c.VisibilityConfigExist() && c.DataStores[c.VisibilityStore].SQL != nil) ||
		(c.SecondaryVisibilityConfigExist() && c.DataStores[c.SecondaryVisibilityStore].SQL != nil)
}

func (c *Persistence) IsCustomVisibilityStore() bool {
	return c.GetVisibilityStoreConfig().CustomDataStoreConfig != nil ||
		c.GetSecondaryVisibilityStoreConfig().CustomDataStoreConfig != nil
}

func (c *Persistence) GetVisibilityStoreConfig() DataStore {
	return c.DataStores[c.VisibilityStore]
}

func (c *Persistence) GetSecondaryVisibilityStoreConfig() DataStore {
	if c.SecondaryVisibilityStore != "" {
		return c.DataStores[c.SecondaryVisibilityStore]
	}
	if c.VisibilityStore != "" {
		ds := c.DataStores[c.VisibilityStore]
		if ds.Elasticsearch != nil && ds.Elasticsearch.GetSecondaryVisibilityIndex() != "" {
			esConfig := *ds.Elasticsearch
			esConfig.Indices = map[string]string{
				client.VisibilityAppName: ds.Elasticsearch.GetSecondaryVisibilityIndex(),
			}
			ds.Elasticsearch = &esConfig
			return ds
		}
	}
	return DataStore{}
}

func (ds *DataStore) GetIndexName() string {
	switch {
	case ds.SQL != nil:
		return ds.SQL.DatabaseName
	case ds.Cassandra != nil:
		return ds.Cassandra.Keyspace
	case ds.Elasticsearch != nil:
		return ds.Elasticsearch.GetVisibilityIndex()
	case ds.CustomDataStoreConfig != nil:
		return ds.CustomDataStoreConfig.IndexName
	default:
		return ""
	}
}

// Validate validates the data store config
func (ds *DataStore) Validate() error {
	storeConfigCount := 0
	if ds.SQL != nil {
		storeConfigCount++
	}
	if ds.Cassandra != nil {
		storeConfigCount++
	}
	if ds.CustomDataStoreConfig != nil {
		storeConfigCount++
	}
	if ds.Elasticsearch != nil {
		storeConfigCount++
	}
	if storeConfigCount != 1 {
		return errors.New(
			"must provide config for one and only one datastore: " +
				"elasticsearch, cassandra, sql or custom store",
		)
	}

	if ds.SQL != nil && ds.SQL.TaskScanPartitions == 0 {
		ds.SQL.TaskScanPartitions = 1
	}
	if ds.Cassandra != nil {
		if err := ds.Cassandra.validate(); err != nil {
			return err
		}
	}
	if ds.Elasticsearch != nil {
		if err := ds.Elasticsearch.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// GetConsistency returns the gosql.Consistency setting from the configuration for the given store type
func (c *CassandraStoreConsistency) GetConsistency() gocql.Consistency {
	return gocql.ParseConsistency(c.getConsistencySettings().Consistency)
}

// GetSerialConsistency returns the gosql.SerialConsistency setting from the configuration for the store
func (c *CassandraStoreConsistency) GetSerialConsistency() gocql.SerialConsistency {
	res, err := parseSerialConsistency(c.getConsistencySettings().SerialConsistency)
	if err != nil {
		panic(fmt.Sprintf("unable to decode cassandra serial consistency: %v", err))
	}
	return res
}

func (c *CassandraStoreConsistency) getConsistencySettings() *CassandraConsistencySettings {
	return ensureStoreConsistencyNotNil(c).Default
}

func ensureStoreConsistencyNotNil(c *CassandraStoreConsistency) *CassandraStoreConsistency {
	if c == nil {
		c = &CassandraStoreConsistency{}
	}
	if c.Default == nil {
		c.Default = &CassandraConsistencySettings{}
	}
	if c.Default.Consistency == "" {
		c.Default.Consistency = "LOCAL_QUORUM"
	}
	if c.Default.SerialConsistency == "" {
		c.Default.SerialConsistency = "LOCAL_SERIAL"
	}

	return c
}

func (c *Cassandra) validate() error {
	return c.Consistency.validate()
}

func (c *CassandraStoreConsistency) validate() error {
	if c == nil {
		return nil
	}

	v := reflect.ValueOf(*c)

	for i := 0; i < v.NumField(); i++ {
		s, ok := v.Field(i).Interface().(*CassandraConsistencySettings)
		if ok {
			if err := s.validate(); err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *CassandraConsistencySettings) validate() error {
	if c == nil {
		return nil
	}

	if c.Consistency != "" {
		_, err := gocql.ParseConsistencyWrapper(c.Consistency)
		if err != nil {
			return fmt.Errorf("bad cassandra consistency: %v", err)
		}
	}

	if c.SerialConsistency != "" {
		_, err := parseSerialConsistency(c.SerialConsistency)
		if err != nil {
			return fmt.Errorf("bad cassandra serial consistency: %v", err)
		}
	}

	return nil
}

func parseSerialConsistency(serialConsistency string) (gocql.SerialConsistency, error) {
	var s gocql.SerialConsistency
	err := s.UnmarshalText([]byte(strings.ToUpper(serialConsistency)))
	return s, err
}
