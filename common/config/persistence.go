// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
	if c.AdvancedVisibilityStore != "" {
		stores = append(stores, c.AdvancedVisibilityStore)
	}

	// There are 3 config keys:
	// - visibilityStore: can set any data store
	// - secondaryVisibilityStore: can set any data store
	// - advancedVisibilityStore: can only set elasticsearch data store
	// If visibilityStore is set, then it's always the primary.
	// If secondaryVisibilityStore is set, it's always the secondary.
	//
	// Valid dual visibility combinations (order: primary, secondary):
	// - visibilityStore (advanced sql),  secondaryVisibilityStore (advanced sql)
	// - visibilityStore (es),            visibilityStore (es) [via elasticsearch.indices config]
	// - advancedVisibilityStore (es),    advancedVisibilityStore (es) [via elasticsearch.indices config]
	//
	// Invalid dual visibility combinations:
	// - visibilityStore (advanced sql),  secondaryVisibilityStore (standard, es)
	// - visibilityStore (advanced sql),  advancedVisibilityStore (es)
	// - visibilityStore (es),            secondaryVisibilityStore (any)
	// - visibilityStore (es),            advancedVisibilityStore (es)
	// - advancedVisibilityStore (es),    secondaryVisibilityStore (any)
	//
	// The validation for dual visibility pair (advanced sql, advanced sql) is in visibility factory
	// due to circular dependency. This will be better after standard visibility is removed.

	if c.VisibilityStore == "" && c.AdvancedVisibilityStore == "" {
		return errors.New("persistence config: visibilityStore must be specified")
	}
	if c.SecondaryVisibilityStore != "" && c.AdvancedVisibilityStore != "" {
		return errors.New(
			"persistence config: cannot specify both secondaryVisibilityStore and " +
				"advancedVisibilityStore",
		)
	}
	if c.AdvancedVisibilityStore != "" && c.DataStores[c.AdvancedVisibilityStore].Elasticsearch == nil {
		return fmt.Errorf(
			"persistence config: advanced visibility datastore %q: missing elasticsearch config",
			c.AdvancedVisibilityStore,
		)
	}
	if c.DataStores[c.VisibilityStore].Elasticsearch != nil &&
		(c.SecondaryVisibilityStore != "" || c.AdvancedVisibilityStore != "") {
		return errors.New(
			"persistence config: cannot set secondaryVisibilityStore or advancedVisibilityStore " +
				"when visibilityStore is setting elasticsearch datastore",
		)
	}
	if c.DataStores[c.SecondaryVisibilityStore].Elasticsearch.GetSecondaryVisibilityIndex() != "" {
		return fmt.Errorf(
			"persistence config: secondary visibility datastore %q: elasticsearch config: "+
				"cannot set secondary_visibility",
			c.SecondaryVisibilityStore,
		)
	}

	cntEsConfigs := 0
	for _, st := range stores {
		ds, ok := c.DataStores[st]
		if !ok {
			return fmt.Errorf("persistence config: missing config for datastore %q", st)
		}
		if err := ds.Validate(); err != nil {
			return fmt.Errorf("persistence config: datastore %q: %s", st, err.Error())
		}
		if ds.Elasticsearch != nil {
			cntEsConfigs++
		}
	}

	if cntEsConfigs > 1 {
		return fmt.Errorf(
			"persistence config: cannot have more than one Elasticsearch visibility store config " +
				"(use `elasticsearch.indices.secondary_visibility` config key if you need to set a " +
				"secondary Elasticsearch visibility store)",
		)
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

// AdvancedVisibilityConfigExist returns whether user specified advancedVisibilityStore in config
func (c *Persistence) AdvancedVisibilityConfigExist() bool {
	return c.AdvancedVisibilityStore != ""
}

func (c *Persistence) IsSQLVisibilityStore() bool {
	return (c.VisibilityConfigExist() && c.DataStores[c.VisibilityStore].SQL != nil) ||
		(c.SecondaryVisibilityConfigExist() && c.DataStores[c.SecondaryVisibilityStore].SQL != nil)
}

func (c *Persistence) GetVisibilityStoreConfig() DataStore {
	if c.VisibilityStore != "" {
		return c.DataStores[c.VisibilityStore]
	}
	if c.AdvancedVisibilityStore != "" {
		return c.DataStores[c.AdvancedVisibilityStore]
	}
	// Based on validation above, this should never happen.
	return DataStore{}
}

func (c *Persistence) GetSecondaryVisibilityStoreConfig() DataStore {
	if c.SecondaryVisibilityStore != "" {
		return c.DataStores[c.SecondaryVisibilityStore]
	}
	if c.VisibilityStore != "" {
		if c.AdvancedVisibilityStore != "" {
			return c.DataStores[c.AdvancedVisibilityStore]
		}
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
	if c.AdvancedVisibilityStore != "" {
		ds := c.DataStores[c.AdvancedVisibilityStore]
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
