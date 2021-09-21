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
	for _, st := range stores {
		ds, ok := c.DataStores[st]
		if !ok {
			return fmt.Errorf("persistence config: missing config for datastore %q", st)
		}
		if err := ds.Validate(); err != nil {
			return fmt.Errorf("persistence config: datastore %q: %s", st, err.Error())
		}
	}

	if err := c.validateAdvancedVisibility(); err != nil {
		return err
	}

	return nil
}

// StandardVisibilityConfigExist returns whether user specified visibilityStore in config
func (c *Persistence) StandardVisibilityConfigExist() bool {
	return c.VisibilityStore != ""
}

// AdvancedVisibilityConfigExist returns whether user specified advancedVisibilityStore in config
func (c *Persistence) AdvancedVisibilityConfigExist() bool {
	return c.AdvancedVisibilityStore != ""
}

func (c *Persistence) validateAdvancedVisibility() error {
	if !c.StandardVisibilityConfigExist() && !c.AdvancedVisibilityConfigExist() {
		return errors.New("persistence config: one of visibilityStore or advancedVisibilityStore must be specified")
	}

	if !c.AdvancedVisibilityConfigExist() {
		return nil
	}

	advancedVisibilityDataStore, ok := c.DataStores[c.AdvancedVisibilityStore]
	if !ok {
		return fmt.Errorf("persistence config: advanced visibility datastore %q: missing config", c.AdvancedVisibilityStore)
	}

	if err := advancedVisibilityDataStore.Elasticsearch.Validate(c.AdvancedVisibilityStore); err != nil {
		return err
	}

	return nil
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
	if storeConfigCount != 1 {
		return errors.New("must provide config for one and only one for DataStore of cassandra or sql or custom stores")
	}

	if ds.SQL != nil && ds.SQL.TaskScanPartitions == 0 {
		ds.SQL.TaskScanPartitions = 1
	}
	if ds.Cassandra != nil {
		if err := ds.Cassandra.validate(); err != nil {
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
