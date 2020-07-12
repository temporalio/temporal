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
	"fmt"
	"strings"

	"github.com/gocql/gocql"
)

const (
	// StoreTypeSQL refers to sql based storage as persistence store
	StoreTypeSQL = "sql"
	// StoreTypeCassandra refers to cassandra as persistence store
	StoreTypeCassandra = "cassandra"
)

// StoreType ndicates the type of persistence sub-store that is involved
type StoreType int

const (
	// TaskStoreType represents the Task store type
	TaskStoreType StoreType = iota
	// ShardStoreType represents the Shard store type
	ShardStoreType
	// HistoryStoreType represents the History store type
	HistoryStoreType
	// ExecutionStoreType represents the execution store type
	ExecutionStoreType
	// ClusterMetadataStoreType represents the Cluster Membership Metadata store type
	ClusterMetadataStoreType
	// NamespaceMetadataStoreType represents the Namespace Metadata store type
	NamespaceMetadataStoreType
	// QueueStoreType represents the queue store type
	QueueStoreType
	// VisibilityStoreType represents ths visibility store type
	VisibilityStoreType
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
		if ds.Cassandra != nil {
			if err := ds.Cassandra.validate(); err != nil {
				return err
			}
		}
	}
	return nil
}

// IsAdvancedVisibilityConfigExist returns whether user specified advancedVisibilityStore in config
func (c *Persistence) IsAdvancedVisibilityConfigExist() bool {
	return len(c.AdvancedVisibilityStore) != 0
}

// GetConsistency returns the gosql.Consistency setting from the configuration for the given store type
func (c *CassandraStoreConsistency) GetConsistency(storeType StoreType) gocql.Consistency {
	return gocql.ParseConsistency(c.getConsistencySettings(storeType).Consistency)
}

// GetSerialConsistency returns the gosql.SerialConsistency setting from the configuration for the store
func (c *CassandraStoreConsistency) GetSerialConsistency(storeType StoreType) gocql.SerialConsistency {
	// Error result can be ignored as validate should have been called
	res, _ := parseSerialConsistency(c.getConsistencySettings(storeType).SerialConsistency)
	return res
}

func (c *CassandraStoreConsistency) getConsistencySettings(storeType StoreType) *CassandraConsistencySettings {
	c = ensureStoreConsistencyNotNil(c)
	var s *CassandraConsistencySettings

	switch storeType {
	case TaskStoreType:
		s = c.Task
	case ShardStoreType:
		s = c.Shard
	case ExecutionStoreType:
		s = c.Execution
	case ClusterMetadataStoreType:
		s = c.ClusterMetadata
	case NamespaceMetadataStoreType:
		s = c.NamespaceMetadata
	case QueueStoreType:
		s = c.Queue
	case HistoryStoreType:
		s = c.History
	case VisibilityStoreType:
		s = c.Visibility
	default:
		panic(fmt.Sprintf("unknown store type provided: %v", storeType))
	}

	return mergeConsistencySettings(s, c.Default)
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

func mergeConsistencySettings(c *CassandraConsistencySettings, d *CassandraConsistencySettings) *CassandraConsistencySettings {
	if c == nil {
		c = d
	}
	if c.Consistency == "" {
		c.Consistency = d.Consistency
	}
	if c.SerialConsistency == "" {
		c.SerialConsistency = d.SerialConsistency
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

	settings := []*CassandraConsistencySettings{
		c.Default,
		c.ClusterMetadata,
		c.History,
		c.NamespaceMetadata,
		c.Shard,
		c.Task,
		c.Queue,
		c.Visibility,
		c.Execution,
	}

	for _, s := range settings {
		if err := s.validate(); err != nil {
			return err
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
