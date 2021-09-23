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

package cassandra

import (
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/persistence/schema"
	"go.temporal.io/server/common/resolver"
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

	if err := checkMainKeyspace(cfg, r); err != nil {
		return err
	}
	if cfg.StandardVisibilityConfigExist() {
		return checkVisibilityKeyspace(cfg, r)
	}
	return nil
}

func checkMainKeyspace(
	cfg config.Persistence,
	r resolver.ServiceResolver,
) error {
	ds, ok := cfg.DataStores[cfg.DefaultStore]
	if ok && ds.Cassandra != nil {
		return CheckCompatibleVersion(*ds.Cassandra, r, cassandraschema.Version)
	}
	return nil
}

func checkVisibilityKeyspace(
	cfg config.Persistence,
	r resolver.ServiceResolver,
) error {
	ds, ok := cfg.DataStores[cfg.VisibilityStore]
	if ok && ds.Cassandra != nil {
		return CheckCompatibleVersion(*ds.Cassandra, r, cassandraschema.VisibilityVersion)
	}
	return nil
}

// CheckCompatibleVersion check the version compatibility
func CheckCompatibleVersion(
	cfg config.Cassandra,
	r resolver.ServiceResolver,
	expectedVersion string,
) error {

	session, err := gocql.NewSession(cfg, r, log.NewNoopLogger())
	if err != nil {
		return err
	}
	defer session.Close()

	schemaVersionReader := NewSchemaVersionReader(session)

	return schema.VerifyCompatibleVersion(schemaVersionReader, cfg.Keyspace, expectedVersion)
}
