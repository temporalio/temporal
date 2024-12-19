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

package sql

import (
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/tests/testutils"
)

// TestCluster allows executing cassandra operations in testing.
type TestCluster struct {
	dbName         string
	schemaDir      string
	cfg            config.SQL
	faultInjection *config.FaultInjection
	logger         log.Logger
}

// NewTestCluster returns a new SQL test cluster
func NewTestCluster(
	pluginName string,
	dbName string,
	username string,
	password string,
	host string,
	port int,
	connectAttributes map[string]string,
	schemaDir string,
	faultInjection *config.FaultInjection,
	logger log.Logger,
) *TestCluster {
	var result TestCluster
	result.logger = logger
	result.dbName = dbName

	result.schemaDir = schemaDir
	result.cfg = config.SQL{
		User:               username,
		Password:           password,
		ConnectAddr:        fmt.Sprintf("%v:%v", host, port),
		ConnectProtocol:    "tcp",
		PluginName:         pluginName,
		DatabaseName:       dbName,
		TaskScanPartitions: 4,
		ConnectAttributes:  connectAttributes,
	}

	result.faultInjection = faultInjection
	return &result
}

// DatabaseName from PersistenceTestCluster interface
func (s *TestCluster) DatabaseName() string {
	return s.dbName
}

// SetupTestDatabase from PersistenceTestCluster interface
func (s *TestCluster) SetupTestDatabase() {
	s.CreateDatabase()

	if s.schemaDir == "" {
		s.logger.Info("No schema directory provided, skipping schema setup")
		return
	}

	schemaDir := s.schemaDir + "/"
	if !strings.HasPrefix(schemaDir, "/") && !strings.HasPrefix(schemaDir, "../") {
		temporalPackageDir := testutils.GetRepoRootDirectory()
		schemaDir = path.Join(temporalPackageDir, schemaDir)
	}
	s.LoadSchema(path.Join(schemaDir, "temporal", "schema.sql"))
	s.LoadSchema(path.Join(schemaDir, "visibility", "schema.sql"))
}

// Config returns the persistence config for connecting to this test cluster
func (s *TestCluster) Config() config.Persistence {
	cfg := s.cfg
	return config.Persistence{
		DefaultStore:    "test",
		VisibilityStore: "test",
		DataStores: map[string]config.DataStore{
			"test": {SQL: &cfg, FaultInjection: s.faultInjection},
		},
		TransactionSizeLimit: dynamicconfig.GetIntPropertyFn(primitives.DefaultTransactionSizeLimit),
	}
}

// TearDownTestDatabase from PersistenceTestCluster interface
func (s *TestCluster) TearDownTestDatabase() {
	s.DropDatabase()
}

// CreateDatabase from PersistenceTestCluster interface
func (s *TestCluster) CreateDatabase() {
	cfg2 := s.cfg
	// NOTE need to connect with empty name to create new database
	if cfg2.PluginName != "sqlite" {
		cfg2.DatabaseName = ""
	}

	var db sqlplugin.AdminDB
	var err error
	err = backoff.ThrottleRetry(
		func() error {
			db, err = NewSQLAdminDB(sqlplugin.DbKindUnknown, &cfg2, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
			return err
		},
		backoff.NewExponentialRetryPolicy(time.Second).WithExpirationInterval(time.Minute),
		nil,
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		err := db.Close()
		if err != nil {
			panic(err)
		}
	}()
	err = db.CreateDatabase(s.cfg.DatabaseName)
	if err != nil {
		panic(err)
	}
	s.logger.Info("created database", tag.NewStringTag("database", s.cfg.DatabaseName))
}

// DropDatabase from PersistenceTestCluster interface
func (s *TestCluster) DropDatabase() {
	cfg2 := s.cfg

	if cfg2.PluginName == "sqlite" && cfg2.DatabaseName != ":memory:" && cfg2.ConnectAttributes["mode"] != "memory" {
		if len(cfg2.DatabaseName) > 3 { // 3 should mean not ., .., empty, or /
			err := os.Remove(cfg2.DatabaseName)
			if err != nil {
				panic(err)
			}
		}
		return
	}

	// NOTE need to connect with empty name to drop the database
	cfg2.DatabaseName = ""
	db, err := NewSQLAdminDB(sqlplugin.DbKindUnknown, &cfg2, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		panic(err)
	}
	defer func() {
		err := db.Close()
		if err != nil {
			panic(err)
		}
	}()
	err = db.DropDatabase(s.cfg.DatabaseName)
	if err != nil {
		panic(err)
	}
	s.logger.Info("dropped database", tag.NewStringTag("database", s.cfg.DatabaseName))
}

// LoadSchema from PersistenceTestCluster interface
func (s *TestCluster) LoadSchema(schemaFile string) {
	statements, err := p.LoadAndSplitQuery([]string{schemaFile})
	if err != nil {
		s.logger.Fatal("LoadSchema", tag.Error(err))
	}

	var db sqlplugin.AdminDB
	err = backoff.ThrottleRetry(
		func() error {
			db, err = NewSQLAdminDB(sqlplugin.DbKindUnknown, &s.cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
			return err
		},
		backoff.NewExponentialRetryPolicy(time.Second).WithExpirationInterval(time.Minute),
		nil,
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		err := db.Close()
		if err != nil {
			panic(err)
		}
	}()

	for _, stmt := range statements {
		if err = db.Exec(stmt); err != nil {
			s.logger.Fatal("LoadSchema", tag.Error(err))
		}
	}
	s.logger.Info("loaded schema")
}
