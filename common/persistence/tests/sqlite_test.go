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

package tests

import (
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/environment"
)

// TODO merge the initialization with existing persistence setup
const (
	testSQLiteClusterName = "temporal_sqlite_cluster"
	testSQLiteSchemaDir   = "schema/sqlite/v3" // specify if mode is not "memory"
)

func TestSQLiteExecutionMutableStateStoreSuite(t *testing.T) {
	cfg := NewSQLiteMemoryConfig()
	SetupSQLiteDatabase(cfg)
	logger := log.NewNoopLogger()
	factory := sql.NewFactory(
		*cfg,
		resolver.NewNoopResolver(),
		testSQLiteClusterName,
		logger,
	)
	shardStore, err := factory.NewShardStore()
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	executionStore, err := factory.NewExecutionStore()
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		factory.Close()
	}()

	s := NewExecutionMutableStateSuite(
		t,
		shardStore,
		executionStore,
		serialization.NewSerializer(),
		logger,
	)
	suite.Run(t, s)
}

func TestSQLiteExecutionMutableStateTaskStoreSuite(t *testing.T) {
	cfg := NewSQLiteMemoryConfig()
	SetupSQLiteDatabase(cfg)
	logger := log.NewNoopLogger()
	factory := sql.NewFactory(
		*cfg,
		resolver.NewNoopResolver(),
		testSQLiteClusterName,
		logger,
	)
	shardStore, err := factory.NewShardStore()
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	executionStore, err := factory.NewExecutionStore()
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		factory.Close()
	}()

	s := NewExecutionMutableStateTaskSuite(
		t,
		shardStore,
		executionStore,
		serialization.NewSerializer(),
		logger,
	)
	suite.Run(t, s)
}

func TestSQLiteHistoryStoreSuite(t *testing.T) {
	cfg := NewSQLiteMemoryConfig()
	SetupSQLiteDatabase(cfg)
	logger := log.NewNoopLogger()
	factory := sql.NewFactory(
		*cfg,
		resolver.NewNoopResolver(),
		testSQLiteClusterName,
		logger,
	)
	store, err := factory.NewExecutionStore()
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		factory.Close()
	}()

	s := NewHistoryEventsSuite(t, store, logger)
	suite.Run(t, s)
}

func TestSQLiteTaskQueueSuite(t *testing.T) {
	cfg := NewSQLiteMemoryConfig()
	SetupSQLiteDatabase(cfg)
	logger := log.NewNoopLogger()
	factory := sql.NewFactory(
		*cfg,
		resolver.NewNoopResolver(),
		testSQLiteClusterName,
		logger,
	)
	taskQueueStore, err := factory.NewTaskStore()
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		factory.Close()
	}()

	s := NewTaskQueueSuite(t, taskQueueStore, logger)
	suite.Run(t, s)
}

func TestSQLiteTaskQueueTaskSuite(t *testing.T) {
	cfg := NewSQLiteMemoryConfig()
	SetupSQLiteDatabase(cfg)
	defer os.Remove(cfg.DatabaseName)

	logger := log.NewNoopLogger()
	factory := sql.NewFactory(
		*cfg,
		resolver.NewNoopResolver(),
		testSQLiteClusterName,
		logger,
	)
	taskQueueStore, err := factory.NewTaskStore()
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		factory.Close()
	}()

	s := NewTaskQueueTaskSuite(t, taskQueueStore, logger)
	suite.Run(t, s)
}

// NewSQLiteMemoryConfig returns a new SQLite config for test
func NewSQLiteMemoryConfig() *config.SQL {
	return &config.SQL{
		User:              "",
		Password:          "",
		ConnectAddr:       environment.Localhost,
		ConnectProtocol:   "tcp",
		PluginName:        "sqlite",
		DatabaseName:      "default",
		ConnectAttributes: map[string]string{"mode": "memory", "cache": "private"},
	}
}

// NewSQLiteMemoryConfig returns a new SQLite config for test
func NewSQLiteFileConfig() *config.SQL {
	return &config.SQL{
		User:              "",
		Password:          "",
		ConnectAddr:       environment.Localhost,
		ConnectProtocol:   "tcp",
		PluginName:        "sqlite",
		DatabaseName:      "test_" + persistencetests.GenerateRandomDBName(3),
		ConnectAttributes: map[string]string{"cache": "private"},
	}
}

func SetupSQLiteDatabase(cfg *config.SQL) {
	adminCfg := *cfg
	db, err := sql.NewSQLAdminDB(sqlplugin.DbKindUnknown, &adminCfg, resolver.NewNoopResolver())
	if err != nil {
		panic(fmt.Sprintf("unable to create SQLite admin DB: %v", err))
	}
	defer func() { _ = db.Close() }()

	err = db.CreateDatabase(cfg.DatabaseName)
	if err != nil {
		panic(fmt.Sprintf("unable to create SQLite database: %v", err))
	}

	LoadSchema(cfg, path.Join(testSQLiteSchemaDir, "temporal", "schema.sql"))
	LoadSchema(cfg, path.Join(testSQLiteSchemaDir, "visibility", "schema.sql"))
}

func LoadSchema(cfg *config.SQL, schemaFile string) {
	statements, err := persistence.LoadAndSplitQuery([]string{schemaFile})
	if err != nil {
		panic(fmt.Sprintf("LoadSchema %+v", tag.Error(err)))
	}

	db, err := sql.NewSQLAdminDB(sqlplugin.DbKindUnknown, cfg, resolver.NewNoopResolver())
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
			panic(fmt.Sprintf("LoadSchema %+v", tag.Error(err)))
		}
	}
}
