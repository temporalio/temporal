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
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/shuffle"
	sqliteschema "go.temporal.io/server/schema/sqlite"
)

// TODO merge the initialization with existing persistence setup
const (
	testSQLiteDatabaseNamePrefix = "test_"
	testSQLiteDatabaseNameSuffix = "temporal_persistence"
)

func TestSQLiteNamespaceSuite(t *testing.T) {
	cfg := NewSQLiteConfig()
	SetupSQLiteDatabase(cfg)
	setupSQLiteSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownSQLiteDatabase(cfg)
	}()

	s := newNamespaceSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteQueueMessageSuite(t *testing.T) {
	cfg := NewSQLiteConfig()
	SetupSQLiteDatabase(cfg)
	setupSQLiteSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownSQLiteDatabase(cfg)
	}()

	s := newQueueMessageSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteQueueMetadataSuite(t *testing.T) {
	cfg := NewSQLiteConfig()
	SetupSQLiteDatabase(cfg)
	setupSQLiteSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownSQLiteDatabase(cfg)
	}()

	s := newQueueMetadataSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteMatchingTaskSuite(t *testing.T) {
	cfg := NewSQLiteConfig()
	SetupSQLiteDatabase(cfg)
	setupSQLiteSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownSQLiteDatabase(cfg)
	}()

	s := newMatchingTaskSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteMatchingTaskQueueSuite(t *testing.T) {
	cfg := NewSQLiteConfig()
	SetupSQLiteDatabase(cfg)
	setupSQLiteSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownSQLiteDatabase(cfg)
	}()

	s := newMatchingTaskQueueSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryShardSuite(t *testing.T) {
	cfg := NewSQLiteConfig()
	SetupSQLiteDatabase(cfg)
	setupSQLiteSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownSQLiteDatabase(cfg)
	}()

	s := newHistoryShardSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryNodeSuite(t *testing.T) {
	cfg := NewSQLiteConfig()
	SetupSQLiteDatabase(cfg)
	setupSQLiteSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownSQLiteDatabase(cfg)
	}()

	s := newHistoryNodeSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryTreeSuite(t *testing.T) {
	cfg := NewSQLiteConfig()
	SetupSQLiteDatabase(cfg)
	setupSQLiteSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownSQLiteDatabase(cfg)
	}()

	s := newHistoryTreeSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryCurrentExecutionSuite(t *testing.T) {
	cfg := NewSQLiteConfig()
	SetupSQLiteDatabase(cfg)
	setupSQLiteSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownSQLiteDatabase(cfg)
	}()

	s := newHistoryCurrentExecutionSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryExecutionSuite(t *testing.T) {
	cfg := NewSQLiteConfig()
	SetupSQLiteDatabase(cfg)
	setupSQLiteSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownSQLiteDatabase(cfg)
	}()

	s := newHistoryExecutionSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryTransferTaskSuite(t *testing.T) {
	cfg := NewSQLiteConfig()
	SetupSQLiteDatabase(cfg)
	setupSQLiteSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownSQLiteDatabase(cfg)
	}()

	s := newHistoryTransferTaskSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryTimerTaskSuite(t *testing.T) {
	cfg := NewSQLiteConfig()
	SetupSQLiteDatabase(cfg)
	setupSQLiteSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownSQLiteDatabase(cfg)
	}()

	s := newHistoryTimerTaskSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryReplicationTaskSuite(t *testing.T) {
	cfg := NewSQLiteConfig()
	SetupSQLiteDatabase(cfg)
	setupSQLiteSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownSQLiteDatabase(cfg)
	}()

	s := newHistoryReplicationTaskSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryVisibilityTaskSuite(t *testing.T) {
	cfg := NewSQLiteConfig()
	SetupSQLiteDatabase(cfg)
	setupSQLiteSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownSQLiteDatabase(cfg)
	}()

	s := newHistoryVisibilityTaskSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryTieredStorageTaskSuite(t *testing.T) {
	cfg := NewSQLiteConfig()
	SetupSQLiteDatabase(cfg)
	setupSQLiteSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownSQLiteDatabase(cfg)
	}()

	s := newHistoryTieredStorageTaskSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryReplicationDLQTaskSuite(t *testing.T) {
	cfg := NewSQLiteConfig()
	SetupSQLiteDatabase(cfg)
	setupSQLiteSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownSQLiteDatabase(cfg)
	}()

	s := newHistoryReplicationDLQTaskSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryExecutionBufferSuite(t *testing.T) {
	cfg := NewSQLiteConfig()
	SetupSQLiteDatabase(cfg)
	setupSQLiteSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownSQLiteDatabase(cfg)
	}()

	s := newHistoryExecutionBufferSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryExecutionActivitySuite(t *testing.T) {
	cfg := NewSQLiteConfig()
	SetupSQLiteDatabase(cfg)
	setupSQLiteSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownSQLiteDatabase(cfg)
	}()

	s := newHistoryExecutionActivitySuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryExecutionChildWorkflowSuite(t *testing.T) {
	cfg := NewSQLiteConfig()
	SetupSQLiteDatabase(cfg)
	setupSQLiteSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownSQLiteDatabase(cfg)
	}()

	s := newHistoryExecutionChildWorkflowSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryExecutionTimerSuite(t *testing.T) {
	cfg := NewSQLiteConfig()
	SetupSQLiteDatabase(cfg)
	setupSQLiteSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownSQLiteDatabase(cfg)
	}()

	s := newHistoryExecutionTimerSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryExecutionRequestCancelSuite(t *testing.T) {
	cfg := NewSQLiteConfig()
	SetupSQLiteDatabase(cfg)
	setupSQLiteSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownSQLiteDatabase(cfg)
	}()

	s := newHistoryExecutionRequestCancelSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryExecutionSignalSuite(t *testing.T) {
	cfg := NewSQLiteConfig()
	SetupSQLiteDatabase(cfg)
	setupSQLiteSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownSQLiteDatabase(cfg)
	}()

	s := newHistoryExecutionSignalSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryExecutionSignalRequestSuite(t *testing.T) {
	cfg := NewSQLiteConfig()
	SetupSQLiteDatabase(cfg)
	setupSQLiteSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownSQLiteDatabase(cfg)
	}()

	s := newHistoryExecutionSignalRequestSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteVisibilitySuite(t *testing.T) {
	cfg := NewSQLiteConfig()
	SetupSQLiteDatabase(cfg)
	setupSQLiteSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindVisibility, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownSQLiteDatabase(cfg)
	}()

	s := newVisibilitySuite(t, store)
	suite.Run(t, s)
}

// NewSQLiteConfig returns a new SQLite config for test
func NewSQLiteConfig() *config.SQL {
	return &config.SQL{
		User:            "",
		Password:        "",
		ConnectAddr:     "",
		ConnectProtocol: "",
		ConnectAttributes: map[string]string{
			"mode":  "memory",
			"cache": "shared",
		},
		PluginName:   "sqlite",
		DatabaseName: testSQLiteDatabaseNamePrefix + shuffle.String(testSQLiteDatabaseNameSuffix),
	}
}

func SetupSQLiteDatabase(cfg *config.SQL) {
	adminCfg := *cfg
	// NOTE need to connect with empty name to create new database
	adminCfg.DatabaseName = ""

	db, err := sql.NewSQLAdminDB(sqlplugin.DbKindUnknown, &adminCfg, resolver.NewNoopResolver())
	if err != nil {
		panic(fmt.Sprintf("unable to create SQLite admin DB: %v", err))
	}
	defer func() { _ = db.Close() }()

	err = db.CreateDatabase(cfg.DatabaseName)
	if err != nil {
		panic(fmt.Sprintf("unable to create SQLite database: %v", err))
	}
}

func setupSQLiteSchema(cfg *config.SQL) {
	if err := sqliteschema.SetupSchema(cfg); err != nil {
		panic(err)
	}
}

func TearDownSQLiteDatabase(cfg *config.SQL) {
	adminCfg := *cfg
	// NOTE need to connect with empty name to create new database
	adminCfg.DatabaseName = ""

	db, err := sql.NewSQLAdminDB(sqlplugin.DbKindUnknown, &adminCfg, resolver.NewNoopResolver())
	if err != nil {
		panic(fmt.Sprintf("unable to create SQLite admin DB: %v", err))
	}
	defer func() { _ = db.Close() }()

	err = db.DropDatabase(cfg.DatabaseName)
	if err != nil {
		panic(fmt.Sprintf("unable to drop SQLite database: %v", err))
	}
}
