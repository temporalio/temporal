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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/shuffle"
)

// TODO merge the initialization with existing persistence setup
const (
	testSQLiteDatabaseNamePrefix = "test_"
	testSQLiteDatabaseNameSuffix = "temporal_persistence"
	testSQLiteSchemaDir          = "../../../../../schema/sqlite/v3" // specify if mode is not "memory"
)

func TestSQLiteNamespaceSuite(t *testing.T) {
	cfg := newSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := newNamespaceSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteQueueMessageSuite(t *testing.T) {
	cfg := newSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := newQueueMessageSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteQueueMetadataSuite(t *testing.T) {
	cfg := newSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := newQueueMetadataSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteMatchingTaskSuite(t *testing.T) {
	cfg := newSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := newMatchingTaskSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteMatchingTaskQueueSuite(t *testing.T) {
	cfg := newSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := newMatchingTaskQueueSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryShardSuite(t *testing.T) {
	cfg := newSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := newHistoryShardSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryNodeSuite(t *testing.T) {
	cfg := newSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := newHistoryNodeSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryTreeSuite(t *testing.T) {
	cfg := newSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := newHistoryTreeSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryCurrentExecutionSuite(t *testing.T) {
	cfg := newSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := newHistoryCurrentExecutionSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryExecutionSuite(t *testing.T) {
	cfg := newSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := newHistoryExecutionSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryTransferTaskSuite(t *testing.T) {
	cfg := newSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := newHistoryTransferTaskSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryTimerTaskSuite(t *testing.T) {
	cfg := newSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := newHistoryTimerTaskSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryReplicationTaskSuite(t *testing.T) {
	cfg := newSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := newHistoryReplicationTaskSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryVisibilityTaskSuite(t *testing.T) {
	cfg := newSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := newHistoryVisibilityTaskSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryReplicationDLQTaskSuite(t *testing.T) {
	cfg := newSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := newHistoryReplicationDLQTaskSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryExecutionBufferSuite(t *testing.T) {
	cfg := newSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := newHistoryExecutionBufferSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryExecutionActivitySuite(t *testing.T) {
	cfg := newSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := newHistoryExecutionActivitySuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryExecutionChildWorkflowSuite(t *testing.T) {
	cfg := newSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := newHistoryExecutionChildWorkflowSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryExecutionTimerSuite(t *testing.T) {
	cfg := newSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := newHistoryExecutionTimerSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryExecutionRequestCancelSuite(t *testing.T) {
	cfg := newSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := newHistoryExecutionRequestCancelSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryExecutionSignalSuite(t *testing.T) {
	cfg := newSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := newHistoryExecutionSignalSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryExecutionSignalRequestSuite(t *testing.T) {
	cfg := newSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := newHistoryExecutionSignalRequestSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteVisibilitySuite(t *testing.T) {
	cfg := newSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindVisibility, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := newVisibilitySuite(t, store)
	suite.Run(t, s)
}

// newSQLiteMemoryConfig returns a new SQLite config for test
func newSQLiteMemoryConfig() *config.SQL {
	return &config.SQL{
		User:            "",
		Password:        "",
		ConnectAddr:     "",
		ConnectProtocol: "",
		ConnectAttributes: map[string]string{
			"mode":  "memory",
			"cache": "private",
		},
		PluginName:   sqlite.PluginName,
		DatabaseName: "default",
	}
}

// newSQLiteFileConfig returns a new SQLite config for test
func newSQLiteFileConfig() *config.SQL {
	return &config.SQL{
		User:            "",
		Password:        "",
		ConnectAddr:     "",
		ConnectProtocol: "",
		ConnectAttributes: map[string]string{
			"cache":        "private",
			"setup":        "true",
			"journal_mode": "WAL",
			"synchronous":  "2",
		},
		PluginName:   sqlite.PluginName,
		DatabaseName: testSQLiteDatabaseNamePrefix + shuffle.String(testSQLiteDatabaseNameSuffix),
	}
}

func setupSQLiteDatabase(cfg *config.SQL, t *testing.T) {
	db, err := sql.NewSQLAdminDB(sqlplugin.DbKindUnknown, cfg, resolver.NewNoopResolver())
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	err = db.CreateDatabase(cfg.DatabaseName)
	if err != nil {
		panic(fmt.Sprintf("unable to create SQLite database: %v", err))
	}
}

func TestSQLiteFileNamespaceSuite(t *testing.T) {
	cfg := newSQLiteFileConfig()
	setupSQLiteDatabase(cfg, t)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer testCleanUp(store, cfg, t)

	s := newNamespaceSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileQueueMessageSuite(t *testing.T) {
	cfg := newSQLiteFileConfig()
	setupSQLiteDatabase(cfg, t)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer testCleanUp(store, cfg, t)

	s := newQueueMessageSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileQueueMetadataSuite(t *testing.T) {
	cfg := newSQLiteFileConfig()
	setupSQLiteDatabase(cfg, t)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer testCleanUp(store, cfg, t)

	s := newQueueMetadataSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileMatchingTaskSuite(t *testing.T) {
	cfg := newSQLiteFileConfig()
	setupSQLiteDatabase(cfg, t)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer testCleanUp(store, cfg, t)

	s := newMatchingTaskSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileMatchingTaskQueueSuite(t *testing.T) {
	cfg := newSQLiteFileConfig()
	setupSQLiteDatabase(cfg, t)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer testCleanUp(store, cfg, t)

	s := newMatchingTaskQueueSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileHistoryShardSuite(t *testing.T) {
	cfg := newSQLiteFileConfig()
	setupSQLiteDatabase(cfg, t)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer testCleanUp(store, cfg, t)

	s := newHistoryShardSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileHistoryNodeSuite(t *testing.T) {
	cfg := newSQLiteFileConfig()
	setupSQLiteDatabase(cfg, t)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer testCleanUp(store, cfg, t)

	s := newHistoryNodeSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileHistoryTreeSuite(t *testing.T) {
	cfg := newSQLiteFileConfig()
	setupSQLiteDatabase(cfg, t)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer testCleanUp(store, cfg, t)

	s := newHistoryTreeSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileHistoryCurrentExecutionSuite(t *testing.T) {
	cfg := newSQLiteFileConfig()
	setupSQLiteDatabase(cfg, t)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer testCleanUp(store, cfg, t)

	s := newHistoryCurrentExecutionSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileHistoryExecutionSuite(t *testing.T) {
	cfg := newSQLiteFileConfig()
	setupSQLiteDatabase(cfg, t)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer testCleanUp(store, cfg, t)

	s := newHistoryExecutionSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileHistoryTransferTaskSuite(t *testing.T) {
	cfg := newSQLiteFileConfig()
	setupSQLiteDatabase(cfg, t)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer testCleanUp(store, cfg, t)

	s := newHistoryTransferTaskSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileHistoryTimerTaskSuite(t *testing.T) {
	cfg := newSQLiteFileConfig()
	setupSQLiteDatabase(cfg, t)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer testCleanUp(store, cfg, t)

	s := newHistoryTimerTaskSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileHistoryReplicationTaskSuite(t *testing.T) {
	cfg := newSQLiteFileConfig()
	setupSQLiteDatabase(cfg, t)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer testCleanUp(store, cfg, t)

	s := newHistoryReplicationTaskSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileHistoryVisibilityTaskSuite(t *testing.T) {
	cfg := newSQLiteFileConfig()
	setupSQLiteDatabase(cfg, t)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer testCleanUp(store, cfg, t)

	s := newHistoryVisibilityTaskSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileHistoryReplicationDLQTaskSuite(t *testing.T) {
	cfg := newSQLiteFileConfig()
	setupSQLiteDatabase(cfg, t)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer testCleanUp(store, cfg, t)

	s := newHistoryReplicationDLQTaskSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileHistoryExecutionBufferSuite(t *testing.T) {
	cfg := newSQLiteFileConfig()
	setupSQLiteDatabase(cfg, t)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer testCleanUp(store, cfg, t)

	s := newHistoryExecutionBufferSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileHistoryExecutionActivitySuite(t *testing.T) {
	cfg := newSQLiteFileConfig()
	setupSQLiteDatabase(cfg, t)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer testCleanUp(store, cfg, t)

	s := newHistoryExecutionActivitySuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileHistoryExecutionChildWorkflowSuite(t *testing.T) {
	cfg := newSQLiteFileConfig()
	setupSQLiteDatabase(cfg, t)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer testCleanUp(store, cfg, t)

	s := newHistoryExecutionChildWorkflowSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileHistoryExecutionTimerSuite(t *testing.T) {
	cfg := newSQLiteFileConfig()
	setupSQLiteDatabase(cfg, t)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer testCleanUp(store, cfg, t)

	s := newHistoryExecutionTimerSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileHistoryExecutionRequestCancelSuite(t *testing.T) {
	cfg := newSQLiteFileConfig()
	setupSQLiteDatabase(cfg, t)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer testCleanUp(store, cfg, t)

	s := newHistoryExecutionRequestCancelSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileHistoryExecutionSignalSuite(t *testing.T) {
	cfg := newSQLiteFileConfig()
	setupSQLiteDatabase(cfg, t)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer testCleanUp(store, cfg, t)

	s := newHistoryExecutionSignalSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileHistoryExecutionSignalRequestSuite(t *testing.T) {
	cfg := newSQLiteFileConfig()
	setupSQLiteDatabase(cfg, t)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer testCleanUp(store, cfg, t)

	s := newHistoryExecutionSignalRequestSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileVisibilitySuite(t *testing.T) {
	cfg := newSQLiteFileConfig()
	setupSQLiteDatabase(cfg, t)
	store, err := sql.NewSQLDB(sqlplugin.DbKindVisibility, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer testCleanUp(store, cfg, t)

	s := newVisibilitySuite(t, store)
	suite.Run(t, s)
}

func testCleanUp(d sqlplugin.DB, c *config.SQL, t *testing.T) {
	err := d.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = os.Remove(c.DatabaseName)
	if err != nil {
		t.Fatal(err)
	}
	err = os.Remove(c.DatabaseName + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	err = os.Remove(c.DatabaseName + "-shm")
	if err != nil {
		t.Fatal(err)
	}
}
