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
	"net"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/config"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/shuffle"
	"go.temporal.io/server/environment"

	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql"
)

// TODO merge the initialization with existing persistence setup
const (
	testPostgreSQLUser               = "temporal"
	testPostgreSQLPassword           = "temporal"
	testPostgreSQLConnectionProtocol = "tcp"
	testPostgreSQLDatabaseNamePrefix = "test_"
	testPostgreSQLDatabaseNameSuffix = "temporal_persistence"

	// TODO hard code this dir for now
	//  need to merge persistence test config / initialization in one place
	testPostgreSQLExecutionSchema  = "../../../../../schema/postgresql/v96/temporal/schema.sql"
	testPostgreSQLVisibilitySchema = "../../../../../schema/postgresql/v96/visibility/schema.sql"
)

func TestPostgreSQLNamespaceSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create PostgreSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := newNamespaceSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLQueueMessageSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create PostgreSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := newQueueMessageSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLQueueMetadataSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create PostgreSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := newQueueMetadataSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLMatchingTaskSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create PostgreSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := newMatchingTaskSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLMatchingTaskQueueSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create PostgreSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := newMatchingTaskQueueSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLHistoryShardSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := newHistoryShardSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLHistoryNodeSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := newHistoryNodeSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLHistoryTreeSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := newHistoryTreeSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLHistoryCurrentExecutionSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := newHistoryCurrentExecutionSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLHistoryExecutionSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := newHistoryExecutionSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLHistoryTransferTaskSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := newHistoryTransferTaskSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLHistoryTimerTaskSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := newHistoryTimerTaskSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLHistoryReplicationTaskSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := newHistoryReplicationTaskSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLHistoryVisibilityTaskSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := newHistoryVisibilityTaskSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLHistoryReplicationDLQTaskSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := newHistoryReplicationDLQTaskSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLHistoryExecutionBufferSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := newHistoryExecutionBufferSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLHistoryExecutionActivitySuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := newHistoryExecutionActivitySuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLHistoryExecutionChildWorkflowSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := newHistoryExecutionChildWorkflowSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLHistoryExecutionTimerSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := newHistoryExecutionTimerSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLHistoryExecutionRequestCancelSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := newHistoryExecutionRequestCancelSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLHistoryExecutionSignalSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := newHistoryExecutionSignalSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLHistoryExecutionSignalRequestSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := newHistoryExecutionSignalRequestSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLVisibilitySuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindVisibility, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := newVisibilitySuite(t, store)
	suite.Run(t, s)
}

// NewPostgreSQLConfig returns a new MySQL config for test
func NewPostgreSQLConfig() *config.SQL {
	return &config.SQL{
		User:     testPostgreSQLUser,
		Password: testPostgreSQLPassword,
		ConnectAddr: net.JoinHostPort(
			environment.GetPostgreSQLAddress(),
			strconv.Itoa(environment.GetPostgreSQLPort()),
		),
		ConnectProtocol: testPostgreSQLConnectionProtocol,
		PluginName:      "postgres",
		DatabaseName:    testPostgreSQLDatabaseNamePrefix + shuffle.String(testPostgreSQLDatabaseNameSuffix),
	}
}

func SetupPostgreSQLDatabase(cfg *config.SQL) {
	adminCfg := *cfg
	// NOTE need to connect with empty name to create new database
	adminCfg.DatabaseName = ""

	db, err := sql.NewSQLAdminDB(sqlplugin.DbKindUnknown, &adminCfg, resolver.NewNoopResolver())
	if err != nil {
		panic(fmt.Sprintf("unable to create PostgreSQL admin DB: %v", err))
	}
	defer func() { _ = db.Close() }()

	err = db.CreateDatabase(cfg.DatabaseName)
	if err != nil {
		panic(fmt.Sprintf("unable to create PostgreSQL database: %v", err))
	}
}

func SetupPostgreSQLSchema(cfg *config.SQL) {
	db, err := sql.NewSQLAdminDB(sqlplugin.DbKindUnknown, cfg, resolver.NewNoopResolver())
	if err != nil {
		panic(fmt.Sprintf("unable to create PostgreSQL admin DB: %v", err))
	}
	defer func() { _ = db.Close() }()

	schemaPath, err := filepath.Abs(testPostgreSQLExecutionSchema)
	if err != nil {
		panic(err)
	}

	statements, err := p.LoadAndSplitQuery([]string{schemaPath})
	if err != nil {
		panic(err)
	}

	for _, stmt := range statements {
		if err = db.Exec(stmt); err != nil {
			panic(err)
		}
	}

	schemaPath, err = filepath.Abs(testPostgreSQLVisibilitySchema)
	if err != nil {
		panic(err)
	}

	statements, err = p.LoadAndSplitQuery([]string{schemaPath})
	if err != nil {
		panic(err)
	}

	for _, stmt := range statements {
		if err = db.Exec(stmt); err != nil {
			panic(err)
		}
	}
}

func TearDownPostgreSQLDatabase(cfg *config.SQL) {
	adminCfg := *cfg
	// NOTE need to connect with empty name to create new database
	adminCfg.DatabaseName = ""

	db, err := sql.NewSQLAdminDB(sqlplugin.DbKindUnknown, &adminCfg, resolver.NewNoopResolver())
	if err != nil {
		panic(fmt.Sprintf("unable to create PostgreSQL admin DB: %v", err))
	}
	defer func() { _ = db.Close() }()

	err = db.DropDatabase(cfg.DatabaseName)
	if err != nil {
		panic(fmt.Sprintf("unable to drop PostgreSQL database: %v", err))
	}
}
