// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
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
	"io/ioutil"
	"net"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/service/config"
	"go.temporal.io/server/common/shuffle"
	"go.temporal.io/server/environment"

	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
)

// TODO merge the initialization with existing persistence setup
const (
	testMySQLUser               = "temporal"
	testMySQLPassword           = "temporal"
	testMySQLConnectionProtocol = "tcp"
	testMySQLDatabaseNamePrefix = "test_"
	testMySQLDatabaseNameSuffix = "temporal_persistence"

	// TODO hard code this dir for now
	//  need to merge persistence test config / initialization in one place
	testMySQLSchema = "../../../../../schema/mysql/v57/temporal/schema.sql"
)

func TestMySQLNamespaceSuite(t *testing.T) {
	cfg := NewMySQLConfig()
	SetupMySQLDatabase(cfg)
	SetupMySQLSchema(cfg)
	store, err := sql.NewSQLDB(cfg)
	if err != nil {
		panic(fmt.Sprintf("unable to create MySQL DB: %v", err))
	}
	defer func() {
		_ = store.Close()
		TearDownMySQLDatabase(cfg)
	}()

	s := newNamespaceSuite(t, store)
	suite.Run(t, s)
}

func TestMySQLMatchingTaskSuite(t *testing.T) {
	cfg := NewMySQLConfig()
	SetupMySQLDatabase(cfg)
	SetupMySQLSchema(cfg)
	store, err := sql.NewSQLDB(cfg)
	if err != nil {
		panic(fmt.Sprintf("unable to create MySQL DB: %v", err))
	}
	defer func() {
		_ = store.Close()
		TearDownMySQLDatabase(cfg)
	}()

	s := newMatchingTaskSuite(t, store)
	suite.Run(t, s)
}

func TestMySQLMatchingTaskQueueSuite(t *testing.T) {
	cfg := NewMySQLConfig()
	SetupMySQLDatabase(cfg)
	SetupMySQLSchema(cfg)
	store, err := sql.NewSQLDB(cfg)
	if err != nil {
		panic(fmt.Sprintf("unable to create MySQL DB: %v", err))
	}
	defer func() {
		_ = store.Close()
		TearDownMySQLDatabase(cfg)
	}()

	s := newMatchingTaskQueueSuite(t, store)
	suite.Run(t, s)
}

func TestMySQLHistoryShardSuite(t *testing.T) {
	cfg := NewMySQLConfig()
	SetupMySQLDatabase(cfg)
	SetupMySQLSchema(cfg)
	store, err := sql.NewSQLDB(cfg)
	if err != nil {
		panic(fmt.Sprintf("unable to create MySQL DB: %v", err))
	}
	defer func() {
		_ = store.Close()
		TearDownMySQLDatabase(cfg)
	}()

	s := newHistoryShardSuite(t, store)
	suite.Run(t, s)
}

func TestMySQLHistoryExecutionBufferSuite(t *testing.T) {
	cfg := NewMySQLConfig()
	SetupMySQLDatabase(cfg)
	SetupMySQLSchema(cfg)
	store, err := sql.NewSQLDB(cfg)
	if err != nil {
		panic(fmt.Sprintf("unable to create MySQL DB: %v", err))
	}
	defer func() {
		_ = store.Close()
		TearDownMySQLDatabase(cfg)
	}()

	s := newHistoryExecutionBufferSuite(t, store)
	suite.Run(t, s)
}

func TestMySQLHistoryExecutionActivitySuite(t *testing.T) {
	cfg := NewMySQLConfig()
	SetupMySQLDatabase(cfg)
	SetupMySQLSchema(cfg)
	store, err := sql.NewSQLDB(cfg)
	if err != nil {
		panic(fmt.Sprintf("unable to create MySQL DB: %v", err))
	}
	defer func() {
		_ = store.Close()
		TearDownMySQLDatabase(cfg)
	}()

	s := newHistoryExecutionActivitySuite(t, store)
	suite.Run(t, s)
}

func TestMySQLHistoryExecutionChildWorkflowSuite(t *testing.T) {
	cfg := NewMySQLConfig()
	SetupMySQLDatabase(cfg)
	SetupMySQLSchema(cfg)
	store, err := sql.NewSQLDB(cfg)
	if err != nil {
		panic(fmt.Sprintf("unable to create MySQL DB: %v", err))
	}
	defer func() {
		_ = store.Close()
		TearDownMySQLDatabase(cfg)
	}()

	s := newHistoryExecutionChildWorkflowSuite(t, store)
	suite.Run(t, s)
}

func TestMySQLHistoryExecutionTimerSuite(t *testing.T) {
	cfg := NewMySQLConfig()
	SetupMySQLDatabase(cfg)
	SetupMySQLSchema(cfg)
	store, err := sql.NewSQLDB(cfg)
	if err != nil {
		panic(fmt.Sprintf("unable to create MySQL DB: %v", err))
	}
	defer func() {
		_ = store.Close()
		TearDownMySQLDatabase(cfg)
	}()

	s := newHistoryExecutionTimerSuite(t, store)
	suite.Run(t, s)
}

func TestMySQLHistoryExecutionRequestCancelSuite(t *testing.T) {
	cfg := NewMySQLConfig()
	SetupMySQLDatabase(cfg)
	SetupMySQLSchema(cfg)
	store, err := sql.NewSQLDB(cfg)
	if err != nil {
		panic(fmt.Sprintf("unable to create MySQL DB: %v", err))
	}
	defer func() {
		_ = store.Close()
		TearDownMySQLDatabase(cfg)
	}()

	s := newHistoryExecutionRequestCancelSuite(t, store)
	suite.Run(t, s)
}

func TestMySQLHistoryExecutionSignalSuite(t *testing.T) {
	cfg := NewMySQLConfig()
	SetupMySQLDatabase(cfg)
	SetupMySQLSchema(cfg)
	store, err := sql.NewSQLDB(cfg)
	if err != nil {
		panic(fmt.Sprintf("unable to create MySQL DB: %v", err))
	}
	defer func() {
		_ = store.Close()
		TearDownMySQLDatabase(cfg)
	}()

	s := newHistoryExecutionSignalSuite(t, store)
	suite.Run(t, s)
}

func TestMySQLHistoryExecutionSignalRequestSuite(t *testing.T) {
	cfg := NewMySQLConfig()
	SetupMySQLDatabase(cfg)
	SetupMySQLSchema(cfg)
	store, err := sql.NewSQLDB(cfg)
	if err != nil {
		panic(fmt.Sprintf("unable to create MySQL DB: %v", err))
	}
	defer func() {
		_ = store.Close()
		TearDownMySQLDatabase(cfg)
	}()

	s := newHistoryExecutionSignalRequestSuite(t, store)
	suite.Run(t, s)
}

// NewMySQLConfig returns a new MySQL config for test
func NewMySQLConfig() *config.SQL {
	return &config.SQL{
		User:     testMySQLUser,
		Password: testMySQLPassword,
		ConnectAddr: net.JoinHostPort(
			environment.GetMySQLAddress(),
			strconv.Itoa(environment.GetMySQLPort()),
		),
		ConnectProtocol: testMySQLConnectionProtocol,
		PluginName:      "mysql",
		DatabaseName:    testMySQLDatabaseNamePrefix + shuffle.String(testMySQLDatabaseNameSuffix),
	}
}

func SetupMySQLDatabase(cfg *config.SQL) {
	adminCfg := *cfg
	// NOTE need to connect with empty name to create new database
	adminCfg.DatabaseName = ""

	db, err := sql.NewSQLAdminDB(&adminCfg)
	if err != nil {
		panic(fmt.Sprintf("unable to create MySQL admin DB: %v", err))
	}
	defer func() { _ = db.Close() }()

	err = db.CreateDatabase(cfg.DatabaseName)
	if err != nil {
		panic(fmt.Sprintf("unable to create MySQL database: %v", err))
	}
}

func SetupMySQLSchema(cfg *config.SQL) {
	db, err := sql.NewSQLAdminDB(cfg)
	if err != nil {
		panic(fmt.Sprintf("unable to create MySQL admin DB: %v", err))
	}
	defer func() { _ = db.Close() }()

	schemaPath, err := filepath.Abs(testMySQLSchema)
	if err != nil {
		panic(err)
	}

	content, err := ioutil.ReadFile(schemaPath)
	if err != nil {
		panic(err)
	}
	err = db.Exec(string(content))
	if err != nil {
		panic(err)
	}
}

func TearDownMySQLDatabase(cfg *config.SQL) {
	adminCfg := *cfg
	// NOTE need to connect with empty name to create new database
	adminCfg.DatabaseName = ""

	db, err := sql.NewSQLAdminDB(&adminCfg)
	if err != nil {
		panic(fmt.Sprintf("unable to create MySQL admin DB: %v", err))
	}
	defer func() { _ = db.Close() }()

	err = db.DropDatabase(cfg.DatabaseName)
	if err != nil {
		panic(fmt.Sprintf("unable to create MySQL database: %v", err))
	}
}
