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
	"go.temporal.io/server/common/persistence/sql/sqlplugin/tidb"
	"net"
	"path/filepath"
	"strconv"
	"testing"

	"go.uber.org/zap/zaptest"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/shuffle"
	"go.temporal.io/server/environment"
)

// These test cases adopted from TiDB_test.go
// TODO merge the initialization with existing persistence setup
const (
	testTiDBClusterName = "temporal_tidb_cluster"

	testTiDBUser               = "temporal"
	testTiDBPassword           = "temporal"
	testTiDBConnectionProtocol = "tcp"
	testTiDBDatabaseNamePrefix = "test_"
	testTiDBDatabaseNameSuffix = "temporal_persistence"

	// TODO hard code this dir for now
	//  need to merge persistence test config / initialization in one place
	testTiDBExecutionSchema  = "../../../schema/tidb/v8/temporal/schema.sql"
	testTiDBVisibilitySchema = "../../../schema/tidb/v8/visibility/schema.sql"
)

type (
	TiDBTestData struct {
		Cfg     *config.SQL
		Factory *sql.Factory
		Logger  log.Logger
	}
)

func setUpTiDBTest(t *testing.T) (TiDBTestData, func()) {
	var testData TiDBTestData
	testData.Cfg = NewTiDBConfig()
	testData.Logger = log.NewZapLogger(zaptest.NewLogger(t))
	SetupTiDBDatabase(testData.Cfg)
	SetupTiDBSchema(testData.Cfg)

	testData.Factory = sql.NewFactory(
		*testData.Cfg,
		resolver.NewNoopResolver(),
		testTiDBClusterName,
		testData.Logger,
	)

	tearDown := func() {
		testData.Factory.Close()
		TearDownTiDBDatabase(testData.Cfg)
	}

	return testData, tearDown
}

// NewTiDBConfig returns a new TiDB config for test
func NewTiDBConfig() *config.SQL {
	return &config.SQL{
		User:     testTiDBUser,
		Password: testTiDBPassword,
		ConnectAddr: net.JoinHostPort(
			environment.GetTiDBAddress(),
			strconv.Itoa(environment.GetTiDBPort()),
		),
		ConnectProtocol: testTiDBConnectionProtocol,
		PluginName:      tidb.PluginName,
		DatabaseName:    testTiDBDatabaseNamePrefix + shuffle.String(testTiDBDatabaseNameSuffix),
	}
}

func SetupTiDBDatabase(cfg *config.SQL) {
	adminCfg := *cfg
	// NOTE need to connect with empty name to create new database
	adminCfg.DatabaseName = ""

	db, err := sql.NewSQLAdminDB(sqlplugin.DbKindUnknown, &adminCfg, resolver.NewNoopResolver())
	if err != nil {
		panic(fmt.Sprintf("unable to create TiDB admin DB: %v", err))
	}
	defer func() { _ = db.Close() }()

	err = db.CreateDatabase(cfg.DatabaseName)
	if err != nil {
		panic(fmt.Sprintf("unable to create TiDB database: %v", err))
	}
}

func SetupTiDBSchema(cfg *config.SQL) {
	db, err := sql.NewSQLAdminDB(sqlplugin.DbKindUnknown, cfg, resolver.NewNoopResolver())
	if err != nil {
		panic(fmt.Sprintf("unable to create TiDB admin DB: %v", err))
	}
	defer func() { _ = db.Close() }()

	schemaPath, err := filepath.Abs(testTiDBExecutionSchema)
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

	schemaPath, err = filepath.Abs(testTiDBVisibilitySchema)
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

func TearDownTiDBDatabase(cfg *config.SQL) {
	adminCfg := *cfg
	// NOTE need to connect with empty name to create new database
	adminCfg.DatabaseName = ""

	db, err := sql.NewSQLAdminDB(sqlplugin.DbKindUnknown, &adminCfg, resolver.NewNoopResolver())
	if err != nil {
		panic(fmt.Sprintf("unable to create TiDB admin DB: %v", err))
	}
	defer func() { _ = db.Close() }()

	err = db.DropDatabase(cfg.DatabaseName)
	if err != nil {
		panic(fmt.Sprintf("unable to drop TiDB database: %v", err))
	}
}
