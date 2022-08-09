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

package integration

import (
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/shuffle"
	"go.temporal.io/server/environment"
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
	testMySQLExecutionSchema  = "../../schema/mysql/v57/temporal/schema.sql"
	testMySQLVisibilitySchema = "../../schema/mysql/v57/visibility/schema.sql"
)

func TestMySQLSizeLimitSuite(t *testing.T) {
	cfg := NewMySQLConfig()
	SetupMySQLDatabase(cfg)
	SetupMySQLSchema(cfg)
	logger := log.NewNoopLogger()
	defer func() { TearDownMySQLDatabase(cfg) }()

	s := newSizeLimitSuite(t, logger)
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

	db, err := sql.NewSQLAdminDB(sqlplugin.DbKindUnknown, &adminCfg, resolver.NewNoopResolver())
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
	db, err := sql.NewSQLAdminDB(sqlplugin.DbKindUnknown, cfg, resolver.NewNoopResolver())
	if err != nil {
		panic(fmt.Sprintf("unable to create MySQL admin DB: %v", err))
	}
	defer func() { _ = db.Close() }()

	schemaPath, err := filepath.Abs(testMySQLExecutionSchema)
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

	schemaPath, err = filepath.Abs(testMySQLVisibilitySchema)
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

func TearDownMySQLDatabase(cfg *config.SQL) {
	adminCfg := *cfg
	// NOTE need to connect with empty name to create new database
	adminCfg.DatabaseName = ""

	db, err := sql.NewSQLAdminDB(sqlplugin.DbKindUnknown, &adminCfg, resolver.NewNoopResolver())
	if err != nil {
		panic(fmt.Sprintf("unable to create MySQL admin DB: %v", err))
	}
	defer func() { _ = db.Close() }()

	err = db.DropDatabase(cfg.DatabaseName)
	if err != nil {
		panic(fmt.Sprintf("unable to drop MySQL database: %v", err))
	}
}
