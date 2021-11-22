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

package test

import (
	"fmt"
	"math/rand"
	"time"

	"go.temporal.io/server/tests/testhelper"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/tools/common/schema"
)

type (
	// DB is a test interface for a database that supports schema tool
	DB interface {
		schema.DB
		CreateDatabase(name string) error
		DropDatabase(name string) error
		ListTables() ([]string, error)
	}
	// DBTestBase is the base for all test suites that test
	// the functionality of a DB implementation
	DBTestBase struct {
		suite.Suite
		*require.Assertions
		Log    log.Logger
		DBName string
		db     DB
	}
)

// SetupSuiteBase sets up the test suite
func (tb *DBTestBase) SetupSuiteBase(db DB) {
	tb.Assertions = require.New(tb.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, tb.T() will return nil
	tb.Log = log.NewTestLogger()
	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	tb.DBName = fmt.Sprintf("db_client_test_%v", rand.Int63())
	err := db.CreateDatabase(tb.DBName)
	if err != nil {
		tb.Log.Fatal("error creating database, ", tag.Error(err))
	}
	tb.db = db
}

// TearDownSuiteBase tears down the test suite
func (tb *DBTestBase) TearDownSuiteBase() {
	tb.NoError(tb.db.DropDatabase(tb.DBName))
	tb.db.Close()
}

// RunParseFileTest runs a test against the ParseFile method
func (tb *DBTestBase) RunParseFileTest(content string) {
	rootDir := testhelper.MkdirTemp(tb.T(), "", "dbClientTestDir")
	cqlFile := testhelper.CreateTemp(tb.T(), rootDir, "parseCQLTest")

	_, err := cqlFile.WriteString(content)
	tb.NoError(err)
	stmts, err := schema.ParseFile(cqlFile.Name())
	tb.Nil(err)
	tb.Equal(2, len(stmts), "wrong number of sql statements")
}

// RunCreateTest tests schema version table creation
func (tb *DBTestBase) RunCreateTest(db DB) {
	tables, err := db.ListTables()
	tb.Nil(err)
	tb.Equal(0, len(tables))

	err = db.CreateSchemaVersionTables()
	tb.Nil(err)

	expectedTables := make(map[string]struct{})
	expectedTables["schema_version"] = struct{}{}
	expectedTables["schema_update_history"] = struct{}{}

	tables, err = db.ListTables()
	tb.Nil(err)
	tb.Equal(len(expectedTables), len(tables))

	for _, t := range tables {
		_, ok := expectedTables[t]
		tb.True(ok)
		delete(expectedTables, t)
	}
	tb.Equal(0, len(expectedTables))
}

// RunUpdateTest tests update of schema and schema version tables
func (tb *DBTestBase) RunUpdateTest(db DB) {
	err := db.UpdateSchemaVersion("10.0", "5.0")
	tb.Nil(err)
	err = db.WriteSchemaUpdateLog("9.0", "10.0", "abc", "test")
	tb.Nil(err)

	ver, err := db.ReadSchemaVersion()
	tb.Nil(err)
	tb.Equal("10.0", ver)

	err = db.UpdateSchemaVersion("12.0", "5.0")
	tb.Nil(err)
	ver, err = db.ReadSchemaVersion()
	tb.Nil(err)
	tb.Equal("12.0", ver)
}

// RunDropTest tests the drop methods in DB implementation
func (tb *DBTestBase) RunDropTest(db DB) {
	tables, err := db.ListTables()
	tb.Nil(err)
	tb.True(len(tables) > 0)

	err = db.DropAllTables()
	tb.Nil(err)

	tables, err = db.ListTables()
	tb.Nil(err)
	tb.Equal(0, len(tables))

	_, err = db.ReadSchemaVersion()
	tb.NotNil(err)

}
