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
	"os"
	"time"

	"go.temporal.io/server/tests/testhelper"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/urfave/cli"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

// UpdateSchemaTestBase is the base test suite for all tests
// that exercise schema update using the schema tool
type UpdateSchemaTestBase struct {
	suite.Suite
	*require.Assertions
	rand       *rand.Rand
	Logger     log.Logger
	DBName     string
	db         DB
	pluginName string
}

// SetupSuiteBase sets up the test suite
func (tb *UpdateSchemaTestBase) SetupSuiteBase(db DB, pluginName string) {
	tb.Assertions = require.New(tb.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, tb.T() will return nil
	tb.Logger = log.NewTestLogger()
	tb.rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	tb.DBName = fmt.Sprintf("update_test_%v", tb.rand.Int63())
	err := db.CreateDatabase(tb.DBName)
	if err != nil {
		tb.Logger.Fatal("error creating database, ", tag.Error(err))
	}
	tb.db = db
	tb.pluginName = pluginName
}

// TearDownSuiteBase tears down the test suite
func (tb *UpdateSchemaTestBase) TearDownSuiteBase() {
	tb.NoError(tb.db.DropDatabase(tb.DBName))
	tb.db.Close()
}

// RunDryrunTest tests a dryrun schema setup & update
func (tb *UpdateSchemaTestBase) RunDryrunTest(app *cli.App, db DB, dbNameFlag string, dir string, endVersion string) {
	command := append(tb.getCommandBase(), []string{
		dbNameFlag, tb.DBName,
		"-q",
		"setup-schema",
		"-v", "0.0",
	}...)
	tb.NoError(app.Run(command))

	command = append(tb.getCommandBase(), []string{
		dbNameFlag, tb.DBName,
		"-q",
		"update-schema",
		"-d", dir,
	}...)
	tb.NoError(app.Run(command))
	ver, err := db.ReadSchemaVersion()
	tb.Nil(err)
	// update the version to the latest
	tb.Logger.Info(ver)
	tb.Equal(endVersion, ver)
	tb.NoError(db.DropAllTables())
}

// RunUpdateSchemaTest tests schema update
func (tb *UpdateSchemaTestBase) RunUpdateSchemaTest(app *cli.App, db DB, dbNameFlag string, sqlFileContent string, expectedTables []string) {
	tmpDir := testhelper.MkdirTemp(tb.T(), "", "update_schema_test")

	tb.makeSchemaVersionDirs(tmpDir, sqlFileContent)

	command := append(tb.getCommandBase(), []string{
		dbNameFlag, tb.DBName,
		"-q",
		"setup-schema",
		"-v", "0.0",
	}...)
	tb.NoError(app.Run(command))

	command = append(tb.getCommandBase(), []string{
		dbNameFlag, tb.DBName,
		"-q",
		"update-schema",
		"-d", tmpDir,
		"-v", "2.0",
	}...)
	tb.NoError(app.Run(command))

	expected := getExpectedTables(true, expectedTables)
	expected["namespaces"] = struct{}{}

	ver, err := db.ReadSchemaVersion()
	tb.Nil(err)
	tb.Equal("2.0", ver)

	tables, err := db.ListTables()
	tb.Nil(err)
	tb.Equal(len(expected), len(tables))

	for _, t := range tables {
		_, ok := expected[t]
		tb.True(ok)
		delete(expected, t)
	}

	tb.Equal(0, len(expected))
	tb.NoError(db.DropAllTables())
}

func (tb *UpdateSchemaTestBase) makeSchemaVersionDirs(rootDir string, sqlFileContent string) {
	mData := `{
		"CurrVersion": "1.0",
		"MinCompatibleVersion": "1.0",
		"Description": "base version of schema",
		"SchemaUpdateCqlFiles": ["base.sql"]
	}`

	dir := rootDir + "/v1.0"
	tb.NoError(os.Mkdir(rootDir+"/v1.0", os.FileMode(0700)))
	err := os.WriteFile(dir+"/manifest.json", []byte(mData), os.FileMode(0600))
	tb.Nil(err)
	err = os.WriteFile(dir+"/base.sql", []byte(sqlFileContent), os.FileMode(0600))
	tb.Nil(err)

	mData = `{
		"CurrVersion": "2.0",
		"MinCompatibleVersion": "1.0",
		"Description": "v2 of schema",
		"SchemaUpdateCqlFiles": ["namespace.cql"]
	}`

	namespace := `CREATE TABLE namespaces(
	  id     int,
	  PRIMARY KEY (id)
	);`

	dir = rootDir + "/v2.0"
	tb.NoError(os.Mkdir(rootDir+"/v2.0", os.FileMode(0700)))
	err = os.WriteFile(dir+"/manifest.json", []byte(mData), os.FileMode(0600))
	tb.Nil(err)
	err = os.WriteFile(dir+"/namespace.cql", []byte(namespace), os.FileMode(0600))
	tb.Nil(err)
}

func (tb *UpdateSchemaTestBase) getCommandBase() []string {
	command := []string{"./tool"}
	if tb.pluginName != "" {
		command = append(command, []string{
			"-pl", tb.pluginName,
		}...)
	}
	return command
}
