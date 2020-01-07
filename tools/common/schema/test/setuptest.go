// Copyright (c) 2017 Uber Technologies, Inc.
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
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/urfave/cli"

	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/log/tag"
)

// SetupSchemaTestBase is the base test suite for all tests
// that exercise schema setup using the schema tool
type SetupSchemaTestBase struct {
	suite.Suite
	*require.Assertions
	rand   *rand.Rand
	Log    log.Logger
	DBName string
	db     DB
}

// SetupSuiteBase sets up the test suite
func (tb *SetupSchemaTestBase) SetupSuiteBase(db DB) {
	tb.Assertions = require.New(tb.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, tb.T() will return nil
	var err error
	tb.Log, err = loggerimpl.NewDevelopment()
	tb.Require().NoError(err)
	tb.rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	tb.DBName = fmt.Sprintf("setup_test_%v", tb.rand.Int63())
	err = db.CreateDatabase(tb.DBName)
	if err != nil {
		tb.Log.Fatal("error creating database, ", tag.Error(err))
	}
	tb.db = db
}

// TearDownSuiteBase tears down the test suite
func (tb *SetupSchemaTestBase) TearDownSuiteBase() {
	tb.NoError(tb.db.DropDatabase(tb.DBName))
	tb.db.Close()
}

// RunSetupTest exercises the SetupSchema task
func (tb *SetupSchemaTestBase) RunSetupTest(
	app *cli.App, db DB, dbNameFlag string, sqlFileContent string, expectedTables []string) {
	// test command fails without required arguments
	tb.NoError(app.Run([]string{"./tool", dbNameFlag, tb.DBName, "-q", "setup-schema"}))
	tables, err := db.ListTables()
	tb.Nil(err)
	tb.Equal(0, len(tables))

	tmpDir, err := ioutil.TempDir("", "setupSchemaTestDir")
	tb.Nil(err)
	defer os.Remove(tmpDir)

	sqlFile, err := ioutil.TempFile(tmpDir, "setupSchema.cliOptionsTest")
	tb.Nil(err)
	defer os.Remove(sqlFile.Name())

	_, err = sqlFile.WriteString(sqlFileContent)
	tb.NoError(err)

	// make sure command doesn't succeed without version or disable-version
	tb.NoError(app.Run([]string{"./tool", dbNameFlag, tb.DBName, "-q", "setup-schema", "-f", sqlFile.Name()}))
	tables, err = db.ListTables()
	tb.Nil(err)
	tb.Equal(0, len(tables))

	for i := 0; i < 4; i++ {

		ver := strconv.Itoa(int(tb.rand.Int31()))
		versioningEnabled := (i%2 == 0)

		// test overwrite with versioning works
		if versioningEnabled {
			tb.NoError(app.Run([]string{"./tool", dbNameFlag, tb.DBName, "-q", "setup-schema", "-f", sqlFile.Name(), "-version", ver, "-o"}))
		} else {
			tb.NoError(app.Run([]string{"./tool", dbNameFlag, tb.DBName, "-q", "setup-schema", "-f", sqlFile.Name(), "-d", "-o"}))
		}

		expectedTables := getExpectedTables(versioningEnabled, expectedTables)
		tables, err = db.ListTables()
		tb.Nil(err)
		tb.Equal(len(expectedTables), len(tables))

		for _, t := range tables {
			_, ok := expectedTables[t]
			tb.True(ok)
			delete(expectedTables, t)
		}
		tb.Equal(0, len(expectedTables))

		gotVer, err := db.ReadSchemaVersion()
		if versioningEnabled {
			tb.Nil(err)
			tb.Equal(ver, gotVer)
		} else {
			tb.NotNil(err)
		}
	}
}

func getExpectedTables(versioningEnabled bool, wantTables []string) map[string]struct{} {
	expectedTables := make(map[string]struct{})
	for _, tab := range wantTables {
		expectedTables[tab] = struct{}{}
		expectedTables[tab] = struct{}{}
	}
	if versioningEnabled {
		expectedTables["schema_version"] = struct{}{}
		expectedTables["schema_update_history"] = struct{}{}
	}
	return expectedTables
}
