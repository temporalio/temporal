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
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/hashicorp/go-version"
	"github.com/otiai10/copy"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/urfave/cli"

	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/log/tag"
)

// UpdateSchemaTestBase is the base test suite for all tests
// that exercise schema update using the schema tool
type UpdateSchemaTestBase struct {
	suite.Suite
	*require.Assertions
	rand   *rand.Rand
	Log    log.Logger
	DBName string
	db     DB
}

// SetupSuiteBase sets up the test suite
func (tb *UpdateSchemaTestBase) SetupSuiteBase(db DB) {
	tb.Assertions = require.New(tb.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, tb.T() will return nil
	var err error
	tb.Log, err = loggerimpl.NewDevelopment()
	tb.Require().NoError(err)
	tb.rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	tb.DBName = fmt.Sprintf("update_test_%v", tb.rand.Int63())
	err = db.CreateDatabase(tb.DBName)
	if err != nil {
		tb.Log.Fatal("error creating database, ", tag.Error(err))
	}
	tb.db = db
}

// TearDownSuiteBase tears down the test suite
func (tb *UpdateSchemaTestBase) TearDownSuiteBase() {
	tb.NoError(tb.db.DropDatabase(tb.DBName))
	tb.db.Close()
}

// RunDryrunTest tests a dryrun schema setup & update
func (tb *UpdateSchemaTestBase) RunDryrunTest(app *cli.App, db DB, dbNameFlag string, dir string, endVersion string) {
	tb.NoError(app.Run([]string{"./tool", dbNameFlag, tb.DBName, "-q", "setup-schema", "-v", "0.0"}))
	tb.NoError(app.Run([]string{"./tool", dbNameFlag, tb.DBName, "-q", "update-schema", "-d", dir}))
	ver, err := db.ReadSchemaVersion()
	tb.NoError(err)
	// update the version to the latest
	tb.Log.Info(ver)
	tb.Equal(ver, endVersion)
	tb.NoError(db.DropAllTables())
}

// RunShortcutTest checks that shortcut changes match incremental changes
func (tb *UpdateSchemaTestBase) RunShortcutTest(app *cli.App, db DB, dbNameFlag string, dir string, schemaCmd string, schemaCmdArgs ...string) {
	tmpDir, err := ioutil.TempDir("", "shortcut_test")
	tb.NoError(err)
	defer os.RemoveAll(tmpDir)

	versionStrRegex := regexp.MustCompile(`^v\d+(\.\d+)?$`)
	squashVersionStrRegex := regexp.MustCompile(`^s\d+(\.\d+)?-\d+(\.\d+)?$`)

	contents, err := ioutil.ReadDir(dir)
	tb.NoError(err)

	// process the flags for schema tool
	var processedArgs []string
	for _, arg := range schemaCmdArgs {
		if strings.Contains(arg, "%s") {
			processedArgs = append(processedArgs, fmt.Sprintf(arg, tb.DBName))
			continue
		}
		processedArgs = append(processedArgs, arg)
	}

	// a noop schema tool run, to fail early if the command does not exist
	// or the flags are invalid
	co, err := exec.Command(schemaCmd, processedArgs...).CombinedOutput()
	tb.Require().NoError(err, "error running noop schema command %q with arguments %#v\n output: %s",
		schemaCmd, processedArgs, string(co))

	squashes := map[string]string{}
	var targetVersions []string
	// copy the incremental versions and gather shortcut info
	for _, d := range contents {
		if d.IsDir() {
			if versionStrRegex.MatchString(d.Name()) {
				tb.NoError(copy.Copy(filepath.Join(dir, d.Name()), filepath.Join(tmpDir, d.Name())))
			}
			if squashVersionStrRegex.MatchString(d.Name()) {
				splits := strings.Split(d.Name()[1:], "-")
				tb.Equal(2, len(splits))
				squashes[d.Name()] = splits[1]
				targetVersions = append(targetVersions, splits[1])
			}
		}
	}

	// sort the target versions
	sort.Slice(targetVersions, func(i, j int) bool {
		v1, err := version.NewVersion(targetVersions[i])
		tb.NoError(err)
		v2, err := version.NewVersion(targetVersions[j])
		tb.NoError(err)

		return v1.Compare(v2) < 0
	})

	expectedState := map[string]string{}

	tb.NoError(app.Run([]string{"./tool", dbNameFlag, tb.DBName, "-q", "setup-schema", "-v", "0.0"}))
	for _, tv := range targetVersions {
		tb.NoError(app.Run([]string{"./tool", dbNameFlag, tb.DBName, "-q", "update-schema", "-d", tmpDir, "-v", tv}))
		v, err := db.ReadSchemaVersion()
		tb.NoError(err)
		tb.Equal(tv, v)

		o, err := exec.Command(schemaCmd, processedArgs...).Output()
		tb.Require().NoError(err, "error exporting schema: command %q with arguments %#v\n output: %s",
			schemaCmd, processedArgs, string(o))

		expectedState[tv] = string(o)
	}

	tb.NoError(db.DropAllTables())

	for sDirName, ver := range squashes {
		tb.Run(sDirName, func() {
			tmpDestDir := filepath.Join(tmpDir, sDirName)
			copy.Copy(filepath.Join(dir, sDirName), tmpDestDir)
			defer os.RemoveAll(tmpDestDir)
			defer func() { tb.NoError(db.DropAllTables()) }()

			tb.NoError(app.Run([]string{"./tool", dbNameFlag, tb.DBName, "-q", "setup-schema", "-v", "0.0"}))
			tb.NoError(app.Run([]string{"./tool", dbNameFlag, tb.DBName, "-q", "update-schema", "-d", tmpDir, "-v", ver}))

			vo, err := exec.Command(schemaCmd, processedArgs...).Output()
			tb.Require().NoError(err, "error exporting schema: command %q with arguments %#v\n output: %s",
				schemaCmd, processedArgs, string(vo))

			tb.Equal(expectedState[ver], string(vo))
		})
	}
}

// RunUpdateSchemaTest tests schema update
func (tb *UpdateSchemaTestBase) RunUpdateSchemaTest(app *cli.App, db DB, dbNameFlag string, sqlFileContent string, expectedTables []string) {
	tmpDir, err := ioutil.TempDir("", "update_schema_test")
	tb.NoError(err)
	defer os.RemoveAll(tmpDir)

	tb.makeSchemaVersionDirs(tmpDir, sqlFileContent)

	tb.NoError(app.Run([]string{"./tool", dbNameFlag, tb.DBName, "-q", "setup-schema", "-v", "0.0"}))
	tb.NoError(app.Run([]string{"./tool", dbNameFlag, tb.DBName, "-q", "update-schema", "-d", tmpDir, "-v", "2.0"}))

	expectedMap := getExpectedTables(true, expectedTables)
	expectedMap["domains"] = struct{}{}
	var expected []string
	for t := range expectedMap {
		expected = append(expected, t)
	}

	ver, err := db.ReadSchemaVersion()
	tb.NoError(err)
	tb.Equal("2.0", ver)

	tables, err := db.ListTables()
	tb.NoError(err)
	tb.ElementsMatch(expected, tables)

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
	err := ioutil.WriteFile(dir+"/manifest.json", []byte(mData), os.FileMode(0600))
	tb.NoError(err)
	err = ioutil.WriteFile(dir+"/base.sql", []byte(sqlFileContent), os.FileMode(0600))
	tb.NoError(err)

	mData = `{
		"CurrVersion": "2.0",
		"MinCompatibleVersion": "1.0",
		"Description": "v2 of schema",
		"SchemaUpdateCqlFiles": ["domain.cql"]
	}`

	domain := `CREATE TABLE domains(
	  id     int,
	  PRIMARY KEY (id)
	);`

	dir = rootDir + "/v2.0"
	tb.NoError(os.Mkdir(rootDir+"/v2.0", os.FileMode(0700)))
	err = ioutil.WriteFile(dir+"/manifest.json", []byte(mData), os.FileMode(0600))
	tb.NoError(err)
	err = ioutil.WriteFile(dir+"/domain.cql", []byte(domain), os.FileMode(0600))
	tb.NoError(err)
}
