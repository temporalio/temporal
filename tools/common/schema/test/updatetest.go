package test

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/urfave/cli"

	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/loggerimpl"
	"github.com/temporalio/temporal/common/log/tag"
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
	tb.Nil(err)
	// update the version to the latest
	tb.Log.Info(ver)
	tb.Equal(ver, endVersion)
	tb.NoError(db.DropAllTables())
}

// RunUpdateSchemaTest tests schema update
func (tb *UpdateSchemaTestBase) RunUpdateSchemaTest(app *cli.App, db DB, dbNameFlag string, sqlFileContent string, expectedTables []string) {
	tmpDir, err := ioutil.TempDir("", "update_schema_test")
	tb.Nil(err)
	defer os.RemoveAll(tmpDir)

	tb.makeSchemaVersionDirs(tmpDir, sqlFileContent)

	tb.NoError(app.Run([]string{"./tool", dbNameFlag, tb.DBName, "-q", "setup-schema", "-v", "0.0"}))
	tb.NoError(app.Run([]string{"./tool", dbNameFlag, tb.DBName, "-q", "update-schema", "-d", tmpDir, "-v", "2.0"}))

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
	err := ioutil.WriteFile(dir+"/manifest.json", []byte(mData), os.FileMode(0600))
	tb.Nil(err)
	err = ioutil.WriteFile(dir+"/base.sql", []byte(sqlFileContent), os.FileMode(0600))
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
	err = ioutil.WriteFile(dir+"/manifest.json", []byte(mData), os.FileMode(0600))
	tb.Nil(err)
	err = ioutil.WriteFile(dir+"/namespace.cql", []byte(namespace), os.FileMode(0600))
	tb.Nil(err)
}
