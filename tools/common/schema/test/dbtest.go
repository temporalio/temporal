package test

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/tests/testutils"
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
	rootDir := testutils.MkdirTemp(tb.T(), "", "dbClientTestDir")
	cqlFile := testutils.CreateTemp(tb.T(), rootDir, "parseCQLTest")

	_, err := cqlFile.WriteString(content)
	tb.NoError(err)
	stmts, err := persistence.LoadAndSplitQuery([]string{cqlFile.Name()})
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
