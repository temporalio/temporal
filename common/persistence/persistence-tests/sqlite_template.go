package persistencetests

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
	"go.temporal.io/server/common/resolver"
)

// Creating a sqlite database means executing the ~100 statements of the temporal
// and visibility schemas. The driver is pure Go, so under the race detector that
// costs ~150ms, and every test cluster used to pay it. Build the schema once per
// process and hand each database a byte copy of the result instead; copying the
// (~600 KiB) file is several times cheaper, and 6x cheaper under -race.
//
// See FASTBOOT.md for the measurements and schema/sqlite/setup_bench_test.go for
// the benchmark.
var sqliteSchemaTemplate = sync.OnceValues(buildSQLiteSchemaTemplate)

// sqliteTemplateTestCluster is a [PersistenceTestCluster] that seeds its database
// from the shared schema template rather than running the schema DDL. Everything
// else, including teardown, is the plain SQL test cluster's behavior.
type sqliteTemplateTestCluster struct {
	*sql.TestCluster
	dbPath string
	logger log.Logger
}

func newSQLiteTemplateTestCluster(inner *sql.TestCluster, dbPath string, logger log.Logger) PersistenceTestCluster {
	return &sqliteTemplateTestCluster{TestCluster: inner, dbPath: dbPath, logger: logger}
}

func (c *sqliteTemplateTestCluster) SetupTestDatabase() {
	template, err := sqliteSchemaTemplate()
	if err != nil {
		c.logger.Fatal("Failed to build sqlite schema template", tag.Error(err))
	}
	if err := os.MkdirAll(filepath.Dir(c.dbPath), 0o755); err != nil {
		c.logger.Fatal("Failed to create sqlite database directory", tag.Error(err))
	}
	// The plugin only runs the schema for in-memory databases or when the "setup"
	// connect attribute is set, so a seeded file opens with no DDL at all.
	if err := os.WriteFile(c.dbPath, template, 0o600); err != nil {
		c.logger.Fatal("Failed to seed sqlite database from template", tag.Error(err))
	}
}

// buildSQLiteSchemaTemplate creates one database with the full schema and returns
// its file contents.
func buildSQLiteSchemaTemplate() ([]byte, error) {
	dir, err := os.MkdirTemp("", "temporal-sqlite-schema-template-")
	if err != nil {
		return nil, fmt.Errorf("create template dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	path := filepath.Join(dir, "template.db")
	// No journal_mode override: the default rollback journal leaves the whole
	// database in this one file once the connection is closed, so the bytes we
	// read back are complete.
	db, err := sql.NewSQLAdminDB(
		sqlplugin.DbKindUnknown,
		&config.SQL{
			PluginName:        sqlite.PluginName,
			DatabaseName:      path,
			ConnectAttributes: map[string]string{"setup": "true"},
		},
		resolver.NewNoopResolver(),
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
	)
	if err != nil {
		return nil, fmt.Errorf("open template database: %w", err)
	}
	if err := db.Close(); err != nil {
		return nil, fmt.Errorf("close template database: %w", err)
	}

	template, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read template database: %w", err)
	}
	if err := verifySQLiteTemplate(path); err != nil {
		return nil, err
	}
	return template, nil
}

// verifySQLiteTemplate guards against handing out a database whose schema never
// made it to disk, which would otherwise surface as confusing "no such table"
// errors in unrelated tests.
func verifySQLiteTemplate(path string) error {
	db, err := sql.NewSQLAdminDB(
		sqlplugin.DbKindMain,
		&config.SQL{PluginName: sqlite.PluginName, DatabaseName: path},
		resolver.NewNoopResolver(),
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
	)
	if err != nil {
		return fmt.Errorf("reopen template database: %w", err)
	}
	defer func() { _ = db.Close() }()

	tables, err := db.ListTables(path)
	if err != nil {
		return fmt.Errorf("list template tables: %w", err)
	}
	// A sanity floor, not an exact count: the schemas define ~40 tables plus
	// indices, and this only needs to catch an empty or truncated template.
	const minExpectedTables = 20
	if len(tables) < minExpectedTables {
		return fmt.Errorf("sqlite schema template has %d tables, expected at least %d", len(tables), minExpectedTables)
	}
	return nil
}

// sqliteSchemaTemplateSupported reports whether options describe a file-backed
// sqlite database that can be seeded from the template.
func sqliteSchemaTemplateSupported(options *TestBaseOptions) bool {
	return options.SQLDBPluginName == sqlite.PluginName &&
		options.ConnectAttributes["mode"] != "memory" &&
		options.ConnectAttributes["setup"] == "" &&
		options.DBName != "" &&
		options.DBName != ":memory:"
}

// sqliteTestDir is where seeded test databases live. Keeping them out of the
// repo and under one root makes them easy to point at a tmpfs in CI via TMPDIR.
func sqliteTestDir() string {
	return filepath.Join(os.TempDir(), "temporal-test-sqlite")
}
