package sqlite_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
	"go.temporal.io/server/common/resolver"
)

// Every functional test cluster gets its own sqlite database, and for the
// in-memory mode used by default the plugin runs the full schema on first
// connect. These benchmarks size that cost against the alternative of building
// the schema once per process and copying it, which matters because the schema
// DDL is the whole of the test cluster's persistence startup phase (see
// FASTBOOT.md).
//
//	go test -tags test_dep ./schema/sqlite -run '^$' -bench BenchmarkSchemaSetup -benchtime 20x
//
// Run it with -race too: the driver is pure Go, so the race detector instruments
// every page operation and the gap between the two approaches widens sharply.

// BenchmarkSchemaSetupInMemory measures what a test cluster pays today: open a
// fresh in-memory database, which triggers the plugin's schema setup hook.
func BenchmarkSchemaSetupInMemory(b *testing.B) {
	i := 0
	for b.Loop() {
		i++
		db, err := sql.NewSQLAdminDB(
			sqlplugin.DbKindUnknown,
			&config.SQL{
				PluginName:        sqlite.PluginName,
				DatabaseName:      fmt.Sprintf("bench_%d", i),
				ConnectAttributes: map[string]string{"mode": "memory", "cache": "private"},
			},
			resolver.NewNoopResolver(),
			log.NewNoopLogger(),
			metrics.NoopMetricsHandler,
		)
		require.NoError(b, err)

		b.StopTimer()
		require.NoError(b, db.Close())
		b.StartTimer()
	}
}

// BenchmarkSchemaSetupTemplateCopy measures the alternative: build the schema
// once into a template file, then give each cluster a byte copy of it and open
// it with no DDL at all.
func BenchmarkSchemaSetupTemplateCopy(b *testing.B) {
	dir := b.TempDir()
	template := filepath.Join(dir, "template.db")
	templateDB, err := sql.NewSQLAdminDB(
		sqlplugin.DbKindUnknown,
		&config.SQL{
			PluginName:        sqlite.PluginName,
			DatabaseName:      template,
			ConnectAttributes: map[string]string{"setup": "true"},
		},
		resolver.NewNoopResolver(),
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
	)
	require.NoError(b, err)
	require.NoError(b, templateDB.Close())

	golden, err := os.ReadFile(template)
	require.NoError(b, err)
	b.Logf("template size: %d KiB", len(golden)/1024)

	i := 0
	for b.Loop() {
		i++
		path := filepath.Join(dir, fmt.Sprintf("bench_%d.db", i))
		require.NoError(b, os.WriteFile(path, golden, 0o600))
		db, err := sql.NewSQLAdminDB(
			sqlplugin.DbKindUnknown,
			&config.SQL{
				PluginName:        sqlite.PluginName,
				DatabaseName:      path,
				ConnectAttributes: map[string]string{"journal_mode": "wal", "synchronous": "off"},
			},
			resolver.NewNoopResolver(),
			log.NewNoopLogger(),
			metrics.NoopMetricsHandler,
		)
		require.NoError(b, err)

		b.StopTimer()
		require.NoError(b, db.Close())
		require.NoError(b, os.Remove(path))
		b.StartTimer()
	}
}
