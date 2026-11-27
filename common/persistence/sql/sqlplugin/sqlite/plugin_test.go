package sqlite

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/resolver"
	driversqlite "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

func TestTransactionWriterReservation(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name            string
		transactionMode string
		reservesWriter  bool
	}{
		{name: "default", reservesWriter: true},
		{name: "immediate", transactionMode: "immediate", reservesWriter: true},
		{name: "deferred", transactionMode: "deferred"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := &config.SQL{
				DatabaseName: filepath.Join(t.TempDir(), "test.db"),
				ConnectAttributes: map[string]string{
					"mode":         "rwc",
					"cache":        "private",
					"journal_mode": "wal",
					"busy_timeout": "0",
				},
			}
			if tc.transactionMode != "" {
				cfg.ConnectAttributes["_txlock"] = tc.transactionMode
			}
			p := &plugin{}
			firstDB, err := p.createDBConnection(cfg, resolver.NewNoopResolver(), log.NewNoopLogger())
			require.NoError(t, err)
			t.Cleanup(func() { _ = firstDB.Close() })
			secondDB, err := p.createDBConnection(cfg, resolver.NewNoopResolver(), log.NewNoopLogger())
			require.NoError(t, err)
			t.Cleanup(func() { _ = secondDB.Close() })

			_, err = firstDB.ExecContext(t.Context(), "CREATE TABLE counter (value INTEGER)")
			require.NoError(t, err)
			_, err = firstDB.ExecContext(t.Context(), "INSERT INTO counter VALUES (0)")
			require.NoError(t, err)

			firstStore := newDB(sqlplugin.DbKindMain, cfg.DatabaseName, firstDB, nil, log.NewNoopLogger())
			firstTx, err := firstStore.BeginTx(t.Context())
			require.NoError(t, err)
			t.Cleanup(func() { _ = firstTx.Rollback() })
			var value int
			require.NoError(t, firstTx.(*db).conn.GetContext(t.Context(), &value, "SELECT value FROM counter"))
			require.Zero(t, value)

			_, competingWriteErr := secondDB.ExecContext(t.Context(), "UPDATE counter SET value = value + 1")
			_, firstWriteErr := firstTx.(*db).conn.ExecContext(t.Context(), "UPDATE counter SET value = value + 1")
			if tc.reservesWriter {
				require.NoError(t, firstWriteErr)
				sqliteErr, ok := competingWriteErr.(*driversqlite.Error)
				require.True(t, ok, "expected competing writer to be blocked, got %v", competingWriteErr)
				require.Equal(t, sqlite3.SQLITE_BUSY, sqliteErr.Code())
				require.NoError(t, firstTx.Commit())
				_, err = secondDB.ExecContext(t.Context(), "UPDATE counter SET value = value + 1")
				require.NoError(t, err)
				require.NoError(t, secondDB.GetContext(t.Context(), &value, "SELECT value FROM counter"))
				require.Equal(t, 2, value)
			} else {
				require.NoError(t, competingWriteErr)
				t.Logf("Write after competing commit: %v", firstWriteErr)
				sqliteErr, ok := firstWriteErr.(*driversqlite.Error)
				require.True(t, ok, "expected a stale snapshot, got %v", firstWriteErr)
				require.Equal(t, sqlite3.SQLITE_BUSY_SNAPSHOT, sqliteErr.Code())
			}
		})
	}
}
