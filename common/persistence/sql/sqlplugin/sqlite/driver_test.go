package sqlite

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

func TestIsDupEntryError(t *testing.T) {
	t.Parallel()

	dsn := "file:" + filepath.Join(t.TempDir(), "test.db") + "?mode=rwc&cache=private&_pragma=busy_timeout(0)"
	conn, err := sqlx.Connect(goSQLDriverName, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	_, err = conn.Exec(`CREATE TABLE t (
		id INTEGER PRIMARY KEY,
		uniq TEXT UNIQUE,
		nn TEXT NOT NULL
	)`)
	require.NoError(t, err)
	_, err = conn.Exec(`INSERT INTO t (id, uniq, nn) VALUES (1, 'a', 'x')`)
	require.NoError(t, err)

	ctx := t.Context()
	d := &db{}
	for _, tc := range []struct {
		name     string
		sql      string
		wantCode int
		wantDup  bool
		holdLock bool
	}{
		{
			name:     "primary key",
			sql:      `INSERT INTO t (id, uniq, nn) VALUES (1, 'b', 'y')`,
			wantCode: sqlite3.SQLITE_CONSTRAINT_PRIMARYKEY,
			wantDup:  true,
		},
		{
			name:     "unique",
			sql:      `INSERT INTO t (id, uniq, nn) VALUES (2, 'a', 'y')`,
			wantCode: sqlite3.SQLITE_CONSTRAINT_UNIQUE,
			wantDup:  true,
		},
		{
			name:     "not null",
			sql:      `INSERT INTO t (id, uniq) VALUES (3, 'c')`,
			wantCode: sqlite3.SQLITE_CONSTRAINT_NOTNULL,
			wantDup:  false,
		},
		{
			name:     "missing table",
			sql:      `SELECT * FROM missing`,
			wantCode: sqlite3.SQLITE_ERROR,
			wantDup:  false,
		},
		{
			name:     "busy",
			sql:      `INSERT INTO t (id, uniq, nn) VALUES (4, 'd', 'z')`,
			wantCode: sqlite3.SQLITE_BUSY,
			wantDup:  false,
			holdLock: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var execErr error
			if tc.holdLock {
				conn2, err := sqlx.Connect(goSQLDriverName, dsn)
				require.NoError(t, err)
				t.Cleanup(func() { _ = conn2.Close() })

				lock, err := conn2.Conn(ctx)
				require.NoError(t, err)
				t.Cleanup(func() { _ = lock.Close() })

				_, err = lock.ExecContext(ctx, "BEGIN IMMEDIATE")
				require.NoError(t, err)
				t.Cleanup(func() { _, _ = lock.ExecContext(ctx, "ROLLBACK") })

				_, execErr = conn.ExecContext(ctx, tc.sql)
			} else {
				_, execErr = conn.Exec(tc.sql)
			}
			require.Error(t, execErr)

			sqlErr, ok := errors.AsType[*sqlite.Error](execErr)
			require.True(t, ok)
			require.Equal(t, tc.wantCode, sqlErr.Code())
			require.Equal(t, tc.wantDup, d.IsDupEntryError(execErr))
		})
	}
}
