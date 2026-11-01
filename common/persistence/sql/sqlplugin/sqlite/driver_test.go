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

	_, dupPKErr := conn.Exec(`INSERT INTO t (id, uniq, nn) VALUES (1, 'b', 'y')`)
	require.Error(t, dupPKErr)
	_, dupUniqueErr := conn.Exec(`INSERT INTO t (id, uniq, nn) VALUES (2, 'a', 'y')`)
	require.Error(t, dupUniqueErr)
	_, notNullErr := conn.Exec(`INSERT INTO t (id, uniq) VALUES (3, 'c')`)
	require.Error(t, notNullErr)
	_, noTableErr := conn.Exec(`SELECT * FROM missing`)
	require.Error(t, noTableErr)

	ctx := t.Context()
	other, err := sqlx.Connect(goSQLDriverName, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = other.Close() })

	lock, err := other.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = lock.Close() })

	_, err = lock.ExecContext(ctx, "BEGIN IMMEDIATE")
	require.NoError(t, err)

	_, busyErr := conn.ExecContext(ctx, `INSERT INTO t (id, uniq, nn) VALUES (4, 'd', 'z')`)
	require.Error(t, busyErr)

	_, err = lock.ExecContext(ctx, "ROLLBACK")
	require.NoError(t, err)

	d := &db{}
	for _, tc := range []struct {
		name string
		err  error
		code int
		dup  bool
	}{
		{
			name: "primary key",
			err:  dupPKErr,
			code: sqlite3.SQLITE_CONSTRAINT_PRIMARYKEY,
			dup:  true,
		},
		{
			name: "unique",
			err:  dupUniqueErr,
			code: sqlite3.SQLITE_CONSTRAINT_UNIQUE,
			dup:  true,
		},
		{
			name: "not null",
			err:  notNullErr,
			code: sqlite3.SQLITE_CONSTRAINT_NOTNULL,
			dup:  false,
		},
		{
			name: "missing table",
			err:  noTableErr,
			code: sqlite3.SQLITE_ERROR,
			dup:  false,
		},
		{
			name: "busy",
			err:  busyErr,
			code: sqlite3.SQLITE_BUSY,
			dup:  false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sqlErr, ok := errors.AsType[*sqlite.Error](tc.err)
			require.True(t, ok)
			require.Equal(t, tc.code, sqlErr.Code())
			require.Equal(t, tc.dup, d.IsDupEntryError(tc.err))
		})
	}
}
