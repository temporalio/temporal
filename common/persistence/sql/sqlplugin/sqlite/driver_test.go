package sqlite

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

func TestIsDupEntryError(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dsn := "file:" + filepath.Join(t.TempDir(), "t.db")
	conn, err := sqlx.Connect(goSQLDriverName, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	conn.SetMaxOpenConns(1)
	_, err = conn.Exec(`PRAGMA foreign_keys = ON`)
	require.NoError(t, err)
	_, err = conn.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, u TEXT UNIQUE, nn TEXT NOT NULL CHECK (nn <> ''), parent_id INTEGER REFERENCES t(id))`)
	require.NoError(t, err)
	_, err = conn.Exec(`INSERT INTO t (id, u, nn) VALUES (1, 'a', 'x')`)
	require.NoError(t, err)

	_, dupPK := conn.Exec(`INSERT INTO t (id, u, nn) VALUES (1, 'b', 'x')`)
	require.Error(t, dupPK)
	_, dupUnique := conn.Exec(`INSERT INTO t (id, u, nn) VALUES (2, 'a', 'x')`)
	require.Error(t, dupUnique)
	_, notNull := conn.Exec(`INSERT INTO t (id, u) VALUES (3, 'c')`)
	require.Error(t, notNull)
	_, check := conn.Exec(`INSERT INTO t (id, u, nn) VALUES (3, 'c', '')`)
	require.Error(t, check)
	_, foreignKey := conn.Exec(`INSERT INTO t (id, u, nn, parent_id) VALUES (3, 'c', 'x', 99)`)
	require.Error(t, foreignKey)
	_, noTable := conn.Exec(`SELECT * FROM missing`)
	require.Error(t, noTable)

	other, err := sqlx.Connect(goSQLDriverName, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, other.Close()) })
	lock, err := other.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, lock.Close()) })
	_, err = lock.ExecContext(ctx, "BEGIN IMMEDIATE")
	require.NoError(t, err)
	_, busy := conn.Exec(`INSERT INTO t (id, u, nn) VALUES (4, 'd', 'x')`)
	require.Error(t, busy)
	_, err = lock.ExecContext(ctx, "ROLLBACK")
	require.NoError(t, err)

	d := &db{}
	cases := []struct {
		name string
		err  error
		want bool
		code int
	}{
		{name: "duplicate primary key", err: dupPK, want: true, code: sqlite3.SQLITE_CONSTRAINT_PRIMARYKEY},
		{name: "duplicate unique value", err: dupUnique, want: true, code: sqlite3.SQLITE_CONSTRAINT_UNIQUE},
		{name: "wrapped duplicate", err: fmt.Errorf("insert failed: %w", dupPK), want: true, code: sqlite3.SQLITE_CONSTRAINT_PRIMARYKEY},
		{name: "not null violation", err: notNull, code: sqlite3.SQLITE_CONSTRAINT_NOTNULL},
		{name: "check violation", err: check, code: sqlite3.SQLITE_CONSTRAINT_CHECK},
		{name: "foreign key violation", err: foreignKey, code: sqlite3.SQLITE_CONSTRAINT_FOREIGNKEY},
		{name: "no such table", err: noTable, code: sqlite3.SQLITE_ERROR},
		{name: "database is locked", err: busy, code: sqlite3.SQLITE_BUSY},
		{name: "wrapped database is locked", err: fmt.Errorf("insert failed: %w", busy), code: sqlite3.SQLITE_BUSY},
		{name: "nil error", err: nil, want: false},
		{name: "non-sqlite error", err: errors.New("boom"), want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.code != 0 {
				sqlErr, ok := errors.AsType[*sqlite.Error](tc.err)
				require.True(t, ok)
				require.Equal(t, tc.code, sqlErr.Code())
			}
			require.Equal(t, tc.want, d.IsDupEntryError(tc.err), "err: %v", tc.err)
		})
	}
}
