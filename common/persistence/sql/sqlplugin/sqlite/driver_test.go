package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
)

func TestDriverErrors(t *testing.T) {
	conn, err := sql.Open(goSQLDriverName, "file:"+filepath.ToSlash(filepath.Join(t.TempDir(), "errors.db")))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	conn.SetMaxOpenConns(1)

	_, err = conn.Exec("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT UNIQUE)")
	require.NoError(t, err)
	_, err = conn.Exec("INSERT INTO items VALUES (1, 'first')")
	require.NoError(t, err)

	for _, query := range []string{
		"INSERT INTO items VALUES (1, 'second')",
		"INSERT INTO items VALUES (2, 'first')",
	} {
		_, err = conn.Exec(query)
		require.Error(t, err)
		require.True(t, (*db)(nil).IsDupEntryError(fmt.Errorf("insert failed: %w", err)))
		require.False(t, isTableExistsError(err))
	}

	_, err = conn.Exec("CREATE TABLE items (id INTEGER PRIMARY KEY)")
	require.Error(t, err)
	require.True(t, isTableExistsError(fmt.Errorf("create failed: %w", err)))

	_, err = conn.Exec("SELECT * FROM missing_table")
	require.Error(t, err)
	require.False(t, isTableExistsError(err))

	for _, err := range []error{nil, errors.New("unrelated error")} {
		require.False(t, (*db)(nil).IsDupEntryError(err))
		require.False(t, isTableExistsError(err))
	}
}

func TestDriverTimestampCompatibility(t *testing.T) {
	dsn, err := buildDSN(&config.SQL{DatabaseName: filepath.ToSlash(filepath.Join(t.TempDir(), "compatibility.db"))})
	require.NoError(t, err)
	conn, err := sql.Open(goSQLDriverName, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	_, err = conn.Exec("CREATE TABLE items (created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
	require.NoError(t, err)
	want := time.Date(2026, time.October, 9, 12, 34, 56, 123456000, time.UTC)
	for _, timestamp := range []any{want, &want, "2026-10-09 12:34:56.123456+00:00", "2026-10-09 12:34:56.123456"} {
		_, err = conn.Exec("DELETE FROM items")
		require.NoError(t, err)
		_, err = conn.Exec("INSERT INTO items VALUES (?)", timestamp)
		require.NoError(t, err)
		var got time.Time
		require.NoError(t, conn.QueryRow("SELECT created_at FROM items").Scan(&got))
		require.True(t, want.Equal(got), "want %v, got %v", want, got)
		var count int
		require.NoError(t, conn.QueryRow("SELECT count(*) FROM items WHERE created_at >= ? AND created_at < ?", want.Add(-time.Microsecond), want.Add(time.Microsecond)).Scan(&count))
		require.Equal(t, 1, count)
	}
	_, err = conn.Exec("INSERT INTO items DEFAULT VALUES")
	require.NoError(t, err)
	var got time.Time
	require.NoError(t, conn.QueryRow("SELECT created_at FROM items ORDER BY rowid DESC LIMIT 1").Scan(&got))
	require.False(t, got.IsZero())
}

func TestDriverMemorySchema(t *testing.T) {
	for _, cache := range []string{"private", "shared"} {
		t.Run(cache, func(t *testing.T) {
			cfg := &config.SQL{
				DatabaseName:      t.Name(),
				ConnectAttributes: map[string]string{"mode": "memory", "cache": cache},
				MaxConns:          2,
			}
			conn, err := (&plugin{}).createDBConnection(cfg, nil, log.NewNoopLogger())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, conn.Close()) })
			first, err := conn.Conn(context.Background())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, first.Close()) })
			second, err := conn.Conn(context.Background())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, second.Close()) })
			_, err = first.ExecContext(context.Background(), "CREATE TABLE items (id INTEGER PRIMARY KEY)")
			require.NoError(t, err)
			_, err = first.ExecContext(context.Background(), "INSERT INTO items VALUES (1)")
			require.NoError(t, err)
			var got int
			require.NoError(t, second.QueryRowContext(context.Background(), "SELECT id FROM items").Scan(&got))
			require.Equal(t, 1, got)
		})
	}
}

func TestDriverTimestampPrecision(t *testing.T) {
	dsn, err := buildDSN(&config.SQL{
		DatabaseName: filepath.ToSlash(filepath.Join(t.TempDir(), "timestamps.db")),
		ConnectAttributes: map[string]string{
			"busy_timeout": "1000",
		},
	})
	require.NoError(t, err)
	conn, err := sql.Open(goSQLDriverName, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	conn.SetMaxOpenConns(1)

	_, err = conn.Exec("CREATE TABLE items (created_at TIMESTAMP)")
	require.NoError(t, err)
	want := time.Date(2026, time.October, 9, 12, 34, 56, 123456000, time.UTC)
	_, err = conn.Exec("INSERT INTO items VALUES (?)", want)
	require.NoError(t, err)
	var got time.Time
	require.NoError(t, conn.QueryRow("SELECT created_at FROM items").Scan(&got))
	require.True(t, want.Equal(got), "timestamp lost precision: want %v, got %v", want, got)
}
