package libsql

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
)

func TestIsDupEntryError(t *testing.T) {
	d := &db{}

	require.False(t, d.IsDupEntryError(nil))
	require.False(t, d.IsDupEntryError(errors.New("something else")))

	// error code path
	require.True(t, d.IsDupEntryError(
		errors.New("failed to execute query INSERT INTO ...\nerror code = 2067: UNIQUE constraint failed: executions.shard_id"),
	))
	require.True(t, d.IsDupEntryError(
		errors.New("failed to execute query INSERT INTO ...\nerror code = 1555: PRIMARY KEY constraint failed"),
	))

	// string fallback
	require.True(t, d.IsDupEntryError(errors.New("UNIQUE constraint failed: t.c")))
	require.True(t, d.IsDupEntryError(errors.New("primary key constraint failed")))

	// base constraint code should NOT match (we only want the specific extended codes)
	require.False(t, d.IsDupEntryError(errors.New("error code = 19: SQLITE_CONSTRAINT")))
}

func TestIsTableExistsError(t *testing.T) {
	require.False(t, isTableExistsError(nil))
	require.False(t, isTableExistsError(errors.New("something else")))
	require.True(t, isTableExistsError(errors.New("table executions_visibility already exists")))
	require.True(t, isTableExistsError(errors.New("TABLE schema_version ALREADY EXISTS")))
	require.False(t, isTableExistsError(errors.New("table not found")))
}

func TestBuildDSN(t *testing.T) {
	require.Equal(t, ":memory:", buildDSN(&config.SQL{DatabaseName: ":memory:"}))
	require.Equal(t, ":memory:", buildDSN(&config.SQL{
		DatabaseName:      "ignored",
		ConnectAttributes: map[string]string{"mode": "memory"},
	}))
	require.Equal(t, "file:/var/temporal/default.db", buildDSN(&config.SQL{DatabaseName: "/var/temporal/default.db"}))
	require.Equal(t, "file:/var/temporal/default.db", buildDSN(&config.SQL{DatabaseName: "file:/var/temporal/default.db"}))
	require.Equal(t, "file:/tmp/test.db", buildDSN(&config.SQL{DatabaseName: "/tmp/test.db"}))
}

func TestNormalizeDQS(t *testing.T) {
	// DDL with double-quoted JSON paths should be rewritten
	input := `CREATE TABLE t (x TEXT GENERATED ALWAYS AS (JSON_EXTRACT(data, "$.foo")))`
	expected := `CREATE TABLE t (x TEXT GENERATED ALWAYS AS (JSON_EXTRACT(data, '$.foo')))`
	require.Equal(t, expected, normalizeDQS(input))

	// Single-quoted strings pass through unchanged
	input = `CREATE TABLE t (x TEXT GENERATED ALWAYS AS (JSON_EXTRACT(data, '$.foo')))`
	require.Equal(t, input, normalizeDQS(input))

	// Non-DDL statements are not touched
	input = `INSERT INTO t (name) VALUES ("hello")`
	require.Equal(t, input, normalizeDQS(input))

	// Escaped double quotes inside double-quoted string
	input = `CREATE TABLE t (x TEXT DEFAULT "say ""hi""")`
	expected = `CREATE TABLE t (x TEXT DEFAULT 'say ''hi''')`
	require.Equal(t, expected, normalizeDQS(input))

	// Single quotes embedded inside a double-quoted string get escaped
	input = `CREATE TABLE t (x TEXT DEFAULT "it's")`
	expected = `CREATE TABLE t (x TEXT DEFAULT 'it''s')`
	require.Equal(t, expected, normalizeDQS(input))

	// Mixed: single-quoted string followed by double-quoted string
	input = `CREATE TABLE t (a TEXT DEFAULT 'keep', b TEXT DEFAULT "rewrite")`
	expected = `CREATE TABLE t (a TEXT DEFAULT 'keep', b TEXT DEFAULT 'rewrite')`
	require.Equal(t, expected, normalizeDQS(input))

	// ALTER TABLE is also handled
	input = `ALTER TABLE t ADD COLUMN x TEXT GENERATED ALWAYS AS (JSON_EXTRACT(data, "$.bar"))`
	expected = `ALTER TABLE t ADD COLUMN x TEXT GENERATED ALWAYS AS (JSON_EXTRACT(data, '$.bar'))`
	require.Equal(t, expected, normalizeDQS(input))

	// STRFTIME with mixed quoting from Temporal schema
	input = `CREATE TABLE t (ts TIMESTAMP GENERATED ALWAYS AS (STRFTIME('%Y-%m-%d', JSON_EXTRACT(data, "$.dt"))))`
	expected = `CREATE TABLE t (ts TIMESTAMP GENERATED ALWAYS AS (STRFTIME('%Y-%m-%d', JSON_EXTRACT(data, '$.dt'))))`
	require.Equal(t, expected, normalizeDQS(input))
}
