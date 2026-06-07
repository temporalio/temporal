package sqlite

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

func TestRewriteTestSchemaStatements_EnforcesVarcharLengthChecks(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name                 string
		createTableStatement string
	}{
		{
			name: "create table",
			createTableStatement: `CREATE TABLE test (
				id BIGINT NOT NULL,
				value VARCHAR(3),
				PRIMARY KEY (id)
			);`,
		},
		{
			name: "mixed case without semicolon",
			createTableStatement: `create table test (
				id BIGINT NOT NULL,
				value varchar(3),
				PRIMARY KEY (id)
			)`,
		},
		{
			name: "temporary table",
			createTableStatement: `CREATE TEMPORARY TABLE test (
				id BIGINT NOT NULL,
				value VARCHAR(3),
				PRIMARY KEY (id)
			);`,
		},
		{
			name: "if not exists",
			createTableStatement: `CREATE TABLE IF NOT EXISTS test (
				id BIGINT NOT NULL,
				value VARCHAR(3),
				PRIMARY KEY (id)
			);`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			schemaFile := filepath.Join(t.TempDir(), "schema.sql")
			err := os.WriteFile(schemaFile, []byte(tc.createTableStatement), 0o600)
			require.NoError(t, err)

			statements, err := p.LoadAndSplitQuery([]string{schemaFile})
			require.NoError(t, err)

			sqlDB, err := sqlx.Open(goSQLDriverName, ":memory:")
			require.NoError(t, err)
			t.Cleanup(func() { _ = sqlDB.Close() })

			db := newDB(sqlplugin.DbKindUnknown, "test", sqlDB, nil, log.NewNoopLogger())
			statements = db.RewriteTestSchemaStatements(statements)
			for _, stmt := range statements {
				err = db.Exec(stmt)
				require.NoError(t, err)
			}

			err = db.Exec(`INSERT INTO test (id, value) VALUES (1, NULL)`)
			require.NoError(t, err)

			err = db.Exec(`INSERT INTO test (id, value) VALUES (2, 'abc')`)
			require.NoError(t, err)

			err = db.Exec(`INSERT INTO test (id, value) VALUES (3, 'abcd')`)
			require.Error(t, err)
			require.Contains(t, err.Error(), "CHECK constraint failed")
		})
	}
}
