package sql

import (
	"errors"
	flagpkg "flag"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/tools/common/schema"
)

type versionMetadataDB struct {
	sqlplugin.AdminDB
	calls []string
	err   error
}

func (db *versionMetadataDB) CreateSchemaVersionTables() error {
	db.calls = append(db.calls, "create core")
	return db.err
}
func (db *versionMetadataDB) ReadSchemaVersion(string) (string, error) {
	db.calls = append(db.calls, "read core")
	return "1.0", db.err
}
func (db *versionMetadataDB) UpdateSchemaVersion(string, string, string) error {
	db.calls = append(db.calls, "update core")
	return db.err
}
func (db *versionMetadataDB) WriteSchemaUpdateLog(string, string, string, string) error {
	db.calls = append(db.calls, "history core")
	return db.err
}
func (db *versionMetadataDB) CreateVisibilityVersionTables() error {
	db.calls = append(db.calls, "create visibility")
	return db.err
}
func (db *versionMetadataDB) ReadVisibilityVersion(string) (string, error) {
	db.calls = append(db.calls, "read visibility")
	return "2.0", db.err
}
func (db *versionMetadataDB) UpdateVisibilityVersion(string, string, string) error {
	db.calls = append(db.calls, "update visibility")
	return db.err
}
func (db *versionMetadataDB) WriteVisibilityUpdateLog(string, string, string, string) error {
	db.calls = append(db.calls, "history visibility")
	return db.err
}

func TestVersionMetadataSelection(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		visibility bool
		version    string
	}{
		{name: "core", version: "1.0"},
		{name: "visibility", visibility: true, version: "2.0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := &versionMetadataDB{}
			connection := &Connection{dbName: "combined", adminDB: db}
			if tc.visibility {
				connection.visibilityDB = db
			}
			require.NoError(t, connection.CreateSchemaVersionTables())
			version, err := connection.ReadSchemaVersion()
			require.NoError(t, err)
			require.Equal(t, tc.version, version)
			require.NoError(t, connection.UpdateSchemaVersion("3.0", "1.0"))
			require.NoError(t, connection.WriteSchemaUpdateLog(tc.version, "3.0", "checksum", "upgrade"))
			require.Equal(t, []string{"create " + tc.name, "read " + tc.name, "update " + tc.name, "history " + tc.name}, db.calls)

			db.err = errors.New("database unavailable")
			require.ErrorIs(t, connection.CreateSchemaVersionTables(), db.err)
			_, err = connection.ReadSchemaVersion()
			require.ErrorIs(t, err, db.err)
			require.ErrorIs(t, connection.UpdateSchemaVersion("3.0", "1.0"), db.err)
			require.ErrorIs(t, connection.WriteSchemaUpdateLog(tc.version, "3.0", "checksum", "upgrade"), db.err)
		})
	}
}

func TestVisibilityOverwriteRejected(t *testing.T) {
	t.Parallel()
	flags := flagpkg.NewFlagSet("setup", flagpkg.ContinueOnError)
	flags.Bool("visibility", false, "")
	flags.Bool(schema.CLIOptOverwrite, false, "")
	require.NoError(t, flags.Parse([]string{"--visibility", "--" + schema.CLIOptOverwrite}))
	err := setupSchema(cli.NewContext(cli.NewApp(), flags, nil), log.NewNoopLogger())
	require.ErrorContains(t, err, "overwrite drops all database tables")
}
