package schema

import (
	"fmt"

	"github.com/blang/semver/v4"
)

// VerifyCompatibleVersion ensures that the installed version is greater than or equal to the expected version.
func VerifyCompatibleVersion(
	versionReader VersionReader,
	dbName string,
	expectedVersion string,
) error {

	version, err := versionReader.ReadSchemaVersion(dbName)
	if err != nil {
		return fmt.Errorf("unable to read DB schema version keyspace/database: %s error: %v", dbName, err.Error())
	}
	// In most cases, the versions should match. However if after a schema upgrade there is a code
	// rollback, the code version (expected version) would fall lower than the actual version in
	// Cassandra. This check to allow such rollbacks since we only make backwards compatible schema changes.
	versionParsed, _ := semver.ParseTolerant(version)
	expectedVersionParsed, _ := semver.ParseTolerant(expectedVersion)

	if versionParsed.LT(expectedVersionParsed) {
		return fmt.Errorf("version mismatch for keyspace/database: %q. Expected version: %s cannot be greater than Actual version: %s", dbName, expectedVersion, version)
	}
	return nil
}
