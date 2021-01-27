package schema

type (
	VersionReader interface {
		// ReadSchemaVersion returns the current schema version for the keyspace
		ReadSchemaVersion() (string, error)
	}
)
