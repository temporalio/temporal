package schema

import (
	"fmt"
	"regexp"
)

type (
	// ConfigError is an error type that
	// represents a problem with the config
	ConfigError struct {
		msg string
	}
	// UpdateConfig holds the config
	// params for executing a UpdateTask
	UpdateConfig struct {
		DBName        string
		TargetVersion string
		SchemaDir     string
		SchemaName    string
		IsDryRun      bool
	}
	// SetupConfig holds the config
	// params need by the SetupTask
	SetupConfig struct {
		SchemaFilePath    string
		SchemaName        string
		InitialVersion    string
		Overwrite         bool // overwrite previous data
		DisableVersioning bool // do not use schema versioning
	}

	// DB is the database interface that's required to be implemented
	// for the schema-tool to work
	DB interface {
		// Exec executes a cql statement
		Exec(stmt string, args ...interface{}) error
		// DropAllTables drops all tables
		DropAllTables() error
		// CreateSchemaVersionTables sets up the schema version tables
		CreateSchemaVersionTables() error
		// ReadSchemaVersion returns the current schema version for the keyspace
		ReadSchemaVersion() (string, error)
		// UpdateSchemaVersion updates the schema version for the keyspace
		UpdateSchemaVersion(newVersion string, minCompatibleVersion string) error
		// WriteSchemaUpdateLog adds an entry to the schema update history table
		WriteSchemaUpdateLog(oldVersion string, newVersion string, manifestMD5 string, desc string) error
		// Close gracefully closes the client object
		Close()
		// Type gives the type of db (e.g. "cassandra", "sql")
		Type() string
	}
)

const (
	// CLIFlagEndpoint is the cli option for endpoint
	CLIFlagEndpoint = "endpoint"
	// CLIFlagEndpointAlias is the cli flag alias for endpoint
	CLIFlagEndpointAlias = "ep"
	// CLIFlagPort is the cli option for port
	CLIFlagPort = "port"
	// CLIFlagPortAlias is the cli flag alias for port
	CLIFlagPortAlias = "p"
	// CLIFlagUser is the cli option for user
	CLIFlagUser = "user"
	// CLIFlagUserAlias is the cli flag alias for user
	CLIFlagUserAlias = "u"
	// CLIFlagPassword is the cli option for password
	CLIFlagPassword = "password"
	// CLIFlagPasswordAlias is the cli flag alias for password
	CLIFlagPasswordAlias = "pw"
	// CLIOptAuthenticator is the cli option for allowed authenticator settings
	CLIFlagAllowedAuthenticators = "allowed-authenticators"
	// CLIFlagAllowedAuthenticatorsAlias is the cli flag alias for allowed authenticators
	CLIFlagAllowedAuthenticatorsAlias = "aa"
	// CLIFlagTimeout is the cli option for timeout
	CLIFlagTimeout = "timeout"
	// CLIFlagTimeoutAlias is the cli flag alias for timeout
	CLIFlagTimeoutAlias = "t"
	// CLIFlagKeyspace is the cli option for keyspace
	CLIFlagKeyspace = "keyspace"
	// CLIFlagKeyspaceAlias is the cli flag alias for keyspace
	CLIFlagKeyspaceAlias = "k"
	// CLIFlagDatabase is the cli option for database
	CLIFlagDatabase = "database"
	// CLIFlagDatabaseAlias is the cli flag alias for database
	CLIFlagDatabaseAlias = "db"
	// CLIFlagDefaultDb is the cli option used as defaultdb to connect to
	CLIFlagDefaultDb = "defaultdb"
	// CLIFlagPluginName is the cli option for plugin name
	CLIFlagPluginName = "plugin"
	// CLIFlagPluginNameAlias is the cli flag alias for plugin name
	CLIFlagPluginNameAlias = "pl"
	// CLIFlagConnectAttributes is the cli option for connect attributes (key/values via a url query string)
	CLIFlagConnectAttributes = "connect-attributes"
	// CLIFlagConnectAttributesAlias is the cli flag alias for connect attributes
	CLIFlagConnectAttributesAlias = "ca"
	// CLIFlagVersion is the cli option for version
	CLIFlagVersion = "version"
	// CLIFlagVersionAlias is the cli flag alias for version
	CLIFlagVersionAlias = "v"
	// CLIFlagSchemaFile is the cli option for schema file
	CLIFlagSchemaFile = "schema-file"
	// CLIFlagSchemaFileAlias is the cli flag alias for schema file
	CLIFlagSchemaFileAlias = "f"
	// CLIFlagOverwrite is the cli option for overwrite
	CLIFlagOverwrite = "overwrite"
	// CLIFlagOverwriteAlias is the cli flag alias for overwrite
	CLIFlagOverwriteAlias = "o"
	// CLIFlagDisableVersioning is the cli option to disabling versioning
	CLIFlagDisableVersioning = "disable-versioning"
	// CLIFlagDisableVersioningAlias is the cli flag alias to disabling versioning
	CLIFlagDisableVersioningAlias = "d"
	// CLIFlagTargetVersion is the cli option for target version
	CLIFlagTargetVersion = "version"
	// CLIFlagTargetVersionAlias is the cli flag alias for target version
	CLIFlagTargetVersionAlias = "v"
	// CLIFlagSchemaDir is the cli option for schema directory
	CLIFlagSchemaDir = "schema-dir"
	// CLIFlagSchemaDirAlias is the cli flag alias for schema directory
	CLIFlagSchemaDirAlias = "d"
	// CLIFlagSchemaName is the cli option for which pre-embedded schema to use
	CLIFlagSchemaName = "schema-name"
	// CLIFlagSchemaNameAlias is the cli flag alias for which pre-embedded schema to use
	CLIFlagSchemaNameAlias = "s"
	// CLIFlagReplicationFactor is the cli option for replication factor
	CLIFlagReplicationFactor = "replication-factor"
	// CLIFlagReplicationFactorAlias is the cli flag alias for replication factor
	CLIFlagReplicationFactorAlias = "rf"
	// CLIFlagDatacenter is the cli option for NetworkTopologyStrategy datacenter
	CLIFlagDatacenter = "datacenter"
	// CLIFlagDatacenterAlias is the cli flag alias for NetworkTopologyStrategy datacenter
	CLIFlagDatacenterAlias = "dc"
	// CLIOptConsistency is the cli option for consistency settings
	CLIOptConsistency = "consistency"
	// CLIOptAddressTranslator is the cli option for address translator for Cassandra
	CLIOptAddressTranslator = "address-translator"
	// CLIFlagAddressTranslatorAlias is the cli flag alias for address translator
	CLIFlagAddressTranslatorAlias = "at"
	// CLIOptAddressTranslatorOptions is the cli option for options for address translator
	CLIOptAddressTranslatorOptions = "address-translator-options"
	// CLIFlagQuiet is the cli option for quiet mode
	CLIFlagQuiet = "quiet"
	// CLIFlagQuietAlias is the cli flag alias for quiet mode
	CLIFlagQuietAlias = "q"
	// CLIFlagForce is the cli flag for force mode
	CLIFlagForce = "force"
	// CLIFlagForceAlias is the cli flag alias for force mode
	CLIFlagForceAlias = "f"

	// CLIFlagAddressTranslatorOptions is the cli option for address translator of Cassandra
	CLIFlagAddressTranslatorOptions
	// CLIFlagDisableInitialHostLookup is the cli flag for only using supplied hosts to connect to the database
	CLIFlagDisableInitialHostLookup = "disable-initial-host-lookup"

	// CLIFlagEnableTLS enables cassandra client TLS
	CLIFlagEnableTLS = "tls"
	// CLIFlagTLSCertFile is the optional tls cert file (tls must be enabled)
	CLIFlagTLSCertFile = "tls-cert-file"
	// CLIFlagTLSKeyFile is the optional tls key file (tls must be enabled)
	CLIFlagTLSKeyFile = "tls-key-file"
	// CLIFlagTLSCaFile is the optional tls CA file (tls must be enabled)
	CLIFlagTLSCaFile = "tls-ca-file"
	// CLIFlagTLSDisableHostVerification disable tls host verification (tls must be enabled)
	CLIFlagTLSDisableHostVerification = "tls-disable-host-verification"
	// CLIFlagTLSHostName specifies the host name for host name verification
	CLIFlagTLSHostName = "tls-server-name"
)

var rmspaceRegex = regexp.MustCompile(`\s+`)

// NewConfigError creates and returns an instance of ConfigError
func NewConfigError(msg string) error {
	return &ConfigError{msg: msg}
}

// Error returns a string representation of this error
func (e *ConfigError) Error() string {
	return fmt.Sprintf("Config Error:%v", e.msg)
}
