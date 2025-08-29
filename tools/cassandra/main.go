package cassandra

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	dbschemas "go.temporal.io/server/schema"
	"go.temporal.io/server/temporal/environment"
	"go.temporal.io/server/tools/common/schema"
)

// RunTool runs the temporal-cassandra-tool command line tool
func RunTool(args []string) error {
	app := buildCLIOptions()
	return app.Run(args)
}

var osExit = os.Exit

// root handler for all cli commands
func cliHandler(c *cli.Context, handler func(c *cli.Context, logger log.Logger) error, logger log.Logger) {
	quiet := c.Bool(schema.CLIFlagQuiet)
	err := handler(c, logger)
	if err != nil && !quiet {
		osExit(1)
	}
}

func buildCLIOptions() *cli.App {
	app := cli.NewApp()
	app.Name = "temporal-cassandra-tool"
	app.Usage = "Command line tool for temporal cassandra operations"
	app.Version = headers.ServerVersion
	logger := log.NewCLILogger()

	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:    schema.CLIFlagEndpoint,
			Aliases: []string{schema.CLIFlagEndpointAlias},
			Value:   environment.GetCassandraAddress(),
			Usage:   "hostname or ip address of cassandra host to connect to",
			EnvVars: []string{"CASSANDRA_HOST"},
		},
		&cli.IntFlag{
			Name:    schema.CLIFlagPort,
			Aliases: []string{schema.CLIFlagPortAlias},
			Value:   environment.GetCassandraPort(),
			Usage:   "Port of cassandra host to connect to",
			EnvVars: []string{"CASSANDRA_PORT"},
		},
		&cli.StringFlag{
			Name:    schema.CLIFlagUser,
			Aliases: []string{schema.CLIFlagUserAlias},
			Value:   "",
			Usage:   "User name used for authentication for connecting to cassandra host",
			EnvVars: []string{"CASSANDRA_USER"},
		},
		&cli.StringFlag{
			Name:    schema.CLIFlagPassword,
			Aliases: []string{schema.CLIFlagPasswordAlias},
			Value:   "",
			Usage:   "Password used for authentication for connecting to cassandra host",
			EnvVars: []string{"CASSANDRA_PASSWORD"},
		},
		&cli.StringSliceFlag{
			Name:    schema.CLIFlagAllowedAuthenticators,
			Aliases: []string{schema.CLIFlagAllowedAuthenticatorsAlias},
			Value:   nil,
			Usage:   "List of authenticators allowed to be used by the gocql client while connecting to the server.",
			EnvVars: []string{"CASSANDRA_ALLOWED_AUTHENTICATORS"},
		},
		&cli.IntFlag{
			Name:    schema.CLIFlagTimeout,
			Aliases: []string{schema.CLIFlagTimeoutAlias},
			Value:   defaultTimeout,
			Usage:   "request Timeout in seconds used for cql client",
			EnvVars: []string{"CASSANDRA_TIMEOUT"},
		},
		&cli.StringFlag{
			Name:    schema.CLIFlagKeyspace,
			Aliases: []string{schema.CLIFlagKeyspaceAlias},
			Value:   "temporal",
			Usage:   "name of the cassandra Keyspace",
			EnvVars: []string{"CASSANDRA_KEYSPACE"},
		},
		&cli.StringFlag{
			Name:    schema.CLIFlagDatacenter,
			Aliases: []string{schema.CLIFlagDatacenterAlias},
			Value:   "",
			Usage:   "enable NetworkTopologyStrategy by providing datacenter name",
			EnvVars: []string{"CASSANDRA_DATACENTER"},
		},
		&cli.StringFlag{
			Name:    schema.CLIOptAddressTranslator,
			Value:   "",
			Usage:   "name of address translator for cassandra hosts",
			EnvVars: []string{"CASSANDRA_ADDRESS_TRANSLATOR"},
		},
		&cli.StringFlag{
			Name:    schema.CLIOptAddressTranslatorOptions,
			Value:   "",
			Usage:   "colon-separated list of key=value pairs as options for address translator",
			EnvVars: []string{"CASSANDRA_ADDRESS_TRANSLATOR_OPTIONS_CLI"},
		},
		&cli.BoolFlag{
			Name:    schema.CLIFlagQuiet,
			Aliases: []string{schema.CLIFlagQuietAlias},
			Usage:   "Don't set exit status to 1 on error",
		},
		&cli.BoolFlag{
			Name:    schema.CLIFlagDisableInitialHostLookup,
			Usage:   "instructs gocql driver to only connect to the supplied hosts vs. attempting to lookup additional hosts via the system.peers table",
			EnvVars: []string{"CASSANDRA_DISABLE_INITIAL_HOST_LOOKUP"},
		},
		&cli.BoolFlag{
			Name:    schema.CLIFlagEnableTLS,
			Usage:   "enable TLS",
			EnvVars: []string{"CASSANDRA_ENABLE_TLS"},
		},
		&cli.StringFlag{
			Name:    schema.CLIFlagTLSCertFile,
			Usage:   "TLS cert file",
			EnvVars: []string{"CASSANDRA_TLS_CERT"},
		},
		&cli.StringFlag{
			Name:    schema.CLIFlagTLSKeyFile,
			Usage:   "TLS key file",
			EnvVars: []string{"CASSANDRA_TLS_KEY"},
		},
		&cli.StringFlag{
			Name:    schema.CLIFlagTLSCaFile,
			Usage:   "TLS CA file",
			EnvVars: []string{"CASSANDRA_TLS_CA"},
		},
		&cli.StringFlag{
			Name:    schema.CLIFlagTLSHostName,
			Value:   "",
			Usage:   "override for target server name",
			EnvVars: []string{"CASSANDRA_TLS_SERVER_NAME"},
		},
		&cli.BoolFlag{
			Name:    schema.CLIFlagTLSDisableHostVerification,
			Usage:   "disable tls host name verification (tls must be enabled)",
			EnvVars: []string{"CASSANDRA_TLS_DISABLE_HOST_VERIFICATION"},
		},
	}

	app.Commands = []*cli.Command{
		{
			Name:    "setup-schema",
			Aliases: []string{"setup"},
			Usage:   "setup initial version of cassandra schema",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:    schema.CLIFlagVersion,
					Aliases: []string{schema.CLIFlagVersionAlias},
					Usage:   "initial version of the schema, cannot be used with disable-versioning",
				},
				&cli.StringFlag{
					Name:    schema.CLIFlagSchemaFile,
					Aliases: []string{schema.CLIFlagSchemaFileAlias},
					Usage:   "path to the .cql schema file; if un-specified, will just setup versioning tables",
				},
				&cli.StringFlag{
					Name:    schema.CLIFlagSchemaName,
					Aliases: []string{schema.CLIFlagSchemaNameAlias},
					Usage: fmt.Sprintf("name of embedded schema directory with .cql file, one of: %v",
						dbschemas.PathsByDB("cassandra")),
				},
				&cli.BoolFlag{
					Name:    schema.CLIFlagDisableVersioning,
					Aliases: []string{schema.CLIFlagDisableVersioningAlias},
					Usage:   "disable setup of schema versioning",
				},
				&cli.BoolFlag{
					Name:    schema.CLIFlagOverwrite,
					Aliases: []string{schema.CLIFlagOverwriteAlias},
					Usage:   "drop all existing tables before setting up new schema",
				},
			},
			Action: func(c *cli.Context) error {
				cliHandler(c, setupSchema, logger)
				return nil
			},
		},
		{
			Name:    "update-schema",
			Aliases: []string{"update"},
			Usage:   "update cassandra schema to a specific version",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:    schema.CLIFlagTargetVersion,
					Aliases: []string{schema.CLIFlagTargetVersionAlias},
					Usage:   "target version for the schema update, defaults to latest",
				},
				&cli.StringFlag{
					Name:    schema.CLIFlagSchemaDir,
					Aliases: []string{schema.CLIFlagSchemaDirAlias},
					Usage:   "path to directory containing versioned schema",
				},
				&cli.StringFlag{
					Name:    schema.CLIFlagSchemaName,
					Aliases: []string{schema.CLIFlagSchemaNameAlias},
					Usage: fmt.Sprintf("name of embedded versioned schema, one of: %v",
						dbschemas.PathsByDB("cassandra")),
				},
			},
			Action: func(c *cli.Context) error {
				cliHandler(c, updateSchema, logger)
				return nil
			},
		},
		{
			Name:    "create-keyspace",
			Aliases: []string{"create", "create-Keyspace"},
			Usage:   "creates a keyspace with simple strategy or network topology if datacenter name is provided",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:    schema.CLIFlagKeyspace,
					Aliases: []string{schema.CLIFlagKeyspaceAlias},
					Usage:   "name of the keyspace",
				},
				&cli.IntFlag{
					Name:    schema.CLIFlagReplicationFactor,
					Aliases: []string{schema.CLIFlagReplicationFactorAlias},
					Value:   1,
					Usage:   "replication factor for the keyspace",
				},
				&cli.StringFlag{
					Name:    schema.CLIFlagDatacenter,
					Aliases: []string{schema.CLIFlagDatacenterAlias},
					Value:   "",
					Usage:   "enable NetworkTopologyStrategy by providing datacenter name",
				},
			},
			Action: func(c *cli.Context) error {
				cliHandler(c, createKeyspace, logger)
				return nil
			},
		},
		{
			Name:    "drop-keyspace",
			Aliases: []string{"drop"},
			Usage:   "drops a keyspace with simple strategy or network topology if datacenter name is provided",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:    schema.CLIFlagKeyspace,
					Aliases: []string{schema.CLIFlagKeyspaceAlias},
					Usage:   "name of the keyspace",
				},
				&cli.IntFlag{
					Name:    schema.CLIFlagReplicationFactor,
					Aliases: []string{schema.CLIFlagReplicationFactorAlias},
					Value:   1,
					Usage:   "replication factor for the keyspace",
				},
				&cli.StringFlag{
					Name:    schema.CLIFlagDatacenter,
					Aliases: []string{schema.CLIFlagDatacenterAlias},
					Value:   "",
					Usage:   "enable NetworkTopologyStrategy by providing datacenter name",
				},
				&cli.BoolFlag{
					Name:    schema.CLIFlagForce,
					Aliases: []string{schema.CLIFlagForceAlias},
					Usage:   "don't prompt for confirmation",
				},
			},
			Action: func(c *cli.Context) error {
				drop := c.Bool(schema.CLIFlagForce)
				if !drop {
					keyspace := c.String(schema.CLIFlagKeyspace)
					fmt.Printf("Are you sure you want to drop keyspace %q (y/N)? ", keyspace)
					y := ""
					_, _ = fmt.Scanln(&y)
					if y == "y" || y == "Y" {
						drop = true
					}
				}
				if drop {
					cliHandler(c, dropKeyspace, logger)
				}
				return nil
			},
		},
		{
			Name:    "validate-health",
			Aliases: []string{"vh"},
			Usage:   "validates health of cassandra by attempting to establish CQL session to system keyspace",
			Action: func(c *cli.Context) error {
				cliHandler(c, validateHealth, logger)
				return nil
			},
		},
	}

	return app
}
