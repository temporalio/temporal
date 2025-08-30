package schema

import (
	"fmt"
	"slices"
	"strings"

	"github.com/urfave/cli/v2"
	"go.temporal.io/server/common/log"
	dbschemas "go.temporal.io/server/schema"
)

// Setup sets up schema tables
func Setup(cli *cli.Context, db DB, logger log.Logger) error {
	cfg, err := newSetupConfig(cli, db)
	if err != nil {
		return err
	}
	return NewSetupSchemaTask(db, cfg, logger).Run()
}

// Update updates the schema for the specified database
func Update(cli *cli.Context, db DB, logger log.Logger) error {
	cfg, err := newUpdateConfig(cli, db)
	if err != nil {
		return err
	}
	return NewUpdateSchemaTask(db, cfg, logger).Run()
}

func newUpdateConfig(cli *cli.Context, db DB) (*UpdateConfig, error) {
	config := new(UpdateConfig)
	config.SchemaDir = cli.String(CLIFlagSchemaDir)
	config.SchemaName = cli.String(CLIFlagSchemaName)
	config.TargetVersion = cli.String(CLIFlagTargetVersion)

	if err := validateUpdateConfig(config, db); err != nil {
		return nil, err
	}
	return config, nil
}

func newSetupConfig(cli *cli.Context, db DB) (*SetupConfig, error) {
	config := new(SetupConfig)
	config.SchemaFilePath = cli.String(CLIFlagSchemaFile)
	config.SchemaName = cli.String(CLIFlagSchemaName)
	config.InitialVersion = cli.String(CLIFlagVersion)
	config.DisableVersioning = cli.Bool(CLIFlagDisableVersioning)
	config.Overwrite = cli.Bool(CLIFlagOverwrite)

	if err := validateSetupConfig(config, db); err != nil {
		return nil, err
	}
	return config, nil
}

func validateSetupConfig(config *SetupConfig, db DB) error {
	if len(config.SchemaFilePath) == 0 && len(config.SchemaName) == 0 && config.DisableVersioning {
		return NewConfigError("needs either " + flag(CLIFlagSchemaFile) + " or " + flag(CLIFlagSchemaName))
	}
	if (config.DisableVersioning && len(config.InitialVersion) > 0) ||
		(!config.DisableVersioning && len(config.InitialVersion) == 0) {
		return NewConfigError("missing argument; either " + flag(CLIFlagDisableVersioning) + " or " +
			flag(CLIFlagVersion) + " but not both must be specified")
	}
	if len(config.SchemaFilePath) > 0 && len(config.SchemaName) > 0 {
		return NewConfigError("either" + flag(CLIFlagSchemaFile) + " or " +
			flag(CLIFlagSchemaName) + " must be specified")
	}
	if len(config.SchemaName) > 0 {
		if !slices.Contains(dbschemas.PathsByDB(db.Type()), config.SchemaName) {
			return NewConfigError(fmt.Sprintf("%s must be one of: %v",
				flag(CLIFlagSchemaName), dbschemas.PathsByDB(db.Type())))
		}
	}
	if !config.DisableVersioning {
		ver, err := normalizeVersionString(config.InitialVersion)
		if err != nil {
			return NewConfigError("invalid " + flag(CLIFlagVersion) + " argument:" + err.Error())
		}
		config.InitialVersion = ver
	}
	return nil
}

func validateUpdateConfig(config *UpdateConfig, db DB) error {
	if len(config.SchemaDir) == 0 && len(config.SchemaName) == 0 {
		return NewConfigError("missing argument; either" + flag(CLIFlagSchemaDir) + " or " +
			flag(CLIFlagSchemaName) + " must be specified")
	}
	if len(config.SchemaDir) > 0 && len(config.SchemaName) > 0 {
		return NewConfigError("either" + flag(CLIFlagSchemaDir) + " or " +
			flag(CLIFlagSchemaName) + " must be specified")
	}
	if len(config.SchemaName) > 0 {
		if !slices.Contains(dbschemas.PathsByDB(db.Type()), config.SchemaName) {
			return NewConfigError(fmt.Sprintf("%s must be one of: %v",
				flag(CLIFlagSchemaName), dbschemas.PathsByDB(db.Type())))
		}
	}
	if len(config.TargetVersion) > 0 {
		ver, err := normalizeVersionString(config.TargetVersion)
		if err != nil {
			return NewConfigError("invalid " + flag(CLIFlagTargetVersion) + " argument:" + err.Error())
		}
		config.TargetVersion = ver
	}
	return nil
}

func flag(opt string) string {
	return "(-" + opt + ")"
}

func schemaFileEnding(schemaName string) string {
	if strings.Contains(schemaName, "cassandra") {
		return ".cql"
	}
	return ".sql"
}
