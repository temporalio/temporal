// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package schema

import (
	"fmt"

	"github.com/urfave/cli"
)

// VerifyCompatibleVersion ensures that the installed version is greater than or equal to the expected version.
func VerifyCompatibleVersion(
	db DB,
	dbName string,
	expectedVersion string,
) error {

	version, err := db.ReadSchemaVersion()
	if err != nil {
		return fmt.Errorf("unable to read cassandra schema version keyspace/database: %s error: %v", dbName, err.Error())
	}
	// In most cases, the versions should match. However if after a schema upgrade there is a code
	// rollback, the code version (expected version) would fall lower than the actual version in
	// cassandra. This check is to allow such rollbacks since we only make backwards compatible schema
	// changes
	if cmpVersion(version, expectedVersion) < 0 {
		return fmt.Errorf(
			"version mismatch for keyspace/database: %q. Expected version: %s cannot be greater than "+
				"Actual version: %s", dbName, expectedVersion, version,
		)
	}
	return nil
}

// SetupFromConfig sets up schema tables based on the given config
func SetupFromConfig(config *SetupConfig, db DB) error {
	if err := validateSetupConfig(config); err != nil {
		return err
	}
	return newSetupSchemaTask(db, config).Run()
}

// Setup sets up schema tables
func Setup(cli *cli.Context, db DB) error {
	cfg, err := newSetupConfig(cli)
	if err != nil {
		return err
	}
	return newSetupSchemaTask(db, cfg).Run()
}

// Update updates the schema for the specified database
func Update(cli *cli.Context, db DB) error {
	cfg, err := newUpdateConfig(cli)
	if err != nil {
		return err
	}
	return newUpdateSchemaTask(db, cfg).Run()
}

func newUpdateConfig(cli *cli.Context) (*UpdateConfig, error) {
	config := new(UpdateConfig)
	config.SchemaDir = cli.String(CLIOptSchemaDir)
	config.IsDryRun = cli.Bool(CLIOptDryrun)
	config.TargetVersion = cli.String(CLIOptTargetVersion)

	if err := validateUpdateConfig(config); err != nil {
		return nil, err
	}
	return config, nil
}

func newSetupConfig(cli *cli.Context) (*SetupConfig, error) {
	config := new(SetupConfig)
	config.SchemaFilePath = cli.String(CLIOptSchemaFile)
	config.InitialVersion = cli.String(CLIOptVersion)
	config.DisableVersioning = cli.Bool(CLIOptDisableVersioning)
	config.Overwrite = cli.Bool(CLIOptOverwrite)

	if err := validateSetupConfig(config); err != nil {
		return nil, err
	}
	return config, nil
}

func validateSetupConfig(config *SetupConfig) error {
	if len(config.SchemaFilePath) == 0 && config.DisableVersioning {
		return NewConfigError("missing schemaFilePath " + flag(CLIOptSchemaFile))
	}
	if (config.DisableVersioning && len(config.InitialVersion) > 0) ||
		(!config.DisableVersioning && len(config.InitialVersion) == 0) {
		return NewConfigError("either " + flag(CLIOptDisableVersioning) + " or " +
			flag(CLIOptVersion) + " but not both must be specified")
	}
	if !config.DisableVersioning {
		ver, err := parseValidateVersion(config.InitialVersion)
		if err != nil {
			return NewConfigError("invalid " + flag(CLIOptVersion) + " argument:" + err.Error())
		}
		config.InitialVersion = ver
	}
	return nil
}

func validateUpdateConfig(config *UpdateConfig) error {
	if len(config.SchemaDir) == 0 {
		return NewConfigError("missing " + flag(CLIOptSchemaDir) + " argument ")
	}
	if len(config.TargetVersion) > 0 {
		ver, err := parseValidateVersion(config.TargetVersion)
		if err != nil {
			return NewConfigError("invalid " + flag(CLIOptTargetVersion) + " argument:" + err.Error())
		}
		config.TargetVersion = ver
	}
	return nil
}

func flag(opt string) string {
	return "(-" + opt + ")"
}
