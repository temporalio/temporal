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

package sql

import (
	"fmt"
	"log"
	"net"
	"net/url"

	"github.com/urfave/cli"

	"github.com/uber/cadence/common/auth"
	"github.com/uber/cadence/common/service/config"
	"github.com/uber/cadence/schema/mysql"
	"github.com/uber/cadence/tools/common/schema"
)

// VerifyCompatibleVersion ensures that the installed version of cadence and visibility
// is greater than or equal to the expected version.
func VerifyCompatibleVersion(
	cfg config.Persistence,
) error {

	ds, ok := cfg.DataStores[cfg.DefaultStore]
	if ok && ds.SQL != nil {
		err := CheckCompatibleVersion(*ds.SQL, mysql.Version)
		if err != nil {
			return err
		}
	}
	ds, ok = cfg.DataStores[cfg.VisibilityStore]
	if ok && ds.SQL != nil {
		err := CheckCompatibleVersion(*ds.SQL, mysql.VisibilityVersion)
		if err != nil {
			return err
		}
	}
	return nil
}

// CheckCompatibleVersion check the version compatibility
func CheckCompatibleVersion(
	cfg config.SQL,
	expectedVersion string,
) error {
	connection, err := NewConnection(&cfg)

	if err != nil {
		return fmt.Errorf("unable to create SQL connection: %v", err.Error())
	}
	defer connection.Close()

	return schema.VerifyCompatibleVersion(connection, cfg.DatabaseName, expectedVersion)
}

// setupSchema executes the setupSchemaTask
// using the given command line arguments
// as input
func setupSchema(cli *cli.Context) error {
	cfg, err := parseConnectConfig(cli)
	if err != nil {
		return handleErr(schema.NewConfigError(err.Error()))
	}
	conn, err := NewConnection(cfg)
	if err != nil {
		return handleErr(err)
	}
	defer conn.Close()
	if err := schema.Setup(cli, conn); err != nil {
		return handleErr(err)
	}
	return nil
}

// updateSchema executes the updateSchemaTask
// using the given command lien args as input
func updateSchema(cli *cli.Context) error {
	cfg, err := parseConnectConfig(cli)
	if err != nil {
		return handleErr(schema.NewConfigError(err.Error()))
	}
	if cfg.DatabaseName == schema.DryrunDBName {
		if err := doCreateDatabase(cfg, cfg.DatabaseName); err != nil {
			return handleErr(fmt.Errorf("error creating dryrun database: %v", err))
		}
		defer doDropDatabase(cfg, cfg.DatabaseName)
	}
	conn, err := NewConnection(cfg)
	if err != nil {
		return handleErr(err)
	}
	defer conn.Close()
	if err := schema.Update(cli, conn); err != nil {
		return handleErr(err)
	}
	return nil
}

// createDatabase creates a sql database
func createDatabase(cli *cli.Context) error {
	cfg, err := parseConnectConfig(cli)
	if err != nil {
		return handleErr(schema.NewConfigError(err.Error()))
	}
	database := cli.String(schema.CLIOptDatabase)
	if database == "" {
		return handleErr(schema.NewConfigError("missing " + flag(schema.CLIOptDatabase) + " argument "))
	}
	err = doCreateDatabase(cfg, database)
	if err != nil {
		return handleErr(fmt.Errorf("error creating database:%v", err))
	}
	return nil
}

func doCreateDatabase(cfg *config.SQL, name string) error {
	cfg.DatabaseName = ""
	conn, err := NewConnection(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	return conn.CreateDatabase(name)
}

func doDropDatabase(cfg *config.SQL, name string) {
	cfg.DatabaseName = ""
	conn, err := NewConnection(cfg)
	if err != nil {
		logErr(err)
		return
	}
	err = conn.DropDatabase(name)
	if err != nil {
		logErr(err)
	}
	conn.Close()
}

func parseConnectConfig(cli *cli.Context) (*config.SQL, error) {
	cfg := new(config.SQL)

	host := cli.GlobalString(schema.CLIOptEndpoint)
	port := cli.GlobalInt(schema.CLIOptPort)
	cfg.ConnectAddr = fmt.Sprintf("%s:%v", host, port)
	cfg.User = cli.GlobalString(schema.CLIOptUser)
	cfg.Password = cli.GlobalString(schema.CLIOptPassword)
	cfg.DatabaseName = cli.GlobalString(schema.CLIOptDatabase)
	cfg.PluginName = cli.GlobalString(schema.CLIOptPluginName)
	isDryRun := cli.Bool(schema.CLIOptDryrun)

	if cfg.ConnectAttributes == nil {
		cfg.ConnectAttributes = map[string]string{}
	}
	connectAttributesQueryString := cli.GlobalString(schema.CLIOptConnectAttributes)
	if connectAttributesQueryString != "" {
		values, err := url.ParseQuery(connectAttributesQueryString)
		if err != nil {
			return nil, fmt.Errorf("invalid connect attributes: %v", err)
		}
		for key, vals := range values {
			// check to ensure only one value is provider per key
			if len(vals) > 1 {
				return nil, fmt.Errorf("invalid connect attribute %v, only 1 value allowed: %v", key, vals)
			}
			cfg.ConnectAttributes[key] = vals[0]
		}
	}

	if cli.GlobalBool(schema.CLIFlagEnableTLS) {
		cfg.TLS = &auth.TLS{
			Enabled:                true,
			CertFile:               cli.GlobalString(schema.CLIFlagTLSCertFile),
			KeyFile:                cli.GlobalString(schema.CLIFlagTLSKeyFile),
			CaFile:                 cli.GlobalString(schema.CLIFlagTLSCaFile),
			EnableHostVerification: cli.GlobalBool(schema.CLIFlagTLSEnableHostVerification),
		}
	}

	if err := ValidateConnectConfig(cfg, isDryRun); err != nil {
		return nil, err
	}

	return cfg, nil
}

// ValidateConnectConfig validates params
func ValidateConnectConfig(cfg *config.SQL, isDryRun bool) error {
	host, _, err := net.SplitHostPort(cfg.ConnectAddr)
	if err != nil {
		return schema.NewConfigError("invalid host and port " + cfg.ConnectAddr)
	}
	if len(host) == 0 {
		return schema.NewConfigError("missing sql endpoint argument " + flag(schema.CLIOptEndpoint))
	}
	if cfg.DatabaseName == "" {
		if !isDryRun {
			return schema.NewConfigError("missing " + flag(schema.CLIOptDatabase) + " argument")
		}
		cfg.DatabaseName = schema.DryrunDBName
	}
	if cfg.TLS != nil && cfg.TLS.Enabled {
		enabledCaFile := cfg.TLS.CaFile != ""
		enabledCertFile := cfg.TLS.CertFile != ""
		enabledKeyFile := cfg.TLS.KeyFile != ""

		if (enabledCertFile && !enabledKeyFile) || (!enabledCertFile && enabledKeyFile) {
			return schema.NewConfigError("must have both CertFile and KeyFile set")
		}

		if !enabledCaFile && !enabledCertFile && !enabledKeyFile {
			return schema.NewConfigError("must provide tls certs to use")
		}
	}
	return nil
}

func flag(opt string) string {
	return "(-" + opt + ")"
}

func handleErr(err error) error {
	log.Println(err)
	return err
}

func logErr(err error) {
	log.Println(err)
}
