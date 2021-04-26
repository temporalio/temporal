// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/tools/common/schema"
)

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
	err = DoCreateDatabase(cfg, database)
	if err != nil {
		return handleErr(fmt.Errorf("error creating database:%v", err))
	}
	return nil
}

func DoCreateDatabase(cfg *config.SQL, name string) error {
	cfg.DatabaseName = ""
	conn, err := NewConnection(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	return conn.CreateDatabase(name)
}

// dropDatabase drops a sql database
func dropDatabase(cli *cli.Context) error {
	cfg, err := parseConnectConfig(cli)
	if err != nil {
		return handleErr(schema.NewConfigError(err.Error()))
	}
	database := cli.String(schema.CLIOptDatabase)
	if database == "" {
		return handleErr(schema.NewConfigError("missing " + flag(schema.CLIOptDatabase) + " argument "))
	}
	err = DoDropDatabase(cfg, database)
	if err != nil {
		return handleErr(fmt.Errorf("error creating database:%v", err))
	}
	return nil
}

func DoDropDatabase(cfg *config.SQL, name string) error {
	cfg.DatabaseName = ""
	conn, err := NewConnection(cfg)
	if err != nil {
		return err
	}
	err = conn.DropDatabase(name)
	if err != nil {
		return err
	}
	conn.Close()
	return nil
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
			ServerName:             cli.GlobalString(schema.CLIFlagTLSHostName),
			EnableHostVerification: !cli.GlobalBool(schema.CLIFlagTLSDisableHostVerification),
		}
	}

	if err := ValidateConnectConfig(cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

// ValidateConnectConfig validates params
func ValidateConnectConfig(cfg *config.SQL) error {
	host, _, err := net.SplitHostPort(cfg.ConnectAddr)
	if err != nil {
		return schema.NewConfigError("invalid host and port " + cfg.ConnectAddr)
	}
	if len(host) == 0 {
		return schema.NewConfigError("missing sql endpoint argument " + flag(schema.CLIOptEndpoint))
	}
	if cfg.DatabaseName == "" {
		return schema.NewConfigError("missing " + flag(schema.CLIOptDatabase) + " argument")
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
