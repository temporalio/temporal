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

package cassandra

import (
	"fmt"
	"log"

	"github.com/urfave/cli"

	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/environment"
	"go.temporal.io/server/tools/common/schema"
)

const defaultNumReplicas = 1

// SetupSchemaConfig contains the configuration params needed to setup schema tables
type SetupSchemaConfig struct {
	CQLClientConfig
	schema.SetupConfig
}

// setupSchema executes the setupSchemaTask
// using the given command line arguments
// as input
func setupSchema(cli *cli.Context) error {
	config, err := newCQLClientConfig(cli)
	if err != nil {
		return handleErr(schema.NewConfigError(err.Error()))
	}
	client, err := newCQLClient(config)
	if err != nil {
		return handleErr(err)
	}
	defer client.Close()
	if err := schema.Setup(cli, client); err != nil {
		return handleErr(err)
	}
	return nil
}

// updateSchema executes the updateSchemaTask
// using the given command lien args as input
func updateSchema(cli *cli.Context) error {
	config, err := newCQLClientConfig(cli)
	if err != nil {
		return handleErr(schema.NewConfigError(err.Error()))
	}
	client, err := newCQLClient(config)
	if err != nil {
		return handleErr(err)
	}
	defer client.Close()
	if err := schema.Update(cli, client); err != nil {
		return handleErr(err)
	}
	return nil
}

func createKeyspace(cli *cli.Context) error {
	config, err := newCQLClientConfig(cli)
	if err != nil {
		return handleErr(schema.NewConfigError(err.Error()))
	}
	keyspace := cli.String(schema.CLIOptKeyspace)
	if keyspace == "" {
		return handleErr(schema.NewConfigError("missing " + flag(schema.CLIOptKeyspace) + " argument "))
	}
	err = doCreateKeyspace(*config, keyspace)
	if err != nil {
		return handleErr(fmt.Errorf("error creating keyspace:%+v", err))
	}
	return nil
}

func dropKeyspace(cli *cli.Context) error {
	config, err := newCQLClientConfig(cli)
	if err != nil {
		return handleErr(schema.NewConfigError(err.Error()))
	}
	keyspace := cli.String(schema.CLIOptKeyspace)
	if keyspace == "" {
		return handleErr(schema.NewConfigError("missing " + flag(schema.CLIOptKeyspace) + " argument "))
	}
	err = doDropKeyspace(*config, keyspace)
	if err != nil {
		return handleErr(fmt.Errorf("error dropping keyspace:%+v", err))
	}
	return nil
}

func validateHealth(cli *cli.Context) error {
	config, err := newCQLClientConfig(cli)
	if err != nil {
		return handleErr(schema.NewConfigError(err.Error()))
	}

	config.Keyspace = systemKeyspace

	client, err := newCQLClient(config)
	if err != nil {
		return handleErr(fmt.Errorf("unable to establish CQL session:%+v", err))
	}

	defer client.Close()
	return nil
}

func doCreateKeyspace(cfg CQLClientConfig, name string) error {
	cfg.Keyspace = systemKeyspace
	client, err := newCQLClient(&cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	return client.createKeyspace(name)
}

func doDropKeyspace(cfg CQLClientConfig, name string) error {
	cfg.Keyspace = systemKeyspace
	client, err := newCQLClient(&cfg)
	if err != nil {
		return err
	}
	err = client.dropKeyspace(name)
	if err != nil {
		return err
	}
	client.Close()
	return nil
}

func newCQLClientConfig(cli *cli.Context) (*CQLClientConfig, error) {
	config := new(CQLClientConfig)
	config.Hosts = cli.GlobalString(schema.CLIOptEndpoint)
	config.Port = cli.GlobalInt(schema.CLIOptPort)
	config.User = cli.GlobalString(schema.CLIOptUser)
	config.Password = cli.GlobalString(schema.CLIOptPassword)
	config.Timeout = cli.GlobalInt(schema.CLIOptTimeout)
	config.Keyspace = cli.GlobalString(schema.CLIOptKeyspace)
	config.numReplicas = cli.Int(schema.CLIOptReplicationFactor)
	config.Datacenter = cli.String(schema.CLIOptDatacenter)

	if cli.GlobalBool(schema.CLIFlagEnableTLS) {
		config.TLS = &auth.TLS{
			Enabled:                true,
			CertFile:               cli.GlobalString(schema.CLIFlagTLSCertFile),
			KeyFile:                cli.GlobalString(schema.CLIFlagTLSKeyFile),
			CaFile:                 cli.GlobalString(schema.CLIFlagTLSCaFile),
			ServerName:             cli.GlobalString(schema.CLIFlagTLSHostName),
			EnableHostVerification: !cli.GlobalBool(schema.CLIFlagTLSDisableHostVerification),
		}
	}

	if err := validateCQLClientConfig(config); err != nil {
		return nil, err
	}
	return config, nil
}

func validateCQLClientConfig(config *CQLClientConfig) error {
	if len(config.Hosts) == 0 {
		return schema.NewConfigError("missing cassandra endpoint argument " + flag(schema.CLIOptEndpoint))
	}
	if config.Keyspace == "" {
		return schema.NewConfigError("missing " + flag(schema.CLIOptKeyspace) + " argument ")
	}
	if config.Port == 0 {
		config.Port = environment.GetCassandraPort()
	}
	if config.numReplicas == 0 {
		config.numReplicas = defaultNumReplicas
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
