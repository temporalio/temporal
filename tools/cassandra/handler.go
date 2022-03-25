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
	"github.com/urfave/cli"

	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
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
func setupSchema(cli *cli.Context, logger log.Logger) error {
	config, err := newCQLClientConfig(cli)
	if err != nil {
		logger.Error("Unable to read config.", tag.Error(schema.NewConfigError(err.Error())))
		return err
	}
	client, err := newCQLClient(config, logger)
	if err != nil {
		logger.Error("Unable to establish CQL session.", tag.Error(err))
		return err
	}
	defer client.Close()
	if err := schema.Setup(cli, client, logger); err != nil {
		logger.Error("Unable to setup CQL schema.", tag.Error(err))
		return err
	}
	return nil
}

// updateSchema executes the updateSchemaTask
// using the given command lien args as input
func updateSchema(cli *cli.Context, logger log.Logger) error {
	config, err := newCQLClientConfig(cli)
	if err != nil {
		logger.Error("Unable to read config.", tag.Error(schema.NewConfigError(err.Error())))
		return err
	}
	client, err := newCQLClient(config, logger)
	if err != nil {
		logger.Error("Unable to establish CQL session.", tag.Error(err))
		return err
	}
	defer client.Close()
	if err := schema.Update(cli, client, logger); err != nil {
		logger.Error("Unable to update CQL schema.", tag.Error(err))
		return err
	}
	return nil
}

func createKeyspace(cli *cli.Context, logger log.Logger) error {
	config, err := newCQLClientConfig(cli)
	if err != nil {
		logger.Error("Unable to read config.", tag.Error(schema.NewConfigError(err.Error())))
		return err
	}
	keyspace := cli.String(schema.CLIOptKeyspace)
	if keyspace == "" {
		logger.Error("Unable to read config.", tag.Error(schema.NewConfigError("missing "+flag(schema.CLIOptKeyspace)+" argument ")))
		return err
	}
	err = doCreateKeyspace(config, keyspace, logger)
	if err != nil {
		logger.Error("Unable to create keyspace.", tag.Error(err))
		return err
	}
	return nil
}

func dropKeyspace(cli *cli.Context, logger log.Logger) error {
	config, err := newCQLClientConfig(cli)
	if err != nil {
		logger.Error("Unable to read config.", tag.Error(schema.NewConfigError(err.Error())))
		return err
	}
	keyspace := cli.String(schema.CLIOptKeyspace)
	if keyspace == "" {
		logger.Error("Unable to read config.", tag.Error(schema.NewConfigError("missing "+flag(schema.CLIOptKeyspace)+" argument ")))
		return err
	}
	err = doDropKeyspace(config, keyspace, logger)
	if err != nil {
		logger.Error("Unable to drop keyspace.", tag.Error(err))
		return err
	}
	return nil
}

func validateHealth(cli *cli.Context, logger log.Logger) error {
	config, err := newCQLClientConfig(cli)
	if err != nil {
		logger.Error("Unable to read config.", tag.Error(schema.NewConfigError(err.Error())))
		return err
	}

	config.Keyspace = systemKeyspace

	client, err := newCQLClient(config, logger)
	if err != nil {
		logger.Error("Unable to establish CQL session.", tag.Error(err))
		return err
	}

	defer client.Close()
	return nil
}

func doCreateKeyspace(cfg *CQLClientConfig, name string, logger log.Logger) error {
	cfg.Keyspace = systemKeyspace
	client, err := newCQLClient(cfg, logger)
	if err != nil {
		return err
	}
	defer client.Close()
	return client.createKeyspace(name)
}

func doDropKeyspace(cfg *CQLClientConfig, name string, logger log.Logger) error {
	cfg.Keyspace = systemKeyspace
	client, err := newCQLClient(cfg, logger)
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
	config := &CQLClientConfig{
		Hosts:                    cli.GlobalString(schema.CLIOptEndpoint),
		Port:                     cli.GlobalInt(schema.CLIOptPort),
		User:                     cli.GlobalString(schema.CLIOptUser),
		Password:                 cli.GlobalString(schema.CLIOptPassword),
		Timeout:                  cli.GlobalInt(schema.CLIOptTimeout),
		Keyspace:                 cli.GlobalString(schema.CLIOptKeyspace),
		numReplicas:              cli.Int(schema.CLIOptReplicationFactor),
		Datacenter:               cli.String(schema.CLIOptDatacenter),
		Consistency:              cli.String(schema.CLIOptConsistency),
		DisableInitialHostLookup: cli.GlobalBool(schema.CLIFlagDisableInitialHostLookup),
	}

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
