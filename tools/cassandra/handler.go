package cassandra

import (
	"fmt"
	"strings"

	"github.com/urfave/cli/v2"
	"go.temporal.io/server/common/auth"
	c "go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/temporal/environment"
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
// using the given command line args as input
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
	keyspace := cli.String(schema.CLIFlagKeyspace)
	if keyspace == "" {
		err := fmt.Errorf("missing %s argument", flag(schema.CLIFlagKeyspace))
		logger.Error("Unable to read config.", tag.Error(schema.NewConfigError(err.Error())))
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
	keyspace := cli.String(schema.CLIFlagKeyspace)
	if keyspace == "" {
		err := fmt.Errorf("missing %s argument", flag(schema.CLIFlagKeyspace))
		logger.Error("Unable to read config.", tag.Error(schema.NewConfigError(err.Error())))
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
	defer client.Close()
	return client.dropKeyspace(name)
}

func newCQLClientConfig(ctx *cli.Context) (*CQLClientConfig, error) {
	config := &CQLClientConfig{
		Hosts:                    ctx.String(schema.CLIFlagEndpoint),
		Port:                     ctx.Int(schema.CLIFlagPort),
		User:                     ctx.String(schema.CLIFlagUser),
		Password:                 ctx.String(schema.CLIFlagPassword),
		AllowedAuthenticators:    cli.NewStringSlice(schema.CLIFlagAllowedAuthenticators).Value(),
		Timeout:                  ctx.Int(schema.CLIFlagTimeout),
		Keyspace:                 ctx.String(schema.CLIFlagKeyspace),
		numReplicas:              ctx.Int(schema.CLIFlagReplicationFactor),
		Datacenter:               ctx.String(schema.CLIFlagDatacenter),
		Consistency:              ctx.String(schema.CLIOptConsistency),
		DisableInitialHostLookup: ctx.Bool(schema.CLIFlagDisableInitialHostLookup),
	}

	if ctx.Bool(schema.CLIFlagEnableTLS) {
		config.TLS = &auth.TLS{
			Enabled:                true,
			CertFile:               ctx.String(schema.CLIFlagTLSCertFile),
			KeyFile:                ctx.String(schema.CLIFlagTLSKeyFile),
			CaFile:                 ctx.String(schema.CLIFlagTLSCaFile),
			ServerName:             ctx.String(schema.CLIFlagTLSHostName),
			EnableHostVerification: !ctx.Bool(schema.CLIFlagTLSDisableHostVerification),
		}
	}

	config.AddressTranslator = &c.CassandraAddressTranslator{
		Translator: ctx.String(schema.CLIOptAddressTranslator),
		Options:    parseOptionsMap(ctx.String(schema.CLIOptAddressTranslatorOptions)),
	}

	if err := validateCQLClientConfig(config); err != nil {
		return nil, err
	}
	return config, nil
}

func parseOptionsMap(value string) map[string]string {
	if len(value) == 0 {
		return make(map[string]string)
	}

	parsedMap := make(map[string]string)

	for pair := range strings.SplitSeq(value, ",") {
		trimmedPair := strings.ReplaceAll(pair, " ", "")
		if len(trimmedPair) == 0 {
			continue
		}
		splitPair := strings.Split(trimmedPair, "=")
		if len(splitPair) != 2 {
			continue
		}
		if len(splitPair[0]) == 0 || len(splitPair[1]) == 0 {
			continue
		}
		parsedMap[splitPair[0]] = splitPair[1]
	}

	return parsedMap
}

func validateCQLClientConfig(config *CQLClientConfig) error {
	if len(config.Hosts) == 0 {
		return schema.NewConfigError("missing cassandra endpoint argument " + flag(schema.CLIFlagEndpoint))
	}
	if config.Keyspace == "" {
		return schema.NewConfigError("missing " + flag(schema.CLIFlagKeyspace) + " argument ")
	}
	if config.Port == 0 {
		config.Port = environment.GetCassandraPort()
	}
	if config.numReplicas == 0 {
		config.numReplicas = defaultNumReplicas
	}

	if config.AddressTranslator != nil && len(config.AddressTranslator.Options) != 0 {
		if len(config.AddressTranslator.Translator) == 0 {
			return schema.NewConfigError("missing address translator argument " + flag(schema.CLIOptAddressTranslator))
		}
	}

	return nil
}

func flag(opt string) string {
	return "(-" + opt + ")"
}
