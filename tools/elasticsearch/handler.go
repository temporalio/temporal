package elasticsearch

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	"github.com/urfave/cli"
	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/schema"
	commonschema "go.temporal.io/server/tools/common/schema"
)

func createClient(cli *cli.Context, logger log.Logger) (esclient.CLIClient, error) {
	cfg, err := parseElasticConfig(cli)
	if err != nil {
		logger.Error("Unable to parse elasticsearch config.", tag.Error(err))
		return nil, err
	}

	if cfg.AWSRequestSigning.Enabled {
		awsHTTPClient, err := esclient.NewAwsHttpClient(cfg.AWSRequestSigning)
		if err != nil {
			logger.Error("Unable to create AWS HTTP client.", tag.Error(err))
			return nil, err
		}
		cfg.SetHttpClient(awsHTTPClient)
	}

	esClient, err := esclient.NewCLIClient(cfg, logger)
	if err != nil {
		logger.Error("Unable to create elasticsearch client.", tag.Error(err))
		return nil, err
	}

	return esClient, nil
}

// setupSchema creates cluster settings and index template, but not the index
func setupSchema(cli *cli.Context, logger log.Logger) error {
	client, err := createClient(cli, logger)
	if err != nil {
		logger.Error("Unable to read config.", tag.Error(commonschema.NewConfigError(err.Error())))
		return err
	}

	settingsContent, err := schema.ElasticsearchClusterSettings()
	if err != nil {
		logger.Error("Unable to load embedded cluster settings.", tag.Error(err))
		return err
	}

	templateContent, err := schema.ElasticsearchIndexTemplate()
	if err != nil {
		logger.Error("Unable to load embedded index template.", tag.Error(err))
		return err
	}

	task := SetupTask{
		esClient: client,
		logger:   logger,
		config: &SetupConfig{
			SettingsContent: settingsContent,
			TemplateContent: templateContent,
			VisibilityIndex: "", // Don't create index in setup-schema
			FailSilently:    cli.Bool(CLIOptFailSilently),
		},
	}

	return task.RunSchemaSetup()
}

// ping the elasticsearch host and return the json response
func ping(cli *cli.Context, logger log.Logger) error {
	client, err := createClient(cli, logger)
	if err != nil {
		logger.Error("Unable to read config.", tag.Error(commonschema.NewConfigError(err.Error())))
		return err
	}

	err = client.Ping(context.TODO())
	if err != nil {
		logger.Error("Ping failed", tag.Error(err))
		return err
	}

	logger.Info("Pong - Elasticsearch is reachable")
	return nil
}

func parseElasticConfig(cli *cli.Context) (*esclient.Config, error) {
	cfg := new(esclient.Config)

	u, err := url.Parse(cli.GlobalString(commonschema.CLIOptEndpoint))
	if err != nil {
		return nil, fmt.Errorf("invalid elasticsearch URL %q: %w", cli.GlobalString(commonschema.CLIOptEndpoint), err)
	}

	cfg.URL = *u
	cfg.Username = cli.GlobalString(commonschema.CLIOptUser)
	cfg.Password = cli.GlobalString(commonschema.CLIOptPassword)
	cfg.Version = "v7" // Fixed schema version 7
	cfg.Indices = map[string]string{}

	if cli.String(CLIOptVisibilityIndex) != "" {
		cfg.Indices[esclient.VisibilityAppName] = cli.String(CLIOptVisibilityIndex)
	}

	if cli.GlobalString(CLIOptAWSCredentials) != "" {
		cfg.AWSRequestSigning.CredentialProvider = cli.GlobalString(CLIOptAWSCredentials)
		cfg.AWSRequestSigning.Enabled = true

		if cfg.AWSRequestSigning.CredentialProvider == "static" {
			cfg.AWSRequestSigning.Static.AccessKeyID = cfg.Username
			cfg.AWSRequestSigning.Static.SecretAccessKey = cfg.Password
			cfg.AWSRequestSigning.Static.Token = cli.GlobalString(CLIOptAWSToken)
		}
	}

	if cli.GlobalBool(commonschema.CLIFlagEnableTLS) {
		cfg.TLS = &auth.TLS{
			Enabled:                true,
			CertFile:               cli.GlobalString(commonschema.CLIFlagTLSCertFile),
			KeyFile:                cli.GlobalString(commonschema.CLIFlagTLSKeyFile),
			CaFile:                 cli.GlobalString(commonschema.CLIFlagTLSCaFile),
			ServerName:             cli.GlobalString(commonschema.CLIFlagTLSHostName),
			EnableHostVerification: !cli.GlobalBool(commonschema.CLIFlagTLSDisableHostVerification),
		}
	}

	return cfg, nil
}

// updateSchema updates the index template to the latest version, or index mappings if --index is specified
func updateSchema(cli *cli.Context, logger log.Logger) error {
	client, err := createClient(cli, logger)
	if err != nil {
		logger.Error("Unable to read config.", tag.Error(commonschema.NewConfigError(err.Error())))
		return err
	}

	templateContent, err := schema.ElasticsearchIndexTemplate()
	if err != nil {
		logger.Error("Unable to load embedded index template.", tag.Error(err))
		return err
	}

	indexName := cli.String(CLIOptVisibilityIndex)

	task := SetupTask{
		esClient: client,
		logger:   logger,
		config: &SetupConfig{
			SettingsContent: "", // Don't update cluster settings
			TemplateContent: templateContent,
			VisibilityIndex: indexName,
			FailSilently:    cli.Bool(CLIOptFailSilently),
		},
	}

	err = task.RunTemplateUpgrade()
	if err != nil {
		return err
	}

	if indexName != "" {
		return task.RunIndexUpdate()
	}
	return nil
}

// createIndex creates a new visibility index
func createIndex(cli *cli.Context, logger log.Logger) error {
	client, err := createClient(cli, logger)
	if err != nil {
		logger.Error("Unable to read config.", tag.Error(commonschema.NewConfigError(err.Error())))
		return err
	}

	task := SetupTask{
		esClient: client,
		logger:   logger,
		config: &SetupConfig{
			SettingsContent: "", // Don't update cluster settings
			TemplateContent: "", // Don't update template
			VisibilityIndex: cli.String(CLIOptVisibilityIndex),
			FailSilently:    cli.Bool(CLIOptFailSilently),
		},
	}

	return task.RunIndexCreation(context.Background())
}

// dropIndex deletes a visibility index
func dropIndex(cli *cli.Context, logger log.Logger) error {
	client, err := createClient(cli, logger)
	if err != nil {
		logger.Error("Unable to read config.", tag.Error(commonschema.NewConfigError(err.Error())))
		return err
	}

	indexName := cli.String(CLIOptVisibilityIndex)
	if indexName == "" {
		err := errors.New("index name is required")
		logger.Error("Missing index name.", tag.Error(err))
		return err
	}

	failSilently := cli.Bool(CLIOptFailSilently)

	success, err := client.DeleteIndex(context.TODO(), indexName)
	if err != nil {
		if !failSilently {
			logger.Error("Index deletion failed", tag.Error(err), tag.NewStringTag("indexName", indexName))
			return err
		}
		logger.Warn("Index deletion failed", tag.Error(err), tag.NewStringTag("indexName", indexName))
		return nil
	} else if !success {
		err := errors.New("acknowledged=false")
		if !failSilently {
			logger.Error("Index deletion failed without error", tag.Error(err), tag.NewStringTag("indexName", indexName))
			return err
		}
		logger.Warn("Index deletion failed without error", tag.Error(err), tag.NewStringTag("indexName", indexName))
		return nil
	}

	logger.Info("Index deleted successfully", tag.NewStringTag("indexName", indexName))
	return nil
}

func flag(opt string) string {
	return fmt.Sprintf("(--%s)", opt)
}
