package elasticsearch

import (
	"context"
	"fmt"
	"net/url"

	"github.com/urfave/cli/v2"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/tools/common/schema"
)

func createClient(cli *cli.Context, logger log.Logger) (esclient.CLIClient, error) {
	cfg, err := parseElasticConfig(cli)
	if err != nil {
		logger.Error("Unable to parse elasticsearch config.", tag.Error(err))
		return nil, err
	}

	if cfg.AWSRequestSigning.Enabled {
		awsHttpClient, err := esclient.NewAwsHttpClient(cfg.AWSRequestSigning)
		if err != nil {
			logger.Error("Unable to create AWS HTTP client.", tag.Error(err))
			return nil, err
		}
		cfg.SetHttpClient(awsHttpClient)
	}

	esClient, err := esclient.NewCLIClient(cfg, logger)
	if err != nil {
		logger.Error("Unable to create elasticsearch client.", tag.Error(err))
		return nil, err
	}

	return esClient, nil
}

// setup creates a ESClient and runs SetupTask
func setup(cli *cli.Context, logger log.Logger) error {
	client, err := createClient(cli, logger)
	if err != nil {
		logger.Error("Unable to read config.", tag.Error(schema.NewConfigError(err.Error())))
		return err
	}

	task := SetupTask{
		esClient: client,
		logger:   logger,
		config: &SetupConfig{
			SettingsFilePath: cli.String(CLIOptSettingsFile),
			TemplateFilePath: cli.String(CLIOptTemplateFile),
			VisibilityIndex:  cli.String(CLIOptVisibilityIndex),
			FailSilently:     cli.Bool(CLIOptFailSilently),
		},
	}

	return task.Run()
}

// ping the elasticsearch host and return the json response
func ping(cli *cli.Context, logger log.Logger) error {
	client, err := createClient(cli, logger)
	if err != nil {
		logger.Error("Unable to read config.", tag.Error(schema.NewConfigError(err.Error())))
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

	u, err := url.Parse(cli.String(CLIOptESURL))
	if err != nil {
		return nil, fmt.Errorf("invalid elasticsearch URL %q: %w", cli.String(CLIOptESURL), err)
	}

	cfg.URL = *u
	cfg.Username = cli.String(CLIOptESUsername)
	cfg.Password = cli.String(CLIOptESPassword)
	cfg.Version = cli.String(CLIOptESVersion)
	cfg.Indices = map[string]string{}

	if cli.String(CLIOptVisibilityIndex) != "" {
		cfg.Indices[esclient.VisibilityAppName] = cli.String(CLIOptVisibilityIndex)
	}

	if cli.String(CLIOptAWSCredentials) != "" {
		cfg.AWSRequestSigning.CredentialProvider = cli.String(CLIOptAWSCredentials)
		cfg.AWSRequestSigning.Enabled = true

		if cfg.AWSRequestSigning.CredentialProvider == "static" {
			cfg.AWSRequestSigning.Static.AccessKeyID = cfg.Username
			cfg.AWSRequestSigning.Static.SecretAccessKey = cfg.Password
			cfg.AWSRequestSigning.Static.Token = cli.String(CLIOptAWSToken)
		}
	}

	return cfg, nil
}

func flag(opt string) string {
	return fmt.Sprintf("(--%s)", opt)
}
