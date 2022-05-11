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

package elasticsearch

import (
	"context"
	"net/http"
	"net/url"
	"time"

	"github.com/urfave/cli"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/tools/common/schema"
)

func createClient(cli *cli.Context, logger log.Logger) (esclient.CLIClient, error) {
	cfg, err := parseElasticConfig(cli)
	if err != nil {
		return nil, err
	}

	httpClient := http.DefaultClient
	if cfg.AWSRequestSigning.Enabled {
		httpClient, err = esclient.NewAwsHttpClient(cfg.AWSRequestSigning)
		if err != nil {
			return nil, err
		}
	}

	esClient, err := esclient.NewCLIClient(cfg, httpClient, logger)
	if err != nil {
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

// ping continuously pings the elasticsearch host and returns on a response
func ping(cli *cli.Context, logger log.Logger) error {
	client, err := createClient(cli, logger)
	if err != nil {
		logger.Error("Unable to read config.", tag.Error(schema.NewConfigError(err.Error())))
		return err
	}

	for {
		success, err := client.Ping(context.TODO())
		if err == nil && success {
			logger.Info("Received response from ES server")
			return nil
		}

		time.Sleep(1 * time.Second)
	}

}

func parseElasticConfig(cli *cli.Context) (*esclient.Config, error) {
	cfg := new(esclient.Config)

	u, err := url.Parse(cli.GlobalString(CLIOptESURL))
	if err != nil {
		return nil, err
	}

	cfg.URL = *u
	cfg.Username = cli.GlobalString(CLIOptESUsername)
	cfg.Password = cli.GlobalString(CLIOptESPassword)
	cfg.Indices = map[string]string{}

	if cli.GlobalString(CLIOptVisibilityIndex) != "" {
		cfg.Indices[esclient.VisibilityAppName] = cli.GlobalString(CLIOptVisibilityIndex)
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

	return cfg, nil
}

func flag(opt string) string {
	return "(-" + opt + ")"
}
