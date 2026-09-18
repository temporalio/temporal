package persistencetests

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path"
	"time"

	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/temporal/environment"
	"go.temporal.io/server/tests/testutils"
)

type esTestCluster struct {
	cfg    *esclient.Config
	logger log.Logger

	indexName string
}

var _ PersistenceTestCluster = (*esTestCluster)(nil)

func newEsTestCluster(
	hostname string,
	port int,
	username string,
	password string,
	indexName string,
	logger log.Logger,
) *esTestCluster {
	//nolint:revive // insecure scheme is fine in tests
	addr, err := url.Parse(fmt.Sprintf("http://%s:%d", hostname, port))
	if err != nil {
		panic(err)
	}
	return &esTestCluster{
		cfg: &esclient.Config{
			Version:  environment.GetESVersion(),
			URL:      *addr,
			Username: username,
			Password: password,
			Indices: map[string]string{
				esclient.VisibilityAppName: indexName,
			},
		},
		logger:    logger,
		indexName: indexName,
	}
}

// Config implements [PersistenceTestCluster].
func (c *esTestCluster) Config() config.Persistence {
	return config.Persistence{
		VisibilityStore: "test-es-visibility",
		DataStores: map[string]config.DataStore{
			"test-es-visibility": {
				Elasticsearch: c.cfg,
			},
		},
	}
}

// SetupTestDatabase implements [PersistenceTestCluster].
func (c *esTestCluster) SetupTestDatabase() {
	if err := SetupEsIndex(c.cfg, c.logger); err != nil {
		panic(err)
	}
}

// TearDownTestDatabase implements [PersistenceTestCluster].
func (c *esTestCluster) TearDownTestDatabase() {
	if err := DeleteEsIndex(c.cfg, c.logger); err != nil {
		panic(err)
	}
}

func SetupEsIndex(esConfig *esclient.Config, logger log.Logger) error {
	ctx := context.Background()
	var esClient esclient.IntegrationTestsClient
	op := func() error {
		var err error
		esClient, err = esclient.NewFunctionalTestsClient(esConfig, logger)
		if err != nil {
			return err
		}

		return esClient.Ping(ctx)
	}

	err := backoff.ThrottleRetry(
		op,
		backoff.NewExponentialRetryPolicy(time.Second).WithExpirationInterval(time.Minute),
		nil,
	)
	if err != nil {
		logger.Fatal("Failed to connect to elasticsearch", tag.Error(err))
	}

	exists, err := esClient.IndexExists(ctx, esConfig.GetVisibilityIndex())
	if err != nil {
		return err
	}
	if exists {
		logger.Info("Index already exists.", tag.ESIndex(esConfig.GetVisibilityIndex()))
		err = DeleteEsIndex(esConfig, logger)
		if err != nil {
			return err
		}
	}

	indexTemplateFile := path.Join(testutils.GetRepoRootDirectory(), "schema/elasticsearch/visibility/index_template_v7.json")
	logger.Info("Creating index template.", tag.String("templatePath", indexTemplateFile))
	template, err := os.ReadFile(indexTemplateFile)
	if err != nil {
		return err
	}
	// Template name doesn't matter.
	// This operation is idempotent and won't return an error even if template already exists.
	_, err = esClient.IndexPutTemplate(ctx, "temporal_visibility_v1_template", string(template))
	if err != nil {
		return err
	}
	logger.Info("Index template created.")

	logger.Info("Creating index.", tag.ESIndex(esConfig.GetVisibilityIndex()))
	_, err = esClient.CreateIndex(
		ctx,
		esConfig.GetVisibilityIndex(),
		map[string]any{
			"settings": map[string]any{
				"index": map[string]any{
					"number_of_replicas": 0,
				},
			},
		},
	)
	if err != nil {
		return err
	}
	if err := waitForYellowStatus(esClient, esConfig.GetVisibilityIndex()); err != nil {
		return err
	}
	logger.Info("Index created.", tag.ESIndex(esConfig.GetVisibilityIndex()))

	logger.Info("Add custom search attributes for tests.")
	if err := waitForYellowStatus(esClient, esConfig.GetVisibilityIndex()); err != nil {
		return err
	}
	logger.Info("Index setup complete.", tag.ESIndex(esConfig.GetVisibilityIndex()))

	return nil
}

func waitForYellowStatus(esClient esclient.IntegrationTestsClient, index string) error {
	status, err := esClient.WaitForYellowStatus(context.Background(), index)
	if err != nil {
		return err
	}
	if status == "red" {
		return fmt.Errorf("elasticsearch index status for %s is red", index)
	}
	return nil
}

func DeleteEsIndex(esConfig *esclient.Config, logger log.Logger) error {
	esClient, err := esclient.NewFunctionalTestsClient(esConfig, logger)
	if err != nil {
		return err
	}

	logger.Info("Deleting index.", tag.ESIndex(esConfig.GetVisibilityIndex()))
	_, err = esClient.DeleteIndex(context.Background(), esConfig.GetVisibilityIndex())
	if err != nil {
		return err
	}
	logger.Info("Index deleted.", tag.ESIndex(esConfig.GetVisibilityIndex()))
	return nil
}
