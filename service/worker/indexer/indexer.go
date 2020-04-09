package indexer

import (
	"fmt"

	"github.com/temporalio/temporal/common"
	es "github.com/temporalio/temporal/common/elasticsearch"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/messaging"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/service/dynamicconfig"
)

type (
	// Indexer used to consumer data from kafka then send to ElasticSearch
	Indexer struct {
		config              *Config
		kafkaClient         messaging.Client
		esClient            es.Client
		logger              log.Logger
		metricsClient       metrics.Client
		visibilityProcessor *indexProcessor
		visibilityIndexName string
	}

	// Config contains all configs for indexer
	Config struct {
		IndexerConcurrency       dynamicconfig.IntPropertyFn
		ESProcessorNumOfWorkers  dynamicconfig.IntPropertyFn
		ESProcessorBulkActions   dynamicconfig.IntPropertyFn // max number of requests in bulk
		ESProcessorBulkSize      dynamicconfig.IntPropertyFn // max total size of bytes in bulk
		ESProcessorFlushInterval dynamicconfig.DurationPropertyFn
		ValidSearchAttributes    dynamicconfig.MapPropertyFn
	}
)

const (
	visibilityProcessorName = "visibility-processor"
)

// NewIndexer create a new Indexer
func NewIndexer(config *Config, client messaging.Client, esClient es.Client, esConfig *es.Config,
	logger log.Logger, metricsClient metrics.Client) *Indexer {
	logger = logger.WithTags(tag.ComponentIndexer)

	return &Indexer{
		config:              config,
		kafkaClient:         client,
		esClient:            esClient,
		logger:              logger,
		metricsClient:       metricsClient,
		visibilityIndexName: esConfig.Indices[common.VisibilityAppName],
	}
}

// Start indexer
func (x Indexer) Start() error {
	visibilityApp := common.VisibilityAppName
	visConsumerName := getConsumerName(x.visibilityIndexName)
	x.visibilityProcessor = newIndexProcessor(visibilityApp, visConsumerName, x.kafkaClient, x.esClient,
		visibilityProcessorName, x.visibilityIndexName, x.config, x.logger, x.metricsClient)
	return x.visibilityProcessor.Start()
}

// Stop indexer
func (x Indexer) Stop() {
	x.visibilityProcessor.Stop()
}

func getConsumerName(topic string) string {
	return fmt.Sprintf("%s-consumer", topic)
}
