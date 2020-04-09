package elasticsearch

import (
	es "github.com/temporalio/temporal/common/elasticsearch"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/messaging"
	"github.com/temporalio/temporal/common/metrics"
	p "github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/quotas"
	"github.com/temporalio/temporal/common/service/config"
)

// NewESVisibilityManager create a visibility manager for ElasticSearch
// In history, it only needs kafka producer for writing data;
// In frontend, it only needs ES client and related config for reading data
func NewESVisibilityManager(indexName string, esClient es.Client, config *config.VisibilityConfig,
	producer messaging.Producer, metricsClient metrics.Client, log log.Logger) p.VisibilityManager {

	visibilityFromESStore := NewElasticSearchVisibilityStore(esClient, indexName, producer, config, log)
	visibilityFromES := p.NewVisibilityManagerImpl(visibilityFromESStore, log)

	if config != nil {
		// wrap with rate limiter
		if config.MaxQPS != nil && config.MaxQPS() != 0 {
			esRateLimiter := quotas.NewDynamicRateLimiter(
				func() float64 {
					return float64(config.MaxQPS())
				},
			)
			visibilityFromES = p.NewVisibilityPersistenceRateLimitedClient(visibilityFromES, esRateLimiter, log)
		}
		if config.EnableSampling != nil && config.EnableSampling() {
			visibilityFromES = p.NewVisibilitySamplingClient(visibilityFromES, config, metricsClient, log)
		}
	}
	if metricsClient != nil {
		// wrap with metrics
		visibilityFromES = NewVisibilityMetricsClient(visibilityFromES, metricsClient, log)
	}

	return visibilityFromES
}
