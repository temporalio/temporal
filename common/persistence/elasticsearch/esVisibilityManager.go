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
	es "go.temporal.io/server/common/elasticsearch"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/messaging"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/service/config"
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
