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
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/visibility"
	esclient "go.temporal.io/server/common/persistence/visibility/elasticsearch/client"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/searchattribute"
)

// NewVisibilityManager create a visibility manager for Elasticsearch
// In history, it only writes data;
// In frontend, it only needs ES client and related config for reading data
func NewVisibilityManager(
	indexName string,
	esClient esclient.Client,
	cfg *config.VisibilityConfig,
	searchAttributesProvider searchattribute.Provider,
	processor Processor,
	metricsClient metrics.Client,
	log log.Logger,
) visibility.VisibilityManager {

	visStore := NewVisibilityStore(esClient, indexName, searchAttributesProvider, processor, cfg, metricsClient)
	visManager := visibility.NewVisibilityManagerImpl(visStore, searchAttributesProvider, indexName, log)

	if cfg != nil {
		// wrap with rate limiter
		if cfg.MaxQPS != nil && cfg.MaxQPS() != 0 {
			esRateLimiter := quotas.NewDefaultOutgoingDynamicRateLimiter(
				func() float64 { return float64(cfg.MaxQPS()) },
			)
			visManager = visibility.NewVisibilityPersistenceRateLimitedClient(visManager, esRateLimiter, log)
		}
	}
	if metricsClient != nil {
		// wrap with metrics
		visManager = NewVisibilityManagerMetrics(visManager, metricsClient, log)
	}

	return visManager
}
