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

package tests

import (
	"context"
	"os"

	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/log"
	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
)

// CreateESClient create ElasticSearch client for test
func CreateESClient(s *suite.Suite, config *esclient.Config, logger log.Logger) esclient.IntegrationTestsClient {
	client, err := esclient.NewIntegrationTestsClient(config, logger)
	s.Require().NoError(err)
	return client
}

// CreateIndex create test index
func CreateIndex(s *suite.Suite, esClient esclient.IntegrationTestsClient, indexTemplateFile, indexName string) {
	template, err := os.ReadFile(indexTemplateFile)
	s.Require().NoError(err)
	// Template name doesn't matter.
	// This operation is idempotent and won't return an error even if template already exists.
	ack, err := esClient.IndexPutTemplate(context.Background(), "temporal_visibility_v1_template", string(template))
	s.Require().NoError(err)
	s.Require().True(ack)

	// Default configuration of Elasticsearch automatically creates an index on the first access.
	exists, err := esClient.IndexExists(context.Background(), indexName)
	s.Require().NoError(err)
	if !exists {
		// If automatic index creation is disabled, create the index manually.
		_, err = esClient.CreateIndex(context.Background(), indexName)
		s.Require().NoError(err)
		exists, err = esClient.IndexExists(context.Background(), indexName)
		s.Require().NoError(err)
	}
	s.Require().True(exists)
}

// DeleteIndex delete test index
func DeleteIndex(s *suite.Suite, esClient esclient.IntegrationTestsClient, indexName string) {
	acknowledged, err := esClient.DeleteIndex(context.Background(), indexName)
	s.NoError(err)
	s.True(acknowledged)
}
