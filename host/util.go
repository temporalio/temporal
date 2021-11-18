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

package host

import (
	"context"
	"os"

	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/log"
	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
)

// CreateESClient create ElasticSearch client for test
func CreateESClient(s suite.Suite, config *esclient.Config, logger log.Logger) esclient.IntegrationTestsClient {
	client, err := esclient.NewIntegrationTestsClient(config, logger)
	s.Require().NoError(err)
	return client
}

// PutIndexTemplate put index template for test
func PutIndexTemplate(s suite.Suite, esClient esclient.IntegrationTestsClient, templateConfigFile, templateName string) {
	// This function is used exclusively in tests. Excluding it from security checks.
	// #nosec
	template, err := os.ReadFile(templateConfigFile)
	s.Require().NoError(err)
	acknowledged, err := esClient.IndexPutTemplate(context.Background(), templateName, string(template))
	s.Require().NoError(err)
	s.Require().True(acknowledged)
}

// CreateIndex create test index
func CreateIndex(s suite.Suite, esClient esclient.IntegrationTestsClient, indexName string) {
	exists, err := esClient.IndexExists(context.Background(), indexName)
	s.Require().NoError(err)
	if exists {
		acknowledged, err := esClient.DeleteIndex(context.Background(), indexName)
		s.Require().NoError(err)
		s.Require().True(acknowledged)
	}

	acknowledged, err := esClient.CreateIndex(context.Background(), indexName)
	s.Require().NoError(err)
	s.Require().True(acknowledged)
}

// DeleteIndex delete test index
func DeleteIndex(s suite.Suite, esClient esclient.IntegrationTestsClient, indexName string) {
	acknowledged, err := esClient.DeleteIndex(context.Background(), indexName)
	s.NoError(err)
	s.True(acknowledged)
}
