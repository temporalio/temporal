// Copyright (c) 2018 Uber Technologies, Inc.
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
	"io/ioutil"
	"time"

	"github.com/olivere/elastic"
	"github.com/stretchr/testify/suite"
)

// CreateESClient create ElasticSearch client for test
func CreateESClient(s suite.Suite, url string) *elastic.Client {
	var err error
	esClient, err := elastic.NewClient(
		elastic.SetURL(url),
		elastic.SetRetrier(elastic.NewBackoffRetrier(elastic.NewExponentialBackoff(128*time.Millisecond, 513*time.Millisecond))),
	)
	s.Require().NoError(err)
	return esClient
}

// PutIndexTemplate put index template for test
func PutIndexTemplate(s suite.Suite, esClient *elastic.Client, templateConfigFile, templateName string) {
	// This function is used exclusively in tests. Excluding it from security checks.
	// #nosec
	template, err := ioutil.ReadFile(templateConfigFile)
	s.Require().NoError(err)
	putTemplate, err := esClient.IndexPutTemplate(templateName).BodyString(string(template)).Do(createContext())
	s.Require().NoError(err)
	s.Require().True(putTemplate.Acknowledged)
}

// CreateIndex create test index
func CreateIndex(s suite.Suite, esClient *elastic.Client, indexName string) {
	exists, err := esClient.IndexExists(indexName).Do(createContext())
	s.Require().NoError(err)
	if exists {
		deleteTestIndex, err := esClient.DeleteIndex(indexName).Do(createContext())
		s.Require().Nil(err)
		s.Require().True(deleteTestIndex.Acknowledged)
	}

	createTestIndex, err := esClient.CreateIndex(indexName).Do(createContext())
	s.Require().NoError(err)
	s.Require().True(createTestIndex.Acknowledged)
}

// DeleteIndex delete test index
func DeleteIndex(s suite.Suite, esClient *elastic.Client, indexName string) {
	deleteTestIndex, err := esClient.DeleteIndex(indexName).Do(createContext())
	s.Nil(err)
	s.True(deleteTestIndex.Acknowledged)
}
