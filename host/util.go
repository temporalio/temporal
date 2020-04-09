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
	putTemplate, err := esClient.IndexPutTemplate(templateName).BodyString(string(template)).Do(NewContext())
	s.Require().NoError(err)
	s.Require().True(putTemplate.Acknowledged)
}

// CreateIndex create test index
func CreateIndex(s suite.Suite, esClient *elastic.Client, indexName string) {
	exists, err := esClient.IndexExists(indexName).Do(NewContext())
	s.Require().NoError(err)
	if exists {
		deleteTestIndex, err := esClient.DeleteIndex(indexName).Do(NewContext())
		s.Require().NoError(err)
		s.Require().True(deleteTestIndex.Acknowledged)
	}

	createTestIndex, err := esClient.CreateIndex(indexName).Do(NewContext())
	s.Require().NoError(err)
	s.Require().True(createTestIndex.Acknowledged)
}

// DeleteIndex delete test index
func DeleteIndex(s suite.Suite, esClient *elastic.Client, indexName string) {
	deleteTestIndex, err := esClient.DeleteIndex(indexName).Do(NewContext())
	s.NoError(err)
	s.True(deleteTestIndex.Acknowledged)
}
