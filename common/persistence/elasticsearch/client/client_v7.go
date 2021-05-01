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

package client

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/olivere/elastic/v7"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
)

type (
	// clientV7 implements Client
	clientV7 struct {
		esClient *elastic.Client
	}
)

var _ Client = (*clientV7)(nil)

// newClientV7 create a ES client
func newClientV7(config *config.Elasticsearch, httpClient *http.Client, logger log.Logger) (*clientV7, error) {
	options := []elastic.ClientOptionFunc{
		elastic.SetURL(config.URL.String()),
		elastic.SetSniff(false),
		elastic.SetBasicAuth(config.Username, config.Password),

		// Disable health check so we don't block client creation (and thus temporal server startup)
		// if the ES instance happens to be down.
		elastic.SetHealthcheck(false),

		elastic.SetRetrier(elastic.NewBackoffRetrier(elastic.NewExponentialBackoff(128*time.Millisecond, 513*time.Millisecond))),

		// critical to ensure decode of int64 won't lose precision
		elastic.SetDecoder(&elastic.NumberDecoder{}),
	}

	options = append(options, getLoggerOptions(config.LogLevel, logger)...)

	if httpClient != nil {
		options = append(options, elastic.SetHttpClient(httpClient))
	}

	client, err := elastic.NewClient(options...)
	if err != nil {
		return nil, err
	}

	// Re-enable the health check after client has successfully been created.
	client.Stop()
	err = elastic.SetHealthcheck(true)(client)
	if err != nil {
		return nil, err
	}
	client.Start()

	return &clientV7{esClient: client}, nil
}

func newSimpleClientV7(url string) (*clientV7, error) {
	retrier := elastic.NewBackoffRetrier(elastic.NewExponentialBackoff(128*time.Millisecond, 513*time.Millisecond))
	var client *elastic.Client
	var err error
	if client, err = elastic.NewClient(
		elastic.SetURL(url),
		elastic.SetRetrier(retrier),
	); err != nil {
		return nil, err
	}

	return &clientV7{esClient: client}, nil
}

func (c *clientV7) Search(ctx context.Context, p *SearchParameters) (*elastic.SearchResult, error) {
	searchService := c.esClient.Search(p.Index).
		Query(p.Query).
		From(p.From).
		SortBy(p.Sorter...)

	if p.PageSize != 0 {
		searchService.Size(p.PageSize)
	}

	if len(p.SearchAfter) != 0 {
		searchService.SearchAfter(p.SearchAfter...)
	}

	return searchService.Do(ctx)
}

func (c *clientV7) SearchWithDSL(ctx context.Context, index, query string) (*elastic.SearchResult, error) {
	searchResult, err := c.esClient.Search(index).Source(query).Do(ctx)
	return searchResult, err
}

func (c *clientV7) Scroll(ctx context.Context, scrollID string) (*elastic.SearchResult, ScrollService, error) {
	scrollService := elastic.NewScrollService(c.esClient)
	result, err := scrollService.ScrollId(scrollID).Do(ctx)
	return result, scrollService, err
}

func (c *clientV7) ScrollFirstPage(ctx context.Context, index, query string) (*elastic.SearchResult, ScrollService, error) {
	scrollService := elastic.NewScrollService(c.esClient)
	result, err := scrollService.Index(index).Body(query).Do(ctx)
	return result, scrollService, err
}

func (c *clientV7) Count(ctx context.Context, index, query string) (int64, error) {
	return c.esClient.Count(index).BodyString(query).Do(ctx)
}

func (c *clientV7) RunBulkProcessor(ctx context.Context, p *BulkProcessorParameters) (BulkProcessor, error) {
	esBulkProcessor, err := c.esClient.BulkProcessor().
		Name(p.Name).
		Workers(p.NumOfWorkers).
		BulkActions(p.BulkActions).
		BulkSize(p.BulkSize).
		FlushInterval(p.FlushInterval).
		Backoff(p.Backoff).
		Before(p.BeforeFunc).
		After(p.AfterFunc).
		Do(ctx)

	return newBulkProcessorV7(esBulkProcessor), err
}

func (c *clientV7) PutMapping(ctx context.Context, index string, root string, mapping map[string]string) (bool, error) {
	body := buildMappingBody(root, mapping)
	resp, err := c.esClient.PutMapping().Index(index).BodyJson(body).Do(ctx)
	if err != nil {
		return false, err
	}
	return resp.Acknowledged, err
}

func (c *clientV7) WaitForYellowStatus(ctx context.Context, index string) (string, error) {
	resp, err := c.esClient.ClusterHealth().Index(index).WaitForYellowStatus().Do(ctx)
	if err != nil {
		return "", err
	}
	return resp.Status, err
}

func (c *clientV7) GetMapping(ctx context.Context, index string) (map[string]string, error) {
	resp, err := c.esClient.GetMapping().Index(index).Do(ctx)
	if err != nil {
		return nil, err
	}
	return convertMappingBody(resp, index), err
}

func (c *clientV7) CreateIndex(ctx context.Context, index string) (bool, error) {
	resp, err := c.esClient.CreateIndex(index).Do(ctx)
	if err != nil {
		return false, err
	}
	return resp.Acknowledged, nil
}

func (c *clientV7) IsNotFoundError(err error) bool {
	return elastic.IsNotFound(err)
}

func (c *clientV7) CatIndices(ctx context.Context) (elastic.CatIndicesResponse, error) {
	return c.esClient.CatIndices().Do(ctx)
}

func (c *clientV7) Bulk() BulkService {
	return newBulkServiceV7(c.esClient.Bulk())
}

func (c *clientV7) IndexPutTemplate(ctx context.Context, templateName string, bodyString string) (bool, error) {
	resp, err := c.esClient.IndexPutTemplate(templateName).BodyString(bodyString).Do(ctx)
	if err != nil {
		return false, err
	}
	return resp.Acknowledged, nil
}

func (c *clientV7) IndexExists(ctx context.Context, indexName string) (bool, error) {
	return c.esClient.IndexExists(indexName).Do(ctx)
}

func (c *clientV7) DeleteIndex(ctx context.Context, indexName string) (bool, error) {
	resp, err := c.esClient.DeleteIndex(indexName).Do(ctx)
	if err != nil {
		return false, err
	}
	return resp.Acknowledged, nil
}

func (c *clientV7) IndexPutSettings(ctx context.Context, indexName string, bodyString string) (bool, error) {
	resp, err := c.esClient.IndexPutSettings(indexName).BodyString(bodyString).Do(ctx)
	if err != nil {
		return false, err
	}
	return resp.Acknowledged, nil
}

func (c *clientV7) IndexGetSettings(ctx context.Context, indexName string) (map[string]*elastic.IndicesGetSettingsResponse, error) {
	return c.esClient.IndexGetSettings(indexName).Do(ctx)
}

func getLoggerOptions(logLevel string, logger log.Logger) []elastic.ClientOptionFunc {
	switch {
	case strings.EqualFold(logLevel, "trace"):
		return []elastic.ClientOptionFunc{
			elastic.SetErrorLog(newErrorLogger(logger)),
			elastic.SetInfoLog(newInfoLogger(logger)),
			elastic.SetTraceLog(newInfoLogger(logger)),
		}
	case strings.EqualFold(logLevel, "info"):
		return []elastic.ClientOptionFunc{
			elastic.SetErrorLog(newErrorLogger(logger)),
			elastic.SetInfoLog(newInfoLogger(logger)),
		}
	case strings.EqualFold(logLevel, "error"), logLevel == "": // Default is to log errors only.
		return []elastic.ClientOptionFunc{
			elastic.SetErrorLog(newErrorLogger(logger)),
		}
	default:
		return nil
	}
}

func buildMappingBody(root string, mapping map[string]string) map[string]interface{} {
	properties := make(map[string]interface{}, len(mapping))
	for fieldName, fieldType := range mapping {
		properties[fieldName] = map[string]interface{}{
			"type": fieldType,
		}
	}

	body := make(map[string]interface{})
	if len(root) != 0 {
		body["properties"] = map[string]interface{}{
			root: map[string]interface{}{
				"properties": properties,
			},
		}
	} else {
		body["properties"] = properties
	}
	return body
}

func convertMappingBody(esMapping map[string]interface{}, indexName string) map[string]string {
	result := make(map[string]string)
	index, ok := esMapping[indexName]
	if !ok {
		return result
	}
	indexMap, ok := index.(map[string]interface{})
	if !ok {
		return result
	}
	mappings, ok := indexMap["mappings"]
	if !ok {
		return result
	}
	mappingsMap, ok := mappings.(map[string]interface{})
	if !ok {
		return result
	}
	properties, ok := mappingsMap["properties"]
	if !ok {
		return result
	}
	propMap, ok := properties.(map[string]interface{})
	if !ok {
		return result
	}

	for fieldName, fieldProp := range propMap {
		fieldPropMap, ok := fieldProp.(map[string]interface{})
		if !ok {
			continue
		}
		tYpe, ok := fieldPropMap["type"]
		if !ok {
			continue
		}
		typeStr, ok := tYpe.(string)
		if !ok {
			continue
		}
		result[fieldName] = typeStr
	}

	return result
}
