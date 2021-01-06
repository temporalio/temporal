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
	"context"
	"time"

	"github.com/olivere/elastic/v7"
)

type (
	// clientV7 implements Client
	clientV7 struct {
		esClient *elastic.Client
	}
)

var _ Client = (*clientV7)(nil)

// newClientV7 create a ES client
func newClientV7(config *Config) (*clientV7, error) {
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

	if config.AWSRequestSigning.Enabled {
		httpClient, err := newAWSElasticsearchHTTPClient(config.AWSRequestSigning)
		if err != nil {
			return nil, err
		}
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

func (c *clientV7) PutMapping(ctx context.Context, index, root, key, valueType string) error {
	body := c.buildPutMappingBody(root, key, valueType)
	_, err := c.esClient.PutMapping().Index(index).BodyJson(body).Do(ctx)
	return err
}

func (c *clientV7) CreateIndex(ctx context.Context, index string) error {
	_, err := c.esClient.CreateIndex(index).Do(ctx)
	return err
}

func (c *clientV7) buildPutMappingBody(root, key, valueType string) map[string]interface{} {
	body := make(map[string]interface{})
	if len(root) != 0 {
		body["properties"] = map[string]interface{}{
			root: map[string]interface{}{
				"properties": map[string]interface{}{
					key: map[string]interface{}{
						"type": valueType,
					},
				},
			},
		}
	} else {
		body["properties"] = map[string]interface{}{
			key: map[string]interface{}{
				"type": valueType,
			},
		}
	}
	return body
}
