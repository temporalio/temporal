// Copyright (c) 2017 Uber Technologies, Inc.
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

	"github.com/olivere/elastic"
)

type (
	// Client is a wrapper around ElasticSearch client library.
	// It simplifies the interface and enables mocking. We intentionally let implementation details of the elastic library
	// bleed through, as the main purpose is testability not abstraction.
	Client interface {
		Search(ctx context.Context, p *SearchParameters) (*elastic.SearchResult, error)
		SearchWithDSL(ctx context.Context, index, query string) (*elastic.SearchResult, error)
		Scroll(ctx context.Context, scrollID string) (*elastic.SearchResult, ScrollService, error)
		ScrollFirstPage(ctx context.Context, index, query string) (*elastic.SearchResult, ScrollService, error)
		Count(ctx context.Context, index, query string) (int64, error)
		RunBulkProcessor(ctx context.Context, p *BulkProcessorParameters) (*elastic.BulkProcessor, error)
		PutMapping(ctx context.Context, index, root, key, valueType string) error
		CreateIndex(ctx context.Context, index string) error
	}

	// ScrollService is a interface for elastic.ScrollService
	ScrollService interface {
		Clear(ctx context.Context) error
	}

	// SearchParameters holds all required and optional parameters for executing a search
	SearchParameters struct {
		Index       string
		Query       elastic.Query
		From        int
		PageSize    int
		Sorter      []elastic.Sorter
		SearchAfter []interface{}
	}

	// BulkProcessorParameters holds all required and optional parameters for executing bulk service
	BulkProcessorParameters struct {
		Name          string
		NumOfWorkers  int
		BulkActions   int
		BulkSize      int
		FlushInterval time.Duration
		Backoff       elastic.Backoff
		BeforeFunc    elastic.BulkBeforeFunc
		AfterFunc     elastic.BulkAfterFunc
	}

	// elasticWrapper implements Client
	elasticWrapper struct {
		client *elastic.Client
	}

	scrollServiceImpl struct {
		scrollService *elastic.ScrollService
	}
)

var _ Client = (*elasticWrapper)(nil)

// NewClient create a ES client
func NewClient(config *Config) (Client, error) {
	client, err := elastic.NewClient(
		elastic.SetURL(config.URL.String()),
		elastic.SetRetrier(elastic.NewBackoffRetrier(elastic.NewExponentialBackoff(128*time.Millisecond, 513*time.Millisecond))),
		elastic.SetDecoder(&elastic.NumberDecoder{}), // critical to ensure decode of int64 won't lose precise
	)
	if err != nil {
		return nil, err
	}
	return NewWrapperClient(client), nil
}

// NewWrapperClient returns a new implementation of Client
func NewWrapperClient(esClient *elastic.Client) Client {
	return &elasticWrapper{client: esClient}
}

func (c *elasticWrapper) Search(ctx context.Context, p *SearchParameters) (*elastic.SearchResult, error) {
	searchService := c.client.Search(p.Index).
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

func (c *elasticWrapper) SearchWithDSL(ctx context.Context, index, query string) (*elastic.SearchResult, error) {
	return c.client.Search(index).Source(query).Do(ctx)
}

func (c *elasticWrapper) Scroll(ctx context.Context, scrollID string) (
	*elastic.SearchResult, ScrollService, error) {

	scrollService := elastic.NewScrollService(c.client)
	result, err := scrollService.ScrollId(scrollID).Do(ctx)
	return result, &scrollServiceImpl{scrollService}, err
}

func (c *elasticWrapper) ScrollFirstPage(ctx context.Context, index, query string) (
	*elastic.SearchResult, ScrollService, error) {

	scrollService := elastic.NewScrollService(c.client)
	result, err := scrollService.Index(index).Body(query).Do(ctx)
	return result, &scrollServiceImpl{scrollService}, err
}

func (c *elasticWrapper) Count(ctx context.Context, index, query string) (int64, error) {
	return c.client.Count(index).BodyString(query).Do(ctx)
}

func (c *elasticWrapper) RunBulkProcessor(ctx context.Context, p *BulkProcessorParameters) (*elastic.BulkProcessor, error) {
	return c.client.BulkProcessor().
		Name(p.Name).
		Workers(p.NumOfWorkers).
		BulkActions(p.BulkActions).
		BulkSize(p.BulkSize).
		FlushInterval(p.FlushInterval).
		Backoff(p.Backoff).
		Before(p.BeforeFunc).
		After(p.AfterFunc).
		Do(ctx)
}

// root is for nested object like Attr property for search attributes.
func (c *elasticWrapper) PutMapping(ctx context.Context, index, root, key, valueType string) error {
	body := buildPutMappingBody(root, key, valueType)
	_, err := c.client.PutMapping().Index(index).Type("_doc").BodyJson(body).Do(ctx)
	return err
}

func (c *elasticWrapper) CreateIndex(ctx context.Context, index string) error {
	_, err := c.client.CreateIndex(index).Do(ctx)
	return err
}

func buildPutMappingBody(root, key, valueType string) map[string]interface{} {
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

func (s *scrollServiceImpl) Clear(ctx context.Context) error {
	return s.scrollService.Clear(ctx)
}
