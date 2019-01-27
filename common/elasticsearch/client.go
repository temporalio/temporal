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
	"github.com/olivere/elastic"
	"time"
)

type (
	// Client is a wrapper around ElasticSearch client library.
	// It simplifies the interface and enables mocking. We intentionally let implementation details of the elastic library
	// bleed through, as the main purpose is testability not abstraction.
	Client interface {
		Search(ctx context.Context, p *SearchParameters) (*elastic.SearchResult, error)
		RunBulkProcessor(ctx context.Context, p *BulkProcessorParameters) (*elastic.BulkProcessor, error)
	}

	// SearchParameters holds all required and optional parameters for executing a search
	SearchParameters struct {
		Index    string
		Query    elastic.Query
		From     int
		PageSize int
		Sorter   []elastic.Sorter
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
)

var _ Client = (*elasticWrapper)(nil)

// NewClient create a ES client
func NewClient(config *Config) (Client, error) {
	client, err := elastic.NewClient(
		elastic.SetURL(config.URL.String()),
		elastic.SetRetrier(elastic.NewBackoffRetrier(elastic.NewExponentialBackoff(128*time.Millisecond, 513*time.Millisecond))),
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

	return searchService.Do(ctx)
}

func (c *elasticWrapper) RunBulkProcessor(ctx context.Context, p *BulkProcessorParameters) (*elastic.BulkProcessor, error) {
	return c.client.BulkProcessor().
		Name(p.Name).
		Workers(p.NumOfWorkers).
		BulkActions(p.BulkActions).
		BulkSize(p.BulkSize).
		FlushInterval(p.FlushInterval).
		Backoff(p.Backoff).
		After(p.AfterFunc).
		Do(ctx)
}
