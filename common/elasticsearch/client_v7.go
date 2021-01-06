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
	return nil, nil
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
	panic("implement me")
}

func (c *clientV7) Scroll(ctx context.Context, scrollID string) (*elastic.SearchResult, ScrollService, error) {
	panic("implement me")
}

func (c *clientV7) ScrollFirstPage(ctx context.Context, index, query string) (*elastic.SearchResult, ScrollService, error) {
	panic("implement me")
}

func (c *clientV7) Count(ctx context.Context, index, query string) (int64, error) {
	panic("implement me")
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
	panic("implement me")
}

func (c *clientV7) CreateIndex(ctx context.Context, index string) error {
	panic("implement me")
}
