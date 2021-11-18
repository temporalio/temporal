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

//go:generate mockgen -copyright_file ../../../../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination client_mock.go

package client

import (
	"context"
	"time"

	"github.com/olivere/elastic/v7"
	enumspb "go.temporal.io/api/enums/v1"
)

const (
	docTypeV6                           = "_doc"
	versionTypeExternal                 = "external"
	minimumCloseIdleConnectionsInterval = 15 * time.Second
)

type (
	// Client is a wrapper around Elasticsearch client library.
	Client interface {
		Search(ctx context.Context, p *SearchParameters) (*elastic.SearchResult, error)
		Count(ctx context.Context, index string, query elastic.Query) (int64, error)
		RunBulkProcessor(ctx context.Context, p *BulkProcessorParameters) (BulkProcessor, error)

		OpenScroll(ctx context.Context, p *SearchParameters, keepAliveInterval string) (*elastic.SearchResult, error)
		Scroll(ctx context.Context, scrollID string, keepAliveInterval string) (*elastic.SearchResult, error)
		CloseScroll(ctx context.Context, id string) error

		// TODO (alex): move this to some admin client (and join with IntegrationTestsClient)
		PutMapping(ctx context.Context, index string, mapping map[string]enumspb.IndexedValueType) (bool, error)
		WaitForYellowStatus(ctx context.Context, index string) (string, error)
		GetMapping(ctx context.Context, index string) (map[string]string, error)
	}

	// TODO (alex): Combine ClientV7 with Client interface after ES v6 support removal.
	ClientV7 interface {
		Client
		IsPointInTimeSupported(ctx context.Context) bool
		OpenPointInTime(ctx context.Context, index string, keepAliveInterval string) (string, error)
		ClosePointInTime(ctx context.Context, id string) (bool, error)
	}

	CLIClient interface {
		Client
		Delete(ctx context.Context, indexName string, docID string, version int64) error
	}

	IntegrationTestsClient interface {
		CreateIndex(ctx context.Context, index string) (bool, error)
		IndexPutTemplate(ctx context.Context, templateName string, bodyString string) (bool, error)
		IndexExists(ctx context.Context, indexName string) (bool, error)
		DeleteIndex(ctx context.Context, indexName string) (bool, error)
		IndexPutSettings(ctx context.Context, indexName string, bodyString string) (bool, error)
		IndexGetSettings(ctx context.Context, indexName string) (map[string]*elastic.IndicesGetSettingsResponse, error)
	}

	// SearchParameters holds all required and optional parameters for executing a search.
	SearchParameters struct {
		Index    string
		Query    elastic.Query
		PageSize int
		Sorter   []elastic.Sorter

		SearchAfter []interface{}
		PointInTime *elastic.PointInTime
	}
)
