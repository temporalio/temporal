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

//go:generate mockgen -copyright_file ../../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination client_mock.go

package client

import (
	"context"

	"github.com/olivere/elastic/v7"
	enumspb "go.temporal.io/api/enums/v1"
)

const (
	docTypeV6           = "_doc"
	versionTypeExternal = "external"
)

type (
	// Client is a wrapper around ElasticSearch client library.
	// It simplifies the interface and enables mocking. We intentionally let implementation details of the elastic library
	// bleed through, as the main purpose is testability not abstraction.
	Client interface {
		Search(ctx context.Context, p *SearchParameters) (*elastic.SearchResult, error)
		SearchWithDSL(ctx context.Context, index, query string) (*elastic.SearchResult, error)
		Count(ctx context.Context, index, query string) (int64, error)
		RunBulkProcessor(ctx context.Context, p *BulkProcessorParameters) (BulkProcessor, error)

		// TODO (alex): move this to some admin client (and join with IntegrationTestsClient)
		PutMapping(ctx context.Context, index string, mapping map[string]enumspb.IndexedValueType) (bool, error)
		WaitForYellowStatus(ctx context.Context, index string) (string, error)
		GetMapping(ctx context.Context, index string) (map[string]string, error)
	}

	// Combine ClientV7 with Client interface after ES v6 support removal.
	ClientV7 interface {
		OpenPointInTime(ctx context.Context, index string, keepAliveInterval string) (string, error)
		ClosePointInTime(ctx context.Context, id string) (bool, error)
		SearchWithDSLWithPIT(ctx context.Context, query string) (*elastic.SearchResult, error)
	}

	// Deprecated. Remove after ES v6 support removal.
	ClientV6 interface {
		// Deprecated. Remove after ES v6 support removal.
		Scroll(ctx context.Context, scrollID string) (*elastic.SearchResult, ScrollService, error)
		// Deprecated. Remove after ES v6 support removal.
		ScrollFirstPage(ctx context.Context, index, query string) (*elastic.SearchResult, ScrollService, error)
	}

	CLIClient interface {
		Client
		CatIndices(ctx context.Context) (elastic.CatIndicesResponse, error)
		Bulk() BulkService
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

	// ScrollService is a interface for elastic.ScrollService
	// Deprecated. Remove after ES v6 support removal.
	ScrollService interface {
		Clear(ctx context.Context) error
	}

	// SearchParameters holds all required and optional parameters for executing a search.
	SearchParameters struct {
		Index       string
		Query       elastic.Query
		PageSize    int
		Sorter      []elastic.Sorter
		SearchAfter []interface{}
	}
)
