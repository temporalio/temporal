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

// Code generated by mockery v1.0.0. DO NOT EDIT.
package mocks

import (
	context "context"

	elastic "github.com/olivere/elastic"
	elasticsearch "github.com/temporalio/temporal/common/elasticsearch"

	mock "github.com/stretchr/testify/mock"
)

// Client is an autogenerated mock type for the Client type
type Client struct {
	mock.Mock
}

// Count provides a mock function with given fields: ctx, index, query
func (_m *Client) Count(ctx context.Context, index string, query string) (int64, error) {
	ret := _m.Called(ctx, index, query)

	var r0 int64
	if rf, ok := ret.Get(0).(func(context.Context, string, string) int64); ok {
		r0 = rf(ctx, index, query)
	} else {
		r0 = ret.Get(0).(int64)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string, string) error); ok {
		r1 = rf(ctx, index, query)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// CreateIndex provides a mock function with given fields: ctx, index
func (_m *Client) CreateIndex(ctx context.Context, index string) error {
	ret := _m.Called(ctx, index)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string) error); ok {
		r0 = rf(ctx, index)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// PutMapping provides a mock function with given fields: ctx, index, root, key, valueType
func (_m *Client) PutMapping(ctx context.Context, index string, root string, key string, valueType string) error {
	ret := _m.Called(ctx, index, root, key, valueType)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string, string, string, string) error); ok {
		r0 = rf(ctx, index, root, key, valueType)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// RunBulkProcessor provides a mock function with given fields: ctx, p
func (_m *Client) RunBulkProcessor(ctx context.Context, p *elasticsearch.BulkProcessorParameters) (*elastic.BulkProcessor, error) {
	ret := _m.Called(ctx, p)

	var r0 *elastic.BulkProcessor
	if rf, ok := ret.Get(0).(func(context.Context, *elasticsearch.BulkProcessorParameters) *elastic.BulkProcessor); ok {
		r0 = rf(ctx, p)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*elastic.BulkProcessor)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, *elasticsearch.BulkProcessorParameters) error); ok {
		r1 = rf(ctx, p)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Scroll provides a mock function with given fields: ctx, scrollID
func (_m *Client) Scroll(ctx context.Context, scrollID string) (*elastic.SearchResult, elasticsearch.ScrollService, error) {
	ret := _m.Called(ctx, scrollID)

	var r0 *elastic.SearchResult
	if rf, ok := ret.Get(0).(func(context.Context, string) *elastic.SearchResult); ok {
		r0 = rf(ctx, scrollID)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*elastic.SearchResult)
		}
	}

	var r1 elasticsearch.ScrollService
	if rf, ok := ret.Get(1).(func(context.Context, string) elasticsearch.ScrollService); ok {
		r1 = rf(ctx, scrollID)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(elasticsearch.ScrollService)
		}
	}

	var r2 error
	if rf, ok := ret.Get(2).(func(context.Context, string) error); ok {
		r2 = rf(ctx, scrollID)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// ScrollFirstPage provides a mock function with given fields: ctx, index, query
func (_m *Client) ScrollFirstPage(ctx context.Context, index string, query string) (*elastic.SearchResult, elasticsearch.ScrollService, error) {
	ret := _m.Called(ctx, index, query)

	var r0 *elastic.SearchResult
	if rf, ok := ret.Get(0).(func(context.Context, string, string) *elastic.SearchResult); ok {
		r0 = rf(ctx, index, query)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*elastic.SearchResult)
		}
	}

	var r1 elasticsearch.ScrollService
	if rf, ok := ret.Get(1).(func(context.Context, string, string) elasticsearch.ScrollService); ok {
		r1 = rf(ctx, index, query)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(elasticsearch.ScrollService)
		}
	}

	var r2 error
	if rf, ok := ret.Get(2).(func(context.Context, string, string) error); ok {
		r2 = rf(ctx, index, query)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// Search provides a mock function with given fields: ctx, p
func (_m *Client) Search(ctx context.Context, p *elasticsearch.SearchParameters) (*elastic.SearchResult, error) {
	ret := _m.Called(ctx, p)

	var r0 *elastic.SearchResult
	if rf, ok := ret.Get(0).(func(context.Context, *elasticsearch.SearchParameters) *elastic.SearchResult); ok {
		r0 = rf(ctx, p)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*elastic.SearchResult)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, *elasticsearch.SearchParameters) error); ok {
		r1 = rf(ctx, p)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// SearchWithDSL provides a mock function with given fields: ctx, index, query
func (_m *Client) SearchWithDSL(ctx context.Context, index string, query string) (*elastic.SearchResult, error) {
	ret := _m.Called(ctx, index, query)

	var r0 *elastic.SearchResult
	if rf, ok := ret.Get(0).(func(context.Context, string, string) *elastic.SearchResult); ok {
		r0 = rf(ctx, index, query)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*elastic.SearchResult)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string, string) error); ok {
		r1 = rf(ctx, index, query)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
