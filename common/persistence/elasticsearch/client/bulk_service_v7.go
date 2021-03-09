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

	"github.com/olivere/elastic/v7"
)

type (
	bulkServiceV7 struct {
		esBulkService *elastic.BulkService
	}
)

func newBulkServiceV7(esBulkService *elastic.BulkService) *bulkServiceV7 {
	return &bulkServiceV7{
		esBulkService: esBulkService,
	}
}

func (b *bulkServiceV7) Do(ctx context.Context) error {
	_, err := b.esBulkService.Do(ctx)
	return err
}

func (b *bulkServiceV7) NumberOfActions() int {
	return b.esBulkService.NumberOfActions()
}

func (b *bulkServiceV7) Add(request *BulkableRequest) {
	switch request.RequestType {
	case BulkableRequestTypeIndex:
		bulkDeleteRequest := elastic.NewBulkIndexRequest().
			Index(request.Index).
			Id(request.ID).
			VersionType(versionTypeExternal).
			Version(request.Version).
			Doc(request.Doc)
		b.esBulkService.Add(bulkDeleteRequest)
	case BulkableRequestTypeDelete:
		bulkDeleteRequest := elastic.NewBulkDeleteRequest().
			Index(request.Index).
			Id(request.ID).
			VersionType(versionTypeExternal).
			Version(request.Version)
		b.esBulkService.Add(bulkDeleteRequest)
	}
}
