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
	"errors"
	"time"

	"github.com/olivere/elastic/v7"
)

type (
	bulkProcessorImpl struct {
		esBulkProcessor *elastic.BulkProcessor
	}
)

func newBulkProcessor(esBulkProcessor *elastic.BulkProcessor) *bulkProcessorImpl {
	if esBulkProcessor == nil {
		return nil
	}
	return &bulkProcessorImpl{
		esBulkProcessor: esBulkProcessor,
	}
}

func (p *bulkProcessorImpl) Stop() error {
	// Flush can block indefinitely if we can't reach ES. Call it with a timeout to avoid
	// blocking server shutdown. Default fx app shutdown timeout is 15s, so use 5s.
	errC := make(chan error)
	go func() {
		errF := p.esBulkProcessor.Flush()
		errS := p.esBulkProcessor.Stop()
		if errF != nil {
			errC <- errF
		} else {
			errC <- errS
		}
	}()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case err := <-errC:
		return err
	case <-timer.C:
		return errors.New("esBulkProcessor Flush/Stop timed out")
	}
}

func (p *bulkProcessorImpl) Add(request *BulkableRequest) {
	switch request.RequestType {
	case BulkableRequestTypeIndex:
		bulkIndexRequest := elastic.NewBulkIndexRequest().
			Index(request.Index).
			Id(request.ID).
			VersionType(versionTypeExternal).
			Version(request.Version).
			Doc(request.Doc)
		p.esBulkProcessor.Add(bulkIndexRequest)
	case BulkableRequestTypeDelete:
		bulkDeleteRequest := elastic.NewBulkDeleteRequest().
			Index(request.Index).
			Id(request.ID).
			VersionType(versionTypeExternal).
			Version(request.Version)
		p.esBulkProcessor.Add(bulkDeleteRequest)
	}
}
