// Copyright (c) 2019 Uber Technologies, Inc.
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

package checksum

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	commonproto "go.temporal.io/temporal-proto/common"

	"github.com/temporalio/temporal/common"
)

func TestCRC32OverThrift(t *testing.T) {
	// note: do not use a struct with map since
	// iteration order is not guaranteed in Go and
	// so, each call to thrift encode will result in
	// different set of serialized bytes
	obj := &commonproto.WorkflowExecutionInfo{
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: uuid.New(),
			RunId:      uuid.New(),
		},
		StartTime: &types.Int64Value{
			Value: time.Now().UnixNano(),
		},
		HistoryLength: 550,
	}

	parallism := 10
	loopCount := 100
	successCount := int64(0)

	startC := make(chan struct{})
	doneWG := sync.WaitGroup{}
	doneWG.Add(parallism)

	for i := 0; i < parallism; i++ {
		go func() {
			defer doneWG.Done()
			<-startC
			for count := 0; count < loopCount; count++ {
				csum, err := GenerateCRC32(obj, 1)
				if err != nil {
					return
				}
				if err := Verify(obj, csum); err != nil {
					return
				}
				atomic.AddInt64(&successCount, 1)
			}
		}()
	}

	close(startC)
	success := common.AwaitWaitGroup(&doneWG, time.Second)
	assert.True(t, success, "timed out waiting for goroutines to finish")
	assert.Equal(t, int64(parallism*loopCount), successCount)
}
