package checksum

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	executionpb "go.temporal.io/temporal-proto/execution"

	"github.com/temporalio/temporal/common"
)

func TestCRC32OverThrift(t *testing.T) {
	// note: do not use a struct with map since
	// iteration order is not guaranteed in Go and
	// so, each call to thrift encode will result in
	// different set of serialized bytes
	obj := &executionpb.WorkflowExecutionInfo{
		Execution: &executionpb.WorkflowExecution{
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
