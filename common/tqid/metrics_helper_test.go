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

package tqid

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
)

func TestPerTaskQueueScope(t *testing.T) {
	ns := "my_ns"
	tq := UnsafeTaskQueueFamily("ns_id", "my_tq").TaskQueue(enums.TASK_QUEUE_TYPE_WORKFLOW)
	verifyTags(t,
		GetPerTaskQueueScope(metricstest.NewCaptureHandler(), ns, tq, false),
		map[string]string{"namespace": ns, "taskqueue": omitted, "task_type": "Workflow"},
	)

	verifyTags(t,
		GetPerTaskQueueScope(metricstest.NewCaptureHandler(), ns, tq, true),
		map[string]string{"namespace": ns, "taskqueue": tq.Name(), "task_type": "Workflow"},
	)
}

func TestPerTaskQueuePartitionScope_Normal(t *testing.T) {
	ns := "my_ns"
	p := UnsafeTaskQueueFamily("ns_id", "my_tq").TaskQueue(enums.TASK_QUEUE_TYPE_WORKFLOW).NormalPartition(1)
	verifyTags(t,
		GetPerTaskQueuePartitionScope(metricstest.NewCaptureHandler(), ns, p, true, false),
		map[string]string{"namespace": ns, "taskqueue": p.TaskQueue().Name(), "task_type": "Workflow", "partition": normal},
	)
	verifyTags(t,
		GetPerTaskQueuePartitionScope(metricstest.NewCaptureHandler(), ns, p, true, true),
		map[string]string{"namespace": ns, "taskqueue": p.TaskQueue().Name(), "task_type": "Workflow", "partition": "1"},
	)
}

func TestPerTaskQueuePartitionScope_Sticky(t *testing.T) {
	ns := "my_ns"
	p := UnsafeTaskQueueFamily("ns_id", "my_tq").TaskQueue(enums.TASK_QUEUE_TYPE_WORKFLOW).StickyPartition("abc")
	verifyTags(t,
		GetPerTaskQueuePartitionScope(metricstest.NewCaptureHandler(), ns, p, true, false),
		map[string]string{"namespace": ns, "taskqueue": p.TaskQueue().Name(), "task_type": "Workflow", "partition": sticky},
	)
	verifyTags(t,
		GetPerTaskQueuePartitionScope(metricstest.NewCaptureHandler(), ns, p, true, true),
		map[string]string{"namespace": ns, "taskqueue": p.TaskQueue().Name(), "task_type": "Workflow", "partition": sticky},
	)
}

func verifyTags(t *testing.T, handler metrics.Handler, expectedTags map[string]string) {
	a := assert.New(t)
	h, ok := handler.(*metricstest.CaptureHandler)
	a.True(ok)
	capture := h.StartCapture()
	h.Counter("MyMetric").Record(1)
	snap := capture.Snapshot()
	h.StopCapture(capture)
	a.Equal(1, len(snap))
	a.Equal(1, len(snap["MyMetric"]))
	a.Equal(expectedTags, snap["MyMetric"][0].Tags)
}
