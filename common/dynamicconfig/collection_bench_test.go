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

package dynamicconfig_test

import (
	"testing"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
)

func BenchmarkCollection(b *testing.B) {
	// client with just one value
	client1 := dynamicconfig.StaticClient{
		dynamicconfig.MatchingMaxTaskBatchSize.Key(): []dynamicconfig.ConstrainedValue{{Value: 12}},
	}
	cln1 := dynamicconfig.NewCollection(client1, log.NewNoopLogger())
	b.Run("global int", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N/2; i++ {
			size := dynamicconfig.MatchingThrottledLogRPS.Get(cln1)
			_ = size()
			size = dynamicconfig.MatchingThrottledLogRPS.Get(cln1)
			_ = size()
		}
	})
	b.Run("namespace int", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N/2; i++ {
			size := dynamicconfig.HistoryMaxPageSize.Get(cln1)
			_ = size("my-namespace")
			size = dynamicconfig.WorkflowExecutionMaxInFlightUpdates.Get(cln1)
			_ = size("my-namespace")
		}
	})
	b.Run("taskqueue int", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N/2; i++ {
			size := dynamicconfig.MatchingMaxTaskBatchSize.Get(cln1)
			_ = size("my-namespace", "my-task-queue", 1)
			size = dynamicconfig.MatchingGetTasksBatchSize.Get(cln1)
			_ = size("my-namespace", "my-task-queue", 1)
		}
	})

	// client with more constrained values
	client2 := dynamicconfig.StaticClient{
		dynamicconfig.MatchingMaxTaskBatchSize.Key(): []dynamicconfig.ConstrainedValue{
			{
				Constraints: dynamicconfig.Constraints{
					TaskQueueName: "other-tq",
				},
				Value: 18,
			},
			{
				Constraints: dynamicconfig.Constraints{
					Namespace: "other-ns",
				},
				Value: 15,
			},
		},
	}
	cln2 := dynamicconfig.NewCollection(client2, log.NewNoopLogger())
	b.Run("single default", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N/4; i++ {
			size := dynamicconfig.MatchingMaxTaskBatchSize.Get(cln2)
			_ = size("my-namespace", "my-task-queue", 1)
			size = dynamicconfig.MatchingMaxTaskBatchSize.Get(cln2)
			_ = size("my-namespace", "other-tq", 1)
			size = dynamicconfig.MatchingMaxTaskBatchSize.Get(cln2)
			_ = size("other-ns", "my-task-queue", 1)
			size = dynamicconfig.MatchingMaxTaskBatchSize.Get(cln2)
			_ = size("other-ns", "other-tq", 1)
		}
	})
	b.Run("structured default", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N/4; i++ {
			size := dynamicconfig.MatchingNumTaskqueueWritePartitions.Get(cln2)
			_ = size("my-namespace", "my-task-queue", 1)
			size = dynamicconfig.MatchingNumTaskqueueWritePartitions.Get(cln2)
			_ = size("my-namespace", "other-tq", 1)
			size = dynamicconfig.MatchingNumTaskqueueWritePartitions.Get(cln2)
			_ = size("other-ns", "my-task-queue", 1)
			size = dynamicconfig.MatchingNumTaskqueueWritePartitions.Get(cln2)
			_ = size("other-ns", "other-tq", 1)
		}
	})
}
