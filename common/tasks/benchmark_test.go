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

package tasks

import (
	"sync"
	"testing"

	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

type (
	noopProcessor struct{}
	noopTask      struct {
		*sync.WaitGroup
	}
)

var (
	_ Processor    = (*noopProcessor)(nil)
	_ PriorityTask = (*noopTask)(nil)
)

var (
	benchmarkPriorityToWeight = map[Priority]int{
		0: 5,
		1: 3,
		2: 2,
		3: 1,
	}
)

func BenchmarkInterleavedWeightedRoundRobinScheduler_Sequential(b *testing.B) {
	logger := log.NewTestLogger()

	scheduler := NewInterleavedWeightedRoundRobinScheduler(
		InterleavedWeightedRoundRobinSchedulerOptions{
			PriorityToWeight: benchmarkPriorityToWeight,
		},
		&noopProcessor{},
		metrics.NoopMetricsHandler,
		logger,
	)
	scheduler.Start()
	defer scheduler.Stop()

	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(b.N)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		scheduler.Submit(&noopTask{WaitGroup: waitGroup})
	}
	waitGroup.Wait()
}

func BenchmarkInterleavedWeightedRoundRobinScheduler_Parallel(b *testing.B) {
	logger := log.NewTestLogger()

	scheduler := NewInterleavedWeightedRoundRobinScheduler(
		InterleavedWeightedRoundRobinSchedulerOptions{
			PriorityToWeight: benchmarkPriorityToWeight,
		},
		&noopProcessor{},
		metrics.NoopMetricsHandler,
		logger,
	)
	scheduler.Start()
	defer scheduler.Stop()

	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(b.N)

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			scheduler.Submit(&noopTask{WaitGroup: waitGroup})
		}
	})
	waitGroup.Wait()
}

func (n *noopProcessor) Start()           {}
func (n *noopProcessor) Stop()            {}
func (n *noopProcessor) Submit(task Task) { task.Ack() }

func (n *noopTask) Execute() error                   { panic("implement me") }
func (n *noopTask) HandleErr(err error) error        { panic("implement me") }
func (n *noopTask) IsRetryableError(err error) bool  { panic("implement me") }
func (n *noopTask) RetryPolicy() backoff.RetryPolicy { panic("implement me") }
func (n *noopTask) Cancel()                          { panic("implement me") }
func (n *noopTask) Ack()                             { n.Done() }
func (n *noopTask) Nack(err error)                   { panic("implement me") }
func (n *noopTask) Reschedule()                      { panic("implement me") }
func (n *noopTask) State() State                     { panic("implement me") }
func (n *noopTask) GetPriority() Priority            { return 0 }
func (n *noopTask) SetPriority(i Priority)           { panic("implement me") }
