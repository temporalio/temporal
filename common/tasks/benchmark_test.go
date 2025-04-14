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
	"math/rand"
	"sync"
	"testing"

	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/log"
)

type (
	noopScheduler struct{}
	noopTask      struct {
		*sync.WaitGroup
	}
)

var (
	_ Scheduler[*noopTask] = (*noopScheduler)(nil)
	_ Task                 = (*noopTask)(nil)
)

var (
	channelKeyToWeight = map[int]int{
		0: 5,
		1: 3,
		2: 2,
		3: 1,
	}
)

func BenchmarkInterleavedWeightedRoundRobinScheduler_Sequential(b *testing.B) {
	logger := log.NewTestLogger()

	scheduler := NewInterleavedWeightedRoundRobinScheduler(
		InterleavedWeightedRoundRobinSchedulerOptions[*noopTask, int]{
			TaskChannelKeyFn: func(nt *noopTask) int { return rand.Intn(4) },
			ChannelWeightFn:  func(key int) int { return channelKeyToWeight[key] },
		},
		Scheduler[*noopTask](&noopScheduler{}),
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
		InterleavedWeightedRoundRobinSchedulerOptions[*noopTask, int]{
			TaskChannelKeyFn: func(nt *noopTask) int { return rand.Intn(4) },
			ChannelWeightFn:  func(key int) int { return channelKeyToWeight[key] },
		},
		Scheduler[*noopTask](&noopScheduler{}),
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

func (n *noopScheduler) Start()                        {}
func (n *noopScheduler) Stop()                         {}
func (n *noopScheduler) Submit(task *noopTask)         { task.Ack() }
func (n *noopScheduler) TrySubmit(task *noopTask) bool { task.Ack(); return true }

func (n *noopTask) Execute() error                   { panic("implement me") }
func (n *noopTask) HandleErr(err error) error        { panic("implement me") }
func (n *noopTask) IsRetryableError(err error) bool  { panic("implement me") }
func (n *noopTask) RetryPolicy() backoff.RetryPolicy { panic("implement me") }
func (n *noopTask) Abort()                           { panic("implement me") }
func (n *noopTask) Cancel()                          { panic("implement me") }
func (n *noopTask) Ack()                             { n.Done() }
func (n *noopTask) Nack(err error)                   { panic("implement me") }
func (n *noopTask) Reschedule()                      { panic("implement me") }
func (n *noopTask) State() State                     { panic("implement me") }
