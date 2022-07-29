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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination scheduler_mock.go

package queues

import (
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/tasks"
)

type (
	// Scheduler is the component for scheduling and processing
	// task executables. Ack(), Nack() or Reschedule() will always
	// be called on all executables that have been successfully submited.
	// Reschedule() will only be called after the Scheduler has been stopped
	Scheduler interface {
		common.Daemon

		Submit(Executable) error
		TrySubmit(Executable) (bool, error)
	}

	SchedulerOptions struct {
		tasks.ParallelProcessorOptions
		tasks.InterleavedWeightedRoundRobinSchedulerOptions
	}

	schedulerImpl struct {
		priorityAssigner PriorityAssigner
		wRRScheduler     tasks.Scheduler
	}
)

func NewScheduler(
	priorityAssigner PriorityAssigner,
	options SchedulerOptions,
	metricsProvider metrics.MetricsHandler,
	logger log.Logger,
) *schedulerImpl {
	return &schedulerImpl{
		priorityAssigner: priorityAssigner,
		wRRScheduler: tasks.NewInterleavedWeightedRoundRobinScheduler(
			options.InterleavedWeightedRoundRobinSchedulerOptions,
			tasks.NewParallelProcessor(
				&options.ParallelProcessorOptions,
				metricsProvider,
				logger,
			),
			metricsProvider,
			logger,
		),
	}
}

func (s *schedulerImpl) Start() {
	s.wRRScheduler.Start()
}

func (s *schedulerImpl) Stop() {
	s.wRRScheduler.Stop()
}

func (s *schedulerImpl) Submit(
	executable Executable,
) error {
	if err := s.priorityAssigner.Assign(executable); err != nil {
		executable.Logger().Error("Failed to assign task executable priority", tag.Error(err))
		return err
	}

	s.wRRScheduler.Submit(executable)
	return nil
}

func (s *schedulerImpl) TrySubmit(
	executable Executable,
) (bool, error) {
	if err := s.priorityAssigner.Assign(executable); err != nil {
		executable.Logger().Error("Failed to assign task executable priority", tag.Error(err))
		return false, err
	}

	return s.wRRScheduler.TrySubmit(executable), nil
}
