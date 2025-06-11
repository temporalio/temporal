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

package scheduler

import (
	"fmt"
	"time"

	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/hsm"
	"go.uber.org/fx"
)

type (
	BackfillerTaskExecutorOptions struct {
		fx.In

		Config         *Config
		MetricsHandler metrics.Handler
		BaseLogger     log.Logger
		SpecProcessor  SpecProcessor
	}

	backfillerTaskExecutor struct {
		BackfillerTaskExecutorOptions
	}
)

func RegisterBackfillerExecutors(registry *hsm.Registry, options BackfillerTaskExecutorOptions) error {
	e := backfillerTaskExecutor{BackfillerTaskExecutorOptions: options}
	return hsm.RegisterTimerExecutor(registry, e.executeBackfillTask)
}

func (e backfillerTaskExecutor) executeBackfillTask(env hsm.Environment, node *hsm.Node, task BackfillTask) error {
	var result backfillProgressResult

	schedulerNode := node.Parent
	scheduler, err := loadScheduler(schedulerNode)
	if err != nil {
		return err
	}
	logger := newTaggedLogger(e.BaseLogger, scheduler)

	backfiller, err := e.loadBackfiller(node)
	if err != nil {
		return err
	}

	// If the buffer is already full, don't move the watermark at all, just back off
	// and retry.
	tweakables := e.Config.Tweakables(scheduler.Namespace)
	limit, err := e.allowedBufferedStarts(node, tweakables)
	if err != nil {
		return err
	}
	if limit <= 0 {
		result.LastProcessedTime = backfiller.GetLastProcessedTime().AsTime()
		result.NextInvocationTime = env.Now().Add(e.backoffDelay(backfiller))

		return hsm.MachineTransition(node, func(b Backfiller) (hsm.TransitionOutput, error) {
			return TransitionBackfillProgress.Apply(b, EventBackfillProgress{
				Node:                   node,
				backfillProgressResult: result,
			})
		})
	}

	// Process backfills, returning BufferedStarts.
	switch backfiller.RequestType() {
	case RequestTypeBackfill:
		result, err = e.processBackfill(env, scheduler, backfiller, limit)
	case RequestTypeTrigger:
		result, err = e.processTrigger(env, scheduler, backfiller)
	}
	if err != nil {
		logger.Error("Failed to process backfill", tag.Error(err))
		return err
	}

	// Enqueue new BufferedStarts on the Invoker, if we have any.
	if len(result.BufferedStarts) > 0 {
		err = scheduler.EnqueueBufferedStarts(schedulerNode, result.BufferedStarts)
		if err != nil {
			logger.Error("Failed to enqueue BufferedStarts", tag.Error(err))
			return err
		}
	}

	// If we're complete, we can delete this Backfiller node and return without any
	// more tasks.
	if result.Complete {
		logger.Debug(fmt.Sprintf("Backfill complete, deleting Backfiller %s", backfiller.GetBackfillId()))
		return schedulerNode.DeleteChild(node.Key)
	}

	// Otherwise, update progress and reschedule.
	return hsm.MachineTransition(node, func(b Backfiller) (hsm.TransitionOutput, error) {
		return TransitionBackfillProgress.Apply(b, EventBackfillProgress{
			Node:                   node,
			backfillProgressResult: result,
		})
	})
}

// processBackfill processes a Backfiller's BackfillRequest.
func (e backfillerTaskExecutor) processBackfill(
	env hsm.Environment,
	scheduler Scheduler,
	backfiller Backfiller,
	limit int,
) (result backfillProgressResult, err error) {
	request := backfiller.GetBackfillRequest()

	// Restore high watermark if we've already started processing the backfill.
	var startTime time.Time
	lastProcessed := backfiller.GetLastProcessedTime()
	if backfiller.GetAttempt() > 0 {
		startTime = lastProcessed.AsTime()
	} else {
		// On the first attempt, the start time is set slightly behind in order to make
		// the backfill start time inclusive.
		startTime = request.GetStartTime().AsTime().Add(-1 * time.Millisecond)
	}
	endTime := request.GetEndTime().AsTime()
	specResult, err := e.SpecProcessor.ProcessTimeRange(
		scheduler,
		startTime,
		endTime,
		request.GetOverlapPolicy(),
		backfiller.GetBackfillId(),
		true,
		&limit,
	)
	if err != nil {
		return
	}

	next := specResult.NextWakeupTime
	if next.IsZero() || next.After(endTime) {
		result.Complete = true
	} else {
		// More to backfill, indicating the buffer is full. Set the high watermark, and
		// apply a backoff time before attempting to continue filling.
		result.LastProcessedTime = specResult.LastActionTime

		// Apply retry policy.
		result.NextInvocationTime = env.Now().Add(e.backoffDelay(backfiller))
	}
	result.BufferedStarts = specResult.BufferedStarts

	return
}

// backoffDelay returns the amount of delay that should be added when retrying.
func (e backfillerTaskExecutor) backoffDelay(backfiller Backfiller) time.Duration {
	// GetAttempt is incremented early here as we increment the Backfiller's attempt
	// in the transition.
	return e.Config.RetryPolicy().ComputeNextDelay(0, int(backfiller.GetAttempt()+1), nil)
}

// processTrigger processes a Backfiller's TriggerImmediatelyRequest.
func (e backfillerTaskExecutor) processTrigger(env hsm.Environment, scheduler Scheduler, backfiller Backfiller) (result backfillProgressResult, err error) {
	request := backfiller.GetTriggerRequest()
	overlapPolicy := scheduler.resolveOverlapPolicy(request.GetOverlapPolicy())

	// Add a single manual start and mark the backfiller as complete. For batch
	// backfill requests, a deterministic start time is trivial as they follow the
	// schedule. For immediate trigger requests, the `LastProcessedTime` (set to
	// "now" when the Backfiller is spawned to handle a request) is used for start
	// time determinism.
	nowpb := backfiller.GetLastProcessedTime()
	now := nowpb.AsTime()
	requestID := generateRequestID(scheduler, backfiller.GetBackfillId(), now, now)
	result.BufferedStarts = []*schedulespb.BufferedStart{
		{
			NominalTime:   nowpb,
			ActualTime:    nowpb,
			DesiredTime:   nowpb,
			OverlapPolicy: overlapPolicy,
			Manual:        true,
			RequestId:     requestID,
		},
	}
	result.Complete = true

	return
}

// allowedBufferedStarts returns the number of BufferedStarts that the Backfiller should
// buffer, taking into account buffer limits and concurrent backfills.
func (e backfillerTaskExecutor) allowedBufferedStarts(backfillerNode *hsm.Node, tweakables Tweakables) (count int, err error) {
	// Load Invoker to get BufferedStarts.
	invokerNode, err := backfillerNode.Parent.Child([]hsm.Key{InvokerMachineKey})
	if err != nil {
		return
	}
	invoker, err := hsm.MachineData[Invoker](invokerNode)
	if err != nil {
		return
	}

	// Count the number of Backfiller nodes.
	var backfillerCount int
	backfillerCollection := BackfillerCollection(backfillerNode.Parent)
	for _, node := range backfillerCollection.List() {
		b, err := backfillerCollection.Data(node.Key.ID)
		if err != nil {
			return count, err
		}

		// Don't count trigger-immediately requests, as they only fire a single start.
		if b.RequestType() == RequestTypeBackfill {
			backfillerCount++
		}
	}

	// Prevents a division by 0.
	backfillerCount = max(1, backfillerCount)

	// Give half the buffer to Backfillers, distributed evenly.
	return max(0, ((tweakables.MaxBufferSize/2)/backfillerCount)-len(invoker.GetBufferedStarts())), nil
}

func (e backfillerTaskExecutor) loadBackfiller(node *hsm.Node) (Backfiller, error) {
	prevBackfiller, err := hsm.MachineData[Backfiller](node)
	if err != nil {
		return Backfiller{}, err
	}

	return Backfiller{
		BackfillerInternal: common.CloneProto(prevBackfiller.BackfillerInternal),
	}, nil
}
