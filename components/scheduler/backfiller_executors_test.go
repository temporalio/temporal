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

package scheduler_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/components/scheduler"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/hsmtest"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type backfillerExecutorsSuite struct {
	suite.Suite
	controller *gomock.Controller

	env           *fakeEnv
	registry      *hsm.Registry
	backend       *hsmtest.NodeBackend
	config        *scheduler.Config
	rootNode      *hsm.Node
	schedulerNode *hsm.Node
	invokerNode   *hsm.Node
}

func TestBackfillerExecutorsSuite(t *testing.T) {
	suite.Run(t, &backfillerExecutorsSuite{})
}

func (e *backfillerExecutorsSuite) SetupTest() {
	t := e.T()

	// Set up testing HSM environment.
	e.controller = gomock.NewController(t)
	e.env = newFakeEnv()
	e.registry = newRegistry(t)
	e.backend = &hsmtest.NodeBackend{}

	// Set up a scheduler tree.
	e.rootNode = newRoot(t, e.registry, e.backend)
	e.schedulerNode = newSchedulerTree(t, e.registry, e.rootNode, defaultSchedule(), nil)
	invokerNode, err := e.schedulerNode.Child([]hsm.Key{scheduler.InvokerMachineKey})
	require.NoError(t, err)
	e.invokerNode = invokerNode

	// Register the task executor.
	e.config = defaultConfig()
	require.NoError(t, scheduler.RegisterBackfillerExecutors(e.registry, scheduler.BackfillerTaskExecutorOptions{
		Config:         e.config,
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     log.NewNoopLogger(),
		SpecProcessor:  newTestSpecProcessor(e.controller),
	}))
}

func (e *backfillerExecutorsSuite) TearDownTest() {
	e.controller.Finish()
}

// getBackfiller returns a Backfiller in the scheduler tree, failing the test if
// none or more than one are present.
func (e *backfillerExecutorsSuite) getBackfiller() (scheduler.Backfiller, *hsm.Node) {
	t := e.T()

	var nodes []*hsm.Node
	err := e.rootNode.Walk(func(node *hsm.Node) error {
		if node.Key.Type == scheduler.BackfillerMachineType {
			nodes = append(nodes, node)
		}
		return nil
	})
	require.NoError(t, err)

	require.Equal(t, 1, len(nodes))
	backfillerNode := nodes[0]
	backfiller, err := hsm.MachineData[scheduler.Backfiller](backfillerNode)
	require.NoError(t, err)

	return backfiller, backfillerNode
}

func (e *backfillerExecutorsSuite) runBackfillTask() {
	err := e.registry.ExecuteTimerTask(e.env, e.env.node, scheduler.BackfillTask{})
	require.NoError(e.T(), err)
}

type backfillTestCase struct {
	InitialTriggerRequest     *schedulepb.TriggerImmediatelyRequest
	InitialBackfillRequest    *schedulepb.BackfillRequest
	ExpectedBufferedStarts    int
	ExpectedComplete          bool // asserts the Backfiller is deleted
	ExpectedLastProcessedTime time.Time
	ExpectedAttempt           int

	ValidateInvoker    func(t *testing.T, invoker scheduler.Invoker)
	ValidateBackfiller func(t *testing.T, backfiller scheduler.Backfiller)
}

// An immediately-triggered run should result in the machine being deleted after
// completion.
func (e *backfillerExecutorsSuite) TestBackfillTask_TriggerImmediate() {
	request := &schedulepb.TriggerImmediatelyRequest{
		OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
	}
	e.runTestCase(&backfillTestCase{
		InitialTriggerRequest:  request,
		ExpectedBufferedStarts: 1,
		ExpectedComplete:       true,
		ValidateInvoker: func(t *testing.T, invoker scheduler.Invoker) {
			start := invoker.GetBufferedStarts()[0]
			require.Equal(t, request.OverlapPolicy, start.OverlapPolicy)
			require.Equal(t, e.env.now, start.ActualTime.AsTime())
			require.True(t, start.Manual)
			require.Contains(t, start.RequestId, e.env.node.Key.ID)
		},
	})
}

// An immediately-triggered run will back off and retry if the buffer is full.
func (e *backfillerExecutorsSuite) TestBackfillTask_TriggerImmediateFullBuffer() {
	t := e.T()

	// Backfillers get half of the max buffer size, so fill (half the buffer -
	// expected starts).
	invoker, err := hsm.MachineData[scheduler.Invoker](e.invokerNode)
	require.NoError(t, err)
	for i := 0; i < scheduler.DefaultTweakables.MaxBufferSize; i++ {
		invoker.BufferedStarts = append(invoker.BufferedStarts, &schedulespb.BufferedStart{})
	}

	e.runTestCase(&backfillTestCase{
		InitialTriggerRequest:     &schedulepb.TriggerImmediatelyRequest{},
		ExpectedBufferedStarts:    1000,
		ExpectedComplete:          false,
		ExpectedLastProcessedTime: e.env.now,
		ExpectedAttempt:           1,
		ValidateBackfiller: func(t *testing.T, backfiller scheduler.Backfiller) {
			require.True(t, backfiller.NextInvocationTime.AsTime().After(e.env.now))
		},
	})
}

// A backfill request completes entirely should result in the machine being
// deleted after completion.
func (e *backfillerExecutorsSuite) TestBackfillTask_CompleteFill() {
	startTime := time.Now()
	endTime := startTime.Add(5 * defaultInterval)
	request := &schedulepb.BackfillRequest{
		StartTime:     timestamppb.New(startTime),
		EndTime:       timestamppb.New(endTime),
		OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
	}
	e.runTestCase(&backfillTestCase{
		InitialBackfillRequest: request,
		ExpectedBufferedStarts: 5,
		ExpectedComplete:       true,
		ValidateInvoker: func(t *testing.T, invoker scheduler.Invoker) {
			for _, start := range invoker.GetBufferedStarts() {
				require.Equal(t, request.OverlapPolicy, start.OverlapPolicy)
				startAt := start.GetActualTime().AsTime()
				require.True(t, startAt.After(startTime))
				require.True(t, startAt.Before(endTime))
				require.True(t, start.Manual)
				require.Contains(t, start.RequestId, e.env.node.Key.ID)
			}
		},
	})
}

// Backfill start and end times are inclusive, so a backfill scheduled for an
// instant that exactly matches a time in the calendar spec's sequence should result
// in a start.
func (e *backfillerExecutorsSuite) TestBackfillTask_InclusiveStartEnd() {
	t := e.T()

	// Set an identical start and end time, landing on the calendar spec's interval.
	backfillTime := time.Now().Truncate(defaultInterval)
	request := &schedulepb.BackfillRequest{
		StartTime: timestamppb.New(backfillTime),
		EndTime:   timestamppb.New(backfillTime),
	}
	e.runTestCase(&backfillTestCase{
		InitialBackfillRequest: request,
		ExpectedBufferedStarts: 1,
		ExpectedComplete:       true,
	})

	// Clear the Invoker's buffered starts.
	invoker, err := hsm.MachineData[scheduler.Invoker](e.invokerNode)
	require.NoError(t, err)
	invoker.BufferedStarts = nil

	// A hair off and the action won't fire.
	backfillTime = backfillTime.Add(1 * time.Millisecond)
	request = &schedulepb.BackfillRequest{
		StartTime: timestamppb.New(backfillTime),
		EndTime:   timestamppb.New(backfillTime),
	}
	e.runTestCase(&backfillTestCase{
		InitialBackfillRequest: request,
		ExpectedBufferedStarts: 0,
		ExpectedComplete:       true,
	})
}

// When the buffer's completely full, the high watermark shouldn't advance and no
// starts should be buffered.
func (e *backfillerExecutorsSuite) TestBackfillTask_BufferCompletelyFull() {
	t := e.T()

	// Fill buffer past max.
	invoker, err := hsm.MachineData[scheduler.Invoker](e.invokerNode)
	require.NoError(t, err)
	for i := 0; i < scheduler.DefaultTweakables.MaxBufferSize; i++ {
		invoker.BufferedStarts = append(invoker.BufferedStarts, &schedulespb.BufferedStart{})
	}

	startTime := e.env.now
	endTime := startTime.Add(5 * defaultInterval)
	request := &schedulepb.BackfillRequest{
		StartTime: timestamppb.New(startTime),
		EndTime:   timestamppb.New(endTime),
	}
	e.runTestCase(&backfillTestCase{
		InitialBackfillRequest:    request,
		ExpectedBufferedStarts:    1000,
		ExpectedComplete:          false,
		ExpectedAttempt:           1,
		ExpectedLastProcessedTime: startTime,
		ValidateBackfiller: func(t *testing.T, backfiller scheduler.Backfiller) {
			require.True(t, backfiller.NextInvocationTime.AsTime().After(startTime))
		},
	})
}

// When the buffer has capacity left, that capacity should be filled, with the
// remainder left for a retry.
func (e *backfillerExecutorsSuite) TestBackfillTask_PartialFill() {
	t := e.T()

	// Backfillers get half of the max buffer size, so fill (half the buffer -
	// expected starts).
	invoker, err := hsm.MachineData[scheduler.Invoker](e.invokerNode)
	require.NoError(t, err)
	for i := 0; i < (scheduler.DefaultTweakables.MaxBufferSize/2)-5; i++ {
		invoker.BufferedStarts = append(invoker.BufferedStarts, &schedulespb.BufferedStart{})
	}

	startTime := e.env.now
	endTime := startTime.Add(10 * defaultInterval) // Backfill attempt will cover half of this range.
	request := &schedulepb.BackfillRequest{
		StartTime: timestamppb.New(startTime),
		EndTime:   timestamppb.New(endTime),
	}

	// Expect a full buffer, and a high watermark in the middle of the backfill.
	e.runTestCase(&backfillTestCase{
		InitialBackfillRequest:    request,
		ExpectedBufferedStarts:    500,
		ExpectedComplete:          false,
		ExpectedAttempt:           1,
		ExpectedLastProcessedTime: startTime.Add(5 * defaultInterval).Truncate(defaultInterval),
		ValidateBackfiller: func(t *testing.T, backfiller scheduler.Backfiller) {
			require.True(t, backfiller.NextInvocationTime.AsTime().After(startTime))
		},
	})

	// Clear the Invoker's buffer. The remainder of the starts should buffer, and the
	// backfiller should be deleted.
	invoker.BufferedStarts = nil
	e.runBackfillTask()
	require.Equal(t, 5, len(invoker.GetBufferedStarts()))
	_, err = e.rootNode.Child([]hsm.Key{e.env.node.Key})
	require.True(t, errors.Is(err, hsm.ErrStateMachineNotFound))
}

func (e *backfillerExecutorsSuite) runTestCase(c *backfillTestCase) {
	t := e.T()
	schedulerSm, err := hsm.MachineData[scheduler.Scheduler](e.schedulerNode)
	require.NoError(t, err)

	// Exactly one type of request can be set per Backfiller.
	require.False(t, c.InitialBackfillRequest != nil && c.InitialTriggerRequest != nil)
	require.False(t, c.InitialBackfillRequest == nil && c.InitialTriggerRequest == nil)

	// Spawn backfiller.
	var output hsm.TransitionOutput
	if c.InitialTriggerRequest != nil {
		output, err = schedulerSm.RequestImmediate(e.env, e.schedulerNode, c.InitialTriggerRequest)
	} else {
		output, err = schedulerSm.RequestBackfill(e.env, e.schedulerNode, c.InitialBackfillRequest)
	}

	// Either type of request will spawn a Backfiller and schedule a backfill task.
	require.NoError(t, err)
	require.Equal(t, 1, len(output.Tasks))
	require.Equal(t, scheduler.TaskTypeBackfill, output.Tasks[0].Type())

	// Set backfiller node as under test and run the task.
	backfiller, backfillerNode := e.getBackfiller()
	key := backfillerNode.Key
	e.env.node = backfillerNode
	e.runBackfillTask()

	// Validate completion or partial progress.
	if c.ExpectedComplete {
		_, err := e.rootNode.Child([]hsm.Key{key})
		require.True(t, errors.Is(err, hsm.ErrStateMachineNotFound))
	} else {
		require.Equal(t, int64(c.ExpectedAttempt), backfiller.GetAttempt())
		require.Equal(t, c.ExpectedLastProcessedTime, backfiller.GetLastProcessedTime().AsTime())
	}

	// Validate BufferedStarts. More detailed validation must be done in the callbacks.
	invoker, err := hsm.MachineData[scheduler.Invoker](e.invokerNode)
	require.NoError(t, err)
	require.Equal(t, c.ExpectedBufferedStarts, len(invoker.GetBufferedStarts()))

	// Callbacks.
	if c.ValidateInvoker != nil {
		c.ValidateInvoker(t, invoker)
	}
	if c.ValidateBackfiller != nil {
		c.ValidateBackfiller(t, backfiller)
	}
}
