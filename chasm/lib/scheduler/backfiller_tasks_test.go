package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/common/testing/testvars"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type backfillerTasksSuite struct {
	suite.Suite
	*require.Assertions
	protorequire.ProtoAssertions

	controller    *gomock.Controller
	nodeBackend   *chasm.MockNodeBackend
	node          *chasm.Node
	scheduler     *scheduler.Scheduler
	registry      *chasm.Registry
	timeSource    *clock.EventTimeSource
	logger        log.Logger
	specProcessor scheduler.SpecProcessor
}

func TestBackfillerTasksSuite(t *testing.T) {
	suite.Run(t, &backfillerTasksSuite{})
}

func (s *backfillerTasksSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.ProtoAssertions = protorequire.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.logger = testlogger.NewTestLogger(s.T(), testlogger.FailOnExpectedErrorOnly)

	// Use real spec processor for backfiller tests.
	mockMetrics := metrics.NewMockHandler(s.controller)
	mockMetrics.EXPECT().Counter(gomock.Any()).Return(metrics.NoopCounterMetricFunc).AnyTimes()
	mockMetrics.EXPECT().WithTags(gomock.Any()).Return(mockMetrics).AnyTimes()
	mockMetrics.EXPECT().Timer(gomock.Any()).Return(metrics.NoopTimerMetricFunc).AnyTimes()
	s.specProcessor = scheduler.NewSpecProcessor(
		defaultConfig(),
		mockMetrics,
		s.logger,
		legacyscheduler.NewSpecBuilder(),
	)

	s.registry = chasm.NewRegistry(s.logger)
	err := s.registry.Register(&chasm.CoreLibrary{})
	s.NoError(err)
	err = s.registry.Register(newTestLibrary(s.logger, s.specProcessor))
	s.NoError(err)

	s.timeSource = clock.NewEventTimeSource()
	now := time.Now()
	s.timeSource.Update(now)

	tv := testvars.New(s.T())
	s.nodeBackend = &chasm.MockNodeBackend{
		HandleNextTransitionCount: func() int64 { return 2 },
		HandleGetCurrentVersion:   func() int64 { return 1 },
		HandleGetWorkflowKey:      tv.Any().WorkflowKey,
		HandleIsWorkflow:          func() bool { return false },
		HandleCurrentVersionedTransition: func() *persistencespb.VersionedTransition {
			return &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 1,
				TransitionCount:          1,
			}
		},
	}

	s.node = chasm.NewEmptyTree(s.registry, s.timeSource, s.nodeBackend, chasm.DefaultPathEncoder, s.logger)
	ctx := s.newMutableContext()
	s.scheduler = scheduler.NewScheduler(ctx, namespace, namespaceID, scheduleID, defaultSchedule(), nil)
	s.node.SetRootComponent(s.scheduler)

	// Advance Generator's high water mark to 'now'.
	generator := s.scheduler.Generator.Get(ctx)
	generator.LastProcessedTime = timestamppb.New(now)

	_, err = s.node.CloseTransaction()
	s.NoError(err)
}

func (s *backfillerTasksSuite) newMutableContext() chasm.MutableContext {
	return chasm.NewMutableContext(context.Background(), s.node)
}

type backfillTestCase struct {
	InitialTriggerRequest     *schedulepb.TriggerImmediatelyRequest
	InitialBackfillRequest    *schedulepb.BackfillRequest
	ExpectedBufferedStarts    int
	ExpectedComplete          bool // asserts the Backfiller is deleted
	ExpectedLastProcessedTime time.Time
	ExpectedAttempt           int

	ValidateInvoker    func(invoker *scheduler.Invoker)
	ValidateBackfiller func(backfiller *scheduler.Backfiller)
}

// An immediately-triggered run should result in the machine being deleted after
// completion.
func (s *backfillerTasksSuite) TestBackfillTask_TriggerImmediate() {
	request := &schedulepb.TriggerImmediatelyRequest{
		OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
	}
	s.runTestCase(&backfillTestCase{
		InitialTriggerRequest:  request,
		ExpectedBufferedStarts: 1,
		ExpectedComplete:       true,
		ValidateInvoker: func(invoker *scheduler.Invoker) {
			start := invoker.GetBufferedStarts()[0]
			s.Equal(request.OverlapPolicy, start.OverlapPolicy)
			s.True(start.Manual)
		},
	})
}

// An immediately-triggered run will back off and retry if the buffer is full.
func (s *backfillerTasksSuite) TestBackfillTask_TriggerImmediateFullBuffer() {
	// Backfillers get half of the max buffer size, so fill (half the buffer -
	// expected starts).
	ctx := s.newMutableContext()
	invoker := s.scheduler.Invoker.Get(ctx)
	for range scheduler.DefaultTweakables.MaxBufferSize {
		invoker.BufferedStarts = append(invoker.BufferedStarts, &schedulespb.BufferedStart{})
	}

	now := s.timeSource.Now()
	s.runTestCase(&backfillTestCase{
		InitialTriggerRequest:     &schedulepb.TriggerImmediatelyRequest{},
		ExpectedBufferedStarts:    1000,
		ExpectedComplete:          false,
		ExpectedLastProcessedTime: now,
		ExpectedAttempt:           1,
	})
}

// A backfill request completes entirely should result in the machine being
// deleted after completion.
func (s *backfillerTasksSuite) TestBackfillTask_CompleteFill() {
	startTime := s.timeSource.Now()
	endTime := startTime.Add(5 * defaultInterval)
	request := &schedulepb.BackfillRequest{
		StartTime:     timestamppb.New(startTime),
		EndTime:       timestamppb.New(endTime),
		OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
	}
	s.runTestCase(&backfillTestCase{
		InitialBackfillRequest: request,
		ExpectedBufferedStarts: 5,
		ExpectedComplete:       true,
		ValidateInvoker: func(invoker *scheduler.Invoker) {
			for _, start := range invoker.GetBufferedStarts() {
				s.Equal(request.OverlapPolicy, start.OverlapPolicy)
				startAt := start.GetActualTime().AsTime()
				s.True(startAt.After(startTime))
				s.True(startAt.Before(endTime))
				s.True(start.Manual)
			}
		},
	})
}

// Backfill start and end times are inclusive, so a backfill scheduled for an
// instant that exactly matches a time in the calendar spec's sequence should result
// in a start.
func (s *backfillerTasksSuite) TestBackfillTask_InclusiveStartEnd() {
	// Set an identical start and end time, landing on the calendar spec's interval.
	backfillTime := s.timeSource.Now().Truncate(defaultInterval)
	request := &schedulepb.BackfillRequest{
		StartTime: timestamppb.New(backfillTime),
		EndTime:   timestamppb.New(backfillTime),
	}
	s.runTestCase(&backfillTestCase{
		InitialBackfillRequest: request,
		ExpectedBufferedStarts: 1,
		ExpectedComplete:       true,
	})

	// Clear the Invoker's buffered starts.
	ctx := s.newMutableContext()
	invoker := s.scheduler.Invoker.Get(ctx)
	invoker.BufferedStarts = nil

	// A hair off and the action won't fire.
	backfillTime = backfillTime.Add(1 * time.Millisecond)
	request = &schedulepb.BackfillRequest{
		StartTime: timestamppb.New(backfillTime),
		EndTime:   timestamppb.New(backfillTime),
	}
	s.runTestCase(&backfillTestCase{
		InitialBackfillRequest: request,
		ExpectedBufferedStarts: 0,
		ExpectedComplete:       true,
	})
}

// When the buffer's completely full, the high watermark shouldn't advance and no
// starts should be buffered.
func (s *backfillerTasksSuite) TestBackfillTask_BufferCompletelyFull() {
	// Fill buffer past max.
	ctx := s.newMutableContext()
	invoker := s.scheduler.Invoker.Get(ctx)
	for range scheduler.DefaultTweakables.MaxBufferSize {
		invoker.BufferedStarts = append(invoker.BufferedStarts, &schedulespb.BufferedStart{})
	}

	startTime := s.timeSource.Now()
	endTime := startTime.Add(5 * defaultInterval)
	request := &schedulepb.BackfillRequest{
		StartTime: timestamppb.New(startTime),
		EndTime:   timestamppb.New(endTime),
	}
	s.runTestCase(&backfillTestCase{
		InitialBackfillRequest:    request,
		ExpectedBufferedStarts:    1000,
		ExpectedComplete:          false,
		ExpectedAttempt:           1,
		ExpectedLastProcessedTime: startTime,
	})
}

// When the backfill range exceeds buffer capacity, partial filling should occur
// with the remainder left for a retry.
func (s *backfillerTasksSuite) TestBackfillTask_PartialFill() {
	// Use a large backfill range (1000 intervals) that exceeds the backfiller's
	// buffer limit (MaxBufferSize/2 = 500).
	startTime := s.timeSource.Now()
	endTime := startTime.Add(1000 * defaultInterval)
	request := &schedulepb.BackfillRequest{
		StartTime:     timestamppb.New(startTime),
		EndTime:       timestamppb.New(endTime),
		OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
	}

	ctx := s.newMutableContext()
	schedComponent, err := s.node.Component(ctx, chasm.ComponentRef{})
	s.NoError(err)
	sched := schedComponent.(*scheduler.Scheduler)
	backfiller := sched.NewRangeBackfiller(ctx, request)
	_, err = s.node.CloseTransaction()
	s.NoError(err)

	// Backfiller should have processed up to its limit (500), not the full 1000.
	s.False(backfiller.GetLastProcessedTime().AsTime().IsZero())
	s.Equal(int64(1), backfiller.GetAttempt())

	// Backfiller should still exist (not complete).
	ctx = s.newMutableContext()
	schedComponent, err = s.node.Component(ctx, chasm.ComponentRef{})
	s.NoError(err)
	sched = schedComponent.(*scheduler.Scheduler)
	_, ok := sched.Backfillers[backfiller.BackfillId].TryGet(ctx)
	s.True(ok)

	// Manually execute the second iteration since the scheduled continuation
	// task is in the future (after backoff delay).
	invoker := sched.Invoker.Get(ctx)
	invoker.BufferedStarts = nil // Clear to make room for next batch
	executor := scheduler.NewBackfillerTaskExecutor(scheduler.BackfillerTaskExecutorOptions{
		Config:         defaultConfig(),
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     s.logger,
		SpecProcessor:  s.specProcessor,
	})
	err = executor.Execute(ctx, backfiller, chasm.TaskAttributes{}, &schedulerpb.BackfillerTask{})
	s.NoError(err)
	_, err = s.node.CloseTransaction()
	s.NoError(err)

	// After second iteration, should have processed another batch.
	s.Equal(int64(2), backfiller.GetAttempt())
}

func (s *backfillerTasksSuite) runTestCase(c *backfillTestCase) {
	ctx := s.newMutableContext()
	schedComponent, err := s.node.Component(ctx, chasm.ComponentRef{})
	s.NoError(err)
	sched := schedComponent.(*scheduler.Scheduler)
	invoker := sched.Invoker.Get(ctx)

	// Exactly one type of request can be set per Backfiller.
	s.False(c.InitialBackfillRequest != nil && c.InitialTriggerRequest != nil)
	s.False(c.InitialBackfillRequest == nil && c.InitialTriggerRequest == nil)

	// Spawn backfiller.
	var backfiller *scheduler.Backfiller
	if c.InitialTriggerRequest != nil {
		backfiller = sched.NewImmediateBackfiller(ctx, c.InitialTriggerRequest)
	} else {
		backfiller = sched.NewRangeBackfiller(ctx, c.InitialBackfillRequest)
	}

	// Either type of request will spawn a Backfiller and schedule an immediate pure task.
	// The immediate task executes automatically during CloseTransaction().
	_, err = s.node.CloseTransaction()
	s.NoError(err)

	// Validate completion or partial progress.
	if c.ExpectedComplete {
		// Backfiller should no longer be present in the backfiller map.
		_, ok := sched.Backfillers[backfiller.BackfillId].TryGet(ctx)
		s.False(ok)
	} else {
		// TODO - check that a pure task to continue driving backfill exists here. Because
		// a pure task in the tree already has the physically-created status, closing the
		// transaction won't call our backend mock for AddTasks twice. Fix this when CHASM
		// offers unit testing hooks for task generation.

		s.Equal(int64(c.ExpectedAttempt), backfiller.GetAttempt())
		s.Equal(c.ExpectedLastProcessedTime.UTC(), backfiller.GetLastProcessedTime().AsTime())
	}

	// Validate BufferedStarts. More detailed validation must be done in the callbacks.
	s.Equal(c.ExpectedBufferedStarts, len(invoker.GetBufferedStarts()))

	// Validate RequestId -> WorkflowId mapping
	for _, start := range invoker.GetBufferedStarts() {
		s.Equal(start.WorkflowId, invoker.WorkflowID(start.RequestId))
	}

	// Callbacks.
	if c.ValidateInvoker != nil {
		c.ValidateInvoker(invoker)
	}
	if c.ValidateBackfiller != nil {
		c.ValidateBackfiller(backfiller)
	}
}
