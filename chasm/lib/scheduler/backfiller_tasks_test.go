package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/metrics"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type backfillerTasksSuite struct {
	schedulerSuite
	executor *scheduler.BackfillerTaskExecutor
}

func TestBackfillerTasksSuite(t *testing.T) {
	suite.Run(t, &backfillerTasksSuite{})
}

func (s *backfillerTasksSuite) SetupTest() {
	s.schedulerSuite.SetupTest()
	s.executor = scheduler.NewBackfillerTaskExecutor(scheduler.BackfillerTaskExecutorOptions{
		Config:         defaultConfig(),
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     s.logger,
		SpecProcessor:  newTestSpecProcessor(s.controller),
	})
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

// When the buffer has capacity left, that capacity should be filled, with the
// remainder left for a retry.
func (s *backfillerTasksSuite) TestBackfillTask_PartialFill() {
	// Backfillers get half of the max buffer size, so fill (half the buffer -
	// expected starts).
	ctx := s.newMutableContext()
	invoker := s.scheduler.Invoker.Get(ctx)
	for range (scheduler.DefaultTweakables.MaxBufferSize / 2) - 5 {
		invoker.BufferedStarts = append(invoker.BufferedStarts, &schedulespb.BufferedStart{})
	}

	startTime := s.timeSource.Now()
	endTime := startTime.Add(10 * defaultInterval) // Backfill attempt will cover half of this range.
	request := &schedulepb.BackfillRequest{
		StartTime: timestamppb.New(startTime),
		EndTime:   timestamppb.New(endTime),
	}

	// Expect a full buffer, and a high watermark in the middle of the backfill.
	var backfiller *scheduler.Backfiller
	s.runTestCase(&backfillTestCase{
		InitialBackfillRequest:    request,
		ExpectedBufferedStarts:    500,
		ExpectedComplete:          false,
		ExpectedAttempt:           1,
		ExpectedLastProcessedTime: startTime.Add(5 * defaultInterval).Truncate(defaultInterval),
		ValidateBackfiller: func(b *scheduler.Backfiller) {
			backfiller = b
		},
	})

	// Clear the Invoker's buffer. The remainder of the starts should buffer, and the
	// backfiller should be deleted.
	invoker.BufferedStarts = nil
	err := s.executor.Execute(ctx, backfiller, chasm.TaskAttributes{}, &schedulerpb.BackfillerTask{})
	s.NoError(err)
	_, err = s.node.CloseTransaction()
	s.NoError(err)
	s.Equal(5, len(invoker.GetBufferedStarts()))

	// Verify the backfiller is deleted
	_, ok := s.scheduler.Backfillers[backfiller.BackfillId].TryGet(ctx)
	s.False(ok)
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
	_, err = s.node.CloseTransaction()
	s.NoError(err)

	// Run a backfill task.
	err = s.executor.Execute(ctx, backfiller, chasm.TaskAttributes{}, &schedulerpb.BackfillerTask{})
	s.NoError(err)
	_, err = s.node.CloseTransaction() // TODO - remove this when CHASM has unit testing hooks for task generation
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
