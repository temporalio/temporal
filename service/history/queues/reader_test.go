package queues

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/predicates"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
)

type (
	readerSuite struct {
		suite.Suite
		*require.Assertions

		controller      *gomock.Controller
		mockScheduler   *MockScheduler
		mockRescheduler *MockRescheduler

		logger            log.Logger
		metricsHandler    metrics.Handler
		executableFactory ExecutableFactory
		monitor           *monitorImpl
	}
)

func TestReaderSuite(t *testing.T) {
	s := new(readerSuite)
	suite.Run(t, s)
}

func (s *readerSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockScheduler = NewMockScheduler(s.controller)
	s.mockRescheduler = NewMockRescheduler(s.controller)

	s.logger = log.NewTestLogger()
	s.metricsHandler = metrics.NoopMetricsHandler

	s.executableFactory = ExecutableFactoryFn(func(readerID int64, t tasks.Task) Executable {
		return NewExecutable(
			readerID,
			t,
			nil,
			nil,
			nil,
			NewNoopPriorityAssigner(),
			clock.NewRealTimeSource(),
			nil,
			nil,
			nil,
			metrics.NoopMetricsHandler,
			telemetry.NoopTracer,
		)
	})
	s.monitor = newMonitor(tasks.CategoryTypeScheduled, clock.NewRealTimeSource(), &MonitorOptions{
		PendingTasksCriticalCount:   dynamicconfig.GetIntPropertyFn(1000),
		ReaderStuckCriticalAttempts: dynamicconfig.GetIntPropertyFn(5),
		SliceCountCriticalThreshold: dynamicconfig.GetIntPropertyFn(50),
	})
}

func (s *readerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *readerSuite) TestStartLoadStop() {
	r := NewRandomRange()
	scopes := []Scope{NewScope(r, predicates.Universal[tasks.Task]())}

	paginationFnProvider := func(paginationRange Range) collection.PaginationFn[tasks.Task] {
		s.Equal(r, paginationRange)
		return func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			mockTask := tasks.NewMockTask(s.controller)
			mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(r)).AnyTimes()
			mockTask.EXPECT().GetNamespaceID().Return(uuid.New()).AnyTimes()
			return []tasks.Task{mockTask}, nil, nil
		}
	}

	doneCh := make(chan struct{})
	s.mockScheduler.EXPECT().TrySubmit(gomock.Any()).DoAndReturn(func(_ Executable) bool {
		close(doneCh)
		return true
	}).Times(1)
	s.mockRescheduler.EXPECT().Len().Return(0).AnyTimes()

	reader := s.newTestReader(scopes, paginationFnProvider, NoopReaderCompletionFn)
	mockTimeSource := clock.NewEventTimeSource()
	mockTimeSource.Update(scopes[0].Range.ExclusiveMax.FireTime)
	reader.timeSource = mockTimeSource

	reader.Start()
	<-doneCh
	reader.Stop()
}

func (s *readerSuite) TestScopes() {
	scopes := NewRandomScopes(10)

	reader := s.newTestReader(scopes, nil, NoopReaderCompletionFn)
	actualScopes := reader.Scopes()
	for idx, expectedScope := range scopes {
		s.True(expectedScope.Equals(actualScopes[idx]))
	}
}

func (s *readerSuite) TestSplitSlices() {
	scopes := NewRandomScopes(3)
	reader := s.newTestReader(scopes, nil, NoopReaderCompletionFn)

	splitter := func(s Slice) ([]Slice, bool) {
		// split head
		if scope := s.Scope(); !scope.Equals(scopes[0]) {
			return nil, false
		}

		// test remove slice
		return nil, true
	}
	reader.SplitSlices(splitter)
	s.Len(reader.Scopes(), 2)
	s.validateSlicesOrdered(reader)

	splitter = func(s Slice) ([]Slice, bool) {
		// split tail
		if scope := s.Scope(); !scope.Equals(scopes[2]) {
			return nil, false
		}

		left, right := s.SplitByRange(NewRandomKeyInRange(s.Scope().Range))
		_, right = right.SplitByRange(NewRandomKeyInRange(right.Scope().Range))

		return []Slice{left, right}, true
	}
	reader.SplitSlices(splitter)
	s.Len(reader.Scopes(), 3)
	s.validateSlicesOrdered(reader)

	splitter = func(s Slice) ([]Slice, bool) {
		left, right := s.SplitByRange(NewRandomKeyInRange(s.Scope().Range))

		// empty slices should be ignored
		left, empty := left.SplitByRange(left.Scope().Range.ExclusiveMax)
		return []Slice{left, empty, right}, true
	}
	reader.SplitSlices(splitter)
	s.Len(reader.Scopes(), 6)
	s.validateSlicesOrdered(reader)
}

func (s *readerSuite) TestMergeSlices() {
	scopes := NewRandomScopes(rand.Intn(10))
	reader := s.newTestReader(scopes, nil, NoopReaderCompletionFn)

	incomingScopes := NewRandomScopes(10)
	// manually set some scopes to be empty and verify they are ignored during merge
	incomingScopes[2].Predicate = predicates.Empty[tasks.Task]()
	incomingScopes[7].Predicate = predicates.Empty[tasks.Task]()

	incomingSlices := make([]Slice, 0, len(incomingScopes))
	for _, incomingScope := range incomingScopes {
		incomingSlices = append(incomingSlices, NewSlice(nil, s.executableFactory, s.monitor, incomingScope, GrouperNamespaceID{}, noPredicateSizeLimit))
	}

	reader.MergeSlices(incomingSlices...)

	mergedScopes := reader.Scopes()
	for idx, scope := range mergedScopes[:len(mergedScopes)-1] {
		s.False(scope.IsEmpty())
		nextScope := mergedScopes[idx+1]
		if scope.Range.ExclusiveMax.CompareTo(nextScope.Range.InclusiveMin) > 0 {
			panic(fmt.Sprintf(
				"Found overlapping scope in merged slices, left: %v, right: %v",
				scope,
				nextScope,
			))
		}
	}
}

func (s *readerSuite) TestAppendSlices() {
	totalScopes := 10
	scopes := NewRandomScopes(totalScopes)
	currentScopes := scopes[:totalScopes/2]
	reader := s.newTestReader(currentScopes, nil, NoopReaderCompletionFn)

	incomingScopes := scopes[totalScopes/2:]
	incomingScopes[2].Predicate = predicates.Empty[tasks.Task]()
	incomingSlices := make([]Slice, 0, len(incomingScopes))
	for _, incomingScope := range incomingScopes {
		incomingSlices = append(incomingSlices, NewSlice(nil, s.executableFactory, s.monitor, incomingScope, GrouperNamespaceID{}, noPredicateSizeLimit))
	}

	reader.AppendSlices(incomingSlices...)

	scopesAfterAppend := reader.Scopes()
	s.Len(scopesAfterAppend, totalScopes-1) // one empty scope should be ignored
	for idx, scope := range scopesAfterAppend[:len(scopesAfterAppend)-1] {
		s.False(scope.IsEmpty())
		nextScope := scopesAfterAppend[idx+1]
		if scope.Range.ExclusiveMax.CompareTo(nextScope.Range.InclusiveMin) > 0 {
			panic(fmt.Sprintf(
				"Found overlapping scope in appended slices, left: %v, right: %v",
				scope,
				nextScope,
			))
		}
	}
}

func (s *readerSuite) TestShrinkSlices() {
	numScopes := 10
	scopes := NewRandomScopes(numScopes)

	// manually set some scopes to be empty
	emptyIdx := map[int]struct{}{0: {}, 2: {}, 5: {}, 9: {}}
	for idx := range emptyIdx {
		scopes[idx].Range.InclusiveMin = scopes[idx].Range.ExclusiveMax
	}

	reader := s.newTestReader(scopes, nil, NoopReaderCompletionFn)
	completed := reader.ShrinkSlices()
	s.Equal(0, completed)

	actualScopes := reader.Scopes()
	s.Len(actualScopes, numScopes-len(emptyIdx))

	expectedScopes := make([]Scope, 0, numScopes-len(emptyIdx))
	for idx, scope := range scopes {
		if _, ok := emptyIdx[idx]; !ok {
			expectedScopes = append(expectedScopes, scope)
		}
	}

	for idx, expectedScope := range expectedScopes {
		s.True(expectedScope.Equals(actualScopes[idx]))
	}
}

func (s *readerSuite) TestNotify() {
	reader := s.newTestReader([]Scope{}, nil, NoopReaderCompletionFn)

	// pause will set the throttle timer, which notify is supposed to stop
	reader.Pause(time.Hour)

	reader.Lock()
	s.NotNil(reader.throttleTimer)
	reader.Unlock()

	reader.Notify()
	<-reader.notifyCh

	reader.Lock()
	s.Nil(reader.throttleTimer)
	reader.Unlock()
}

func (s *readerSuite) TestPause() {
	scopes := NewRandomScopes(1)

	paginationFnProvider := func(_ Range) collection.PaginationFn[tasks.Task] {
		return func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			mockTask := tasks.NewMockTask(s.controller)
			mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(scopes[0].Range)).AnyTimes()
			mockTask.EXPECT().GetNamespaceID().Return(uuid.New()).AnyTimes()
			return []tasks.Task{mockTask}, nil, nil
		}
	}

	reader := s.newTestReader(scopes, paginationFnProvider, NoopReaderCompletionFn)
	mockTimeSource := clock.NewEventTimeSource()
	mockTimeSource.Update(scopes[0].Range.ExclusiveMax.FireTime)
	reader.timeSource = mockTimeSource

	now := time.Now()
	delay := 100 * time.Millisecond
	reader.Pause(delay / 2)

	// check if existing throttle timer will be overwritten
	reader.Pause(delay)

	doneCh := make(chan struct{})
	s.mockScheduler.EXPECT().TrySubmit(gomock.Any()).DoAndReturn(func(_ Executable) bool {
		s.True(time.Now().After(now.Add(delay)))
		close(doneCh)
		return true
	}).Times(1)
	s.mockRescheduler.EXPECT().Len().Return(0).AnyTimes()

	reader.Start()
	<-doneCh
	reader.Stop()
}

func (s *readerSuite) TestLoadAndSubmitTasks_Throttled() {
	scopes := NewRandomScopes(1)

	completionFnCalled := false
	reader := s.newTestReader(scopes, nil, func(_ int64) { completionFnCalled = true })
	reader.Pause(100 * time.Millisecond)

	s.mockRescheduler.EXPECT().Len().Return(0).AnyTimes()

	// should be no-op
	reader.loadAndSubmitTasks()
	s.False(completionFnCalled)
}

func (s *readerSuite) TestLoadAndSubmitTasks_TooManyPendingTasks() {
	scopes := NewRandomScopes(1)

	completionFnCalled := false
	reader := s.newTestReader(scopes, nil, func(_ int64) { completionFnCalled = true })

	s.monitor.SetSlicePendingTaskCount(
		reader.slices.Front().Value.(Slice),
		reader.options.MaxPendingTasksCount(),
	)

	// should be no-op
	reader.loadAndSubmitTasks()
	s.False(completionFnCalled)
}

func (s *readerSuite) TestLoadAndSubmitTasks_MoreTasks() {
	scopes := NewRandomScopes(1)

	paginationFnProvider := func(_ Range) collection.PaginationFn[tasks.Task] {
		return func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			result := make([]tasks.Task, 0, 100)
			for i := 0; i != 100; i++ {
				mockTask := tasks.NewMockTask(s.controller)
				mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(scopes[0].Range)).AnyTimes()
				mockTask.EXPECT().GetNamespaceID().Return(uuid.New()).AnyTimes()
				result = append(result, mockTask)
			}

			return result, nil, nil
		}
	}

	completionFnCalled := false
	reader := s.newTestReader(scopes, paginationFnProvider, func(_ int64) { completionFnCalled = true })
	mockTimeSource := clock.NewEventTimeSource()
	mockTimeSource.Update(scopes[0].Range.ExclusiveMax.FireTime)
	reader.timeSource = mockTimeSource

	taskSubmitted := 0
	s.mockScheduler.EXPECT().TrySubmit(gomock.Any()).DoAndReturn(func(_ Executable) bool {
		taskSubmitted++
		return true
	}).AnyTimes()
	s.mockRescheduler.EXPECT().Len().Return(0).AnyTimes()

	reader.loadAndSubmitTasks()
	<-reader.notifyCh // should trigger next round of load
	s.Equal(reader.options.BatchSize(), taskSubmitted)
	s.True(scopes[0].Equals(reader.nextReadSlice.Value.(Slice).Scope()))
	s.False(completionFnCalled)
}

func (s *readerSuite) TestLoadAndSubmitTasks_NoMoreTasks_HasNextSlice() {
	scopes := NewRandomScopes(2)

	paginationFnProvider := func(_ Range) collection.PaginationFn[tasks.Task] {
		return func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			mockTask := tasks.NewMockTask(s.controller)
			mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(scopes[0].Range)).AnyTimes()
			mockTask.EXPECT().GetNamespaceID().Return(uuid.New()).AnyTimes()
			return []tasks.Task{mockTask}, nil, nil
		}
	}

	completionFnCalled := false
	reader := s.newTestReader(scopes, paginationFnProvider, func(_ int64) { completionFnCalled = true })
	mockTimeSource := clock.NewEventTimeSource()
	mockTimeSource.Update(scopes[0].Range.ExclusiveMax.FireTime)
	reader.timeSource = mockTimeSource

	taskSubmitted := 0
	s.mockScheduler.EXPECT().TrySubmit(gomock.Any()).DoAndReturn(func(_ Executable) bool {
		taskSubmitted++
		return true
	}).AnyTimes()
	s.mockRescheduler.EXPECT().Len().Return(0).AnyTimes()

	reader.loadAndSubmitTasks()
	<-reader.notifyCh // should trigger next round of load
	s.Equal(1, taskSubmitted)
	s.True(scopes[1].Equals(reader.nextReadSlice.Value.(Slice).Scope()))
	s.False(completionFnCalled)
}

func (s *readerSuite) TestLoadAndSubmitTasks_NoMoreTasks_NoNextSlice() {
	scopes := NewRandomScopes(1)

	paginationFnProvider := func(_ Range) collection.PaginationFn[tasks.Task] {
		return func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			mockTask := tasks.NewMockTask(s.controller)
			mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(scopes[0].Range)).AnyTimes()
			mockTask.EXPECT().GetNamespaceID().Return(uuid.New()).AnyTimes()
			return []tasks.Task{mockTask}, nil, nil
		}
	}

	completionFnCalled := false
	reader := s.newTestReader(scopes, paginationFnProvider, func(_ int64) { completionFnCalled = true })
	mockTimeSource := clock.NewEventTimeSource()
	mockTimeSource.Update(scopes[0].Range.ExclusiveMax.FireTime)
	reader.timeSource = mockTimeSource

	taskSubmitted := 0
	s.mockScheduler.EXPECT().TrySubmit(gomock.Any()).DoAndReturn(func(_ Executable) bool {
		taskSubmitted++
		return true
	}).AnyTimes()
	s.mockRescheduler.EXPECT().Len().Return(0).AnyTimes()

	reader.loadAndSubmitTasks()
	select {
	case <-reader.notifyCh:
		s.Fail("should not signal notify ch as there's no more task or slice")
	default:
		// should not trigger next round of load
	}
	s.Equal(1, taskSubmitted)
	s.Nil(reader.nextReadSlice)
	s.True(completionFnCalled)
}

func (s *readerSuite) TestSubmitTask() {
	r := NewRandomRange()
	scopes := []Scope{NewScope(r, predicates.Universal[tasks.Task]())}
	reader := s.newTestReader(scopes, nil, NoopReaderCompletionFn)

	mockExecutable := NewMockExecutable(s.controller)

	pastFireTime := reader.timeSource.Now().Add(-time.Minute)
	mockExecutable.EXPECT().GetKey().Return(tasks.NewKey(pastFireTime, rand.Int63())).Times(1)
	mockExecutable.EXPECT().SetScheduledTime(gomock.Any()).Times(1)
	s.mockScheduler.EXPECT().TrySubmit(gomock.Any()).Return(true).Times(1)
	reader.submit(mockExecutable)

	mockExecutable.EXPECT().GetKey().Return(tasks.NewKey(pastFireTime, rand.Int63())).Times(1)
	mockExecutable.EXPECT().SetScheduledTime(gomock.Any()).Times(1)
	s.mockScheduler.EXPECT().TrySubmit(gomock.Any()).Return(false).Times(1)
	mockExecutable.EXPECT().Reschedule().Times(1)
	reader.submit(mockExecutable)

	futureFireTime := reader.timeSource.Now().Add(time.Minute)
	mockExecutable.EXPECT().GetKey().Return(tasks.NewKey(futureFireTime, rand.Int63())).Times(1)
	s.mockRescheduler.EXPECT().Add(mockExecutable, futureFireTime.Add(persistence.ScheduledTaskMinPrecision)).Times(1)
	reader.submit(mockExecutable)
}

func (s *readerSuite) validateSlicesOrdered(
	reader Reader,
) {
	scopes := reader.Scopes()
	if len(scopes) <= 1 {
		return
	}

	for idx := range scopes[:len(scopes)-1] {
		s.True(scopes[idx].Range.ExclusiveMax.CompareTo(scopes[idx+1].Range.InclusiveMin) <= 0)
	}
}

func (s *readerSuite) newTestReader(
	scopes []Scope,
	paginationFnProvider PaginationFnProvider,
	completionFn ReaderCompletionFn,
) *ReaderImpl {
	slices := make([]Slice, 0, len(scopes))
	for _, scope := range scopes {
		slice := NewSlice(paginationFnProvider, s.executableFactory, s.monitor, scope, GrouperNamespaceID{}, noPredicateSizeLimit)
		slices = append(slices, slice)
	}

	return NewReader(
		DefaultReaderId,
		slices,
		&ReaderOptions{
			BatchSize:            dynamicconfig.GetIntPropertyFn(10),
			MaxPendingTasksCount: dynamicconfig.GetIntPropertyFn(100),
			PollBackoffInterval:  dynamicconfig.GetDurationPropertyFn(200 * time.Millisecond),
			MaxPredicateSize:     dynamicconfig.GetIntPropertyFn(10),
		},
		s.mockScheduler,
		s.mockRescheduler,
		clock.NewRealTimeSource(),
		NewReaderPriorityRateLimiter(func() float64 { return 20 }, 1),
		s.monitor,
		completionFn,
		s.logger,
		s.metricsHandler,
	)
}
