package queues

import (
	"time"

	"github.com/google/uuid"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/predicates"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
)

// Every task generated here has a fire time inside the single second starting at
// expBase, so every read reports the same truncated watermark. Only the "did this read
// leave work behind" signal can tell the scenarios below apart.
var (
	expBase   = time.Unix(1000, 0).UTC()
	expSecEnd = expBase.Add(time.Second)
)

type expSliceSpec struct {
	scope Scope
	// pageSize tasks are returned per page. unbounded keeps handing out a pagination
	// token so the iterator never exhausts, modelling a backlog far larger than a batch.
	pageSize  int
	unbounded bool
}

func (s *readerSuite) newExpReader(specs []expSliceSpec) *ReaderImpl {
	paginationFnProvider := func(r Range) collection.PaginationFn[tasks.Task] {
		spec := specs[0]
		for _, candidate := range specs {
			if candidate.scope.Range.ContainsKey(r.InclusiveMin) {
				spec = candidate
				break
			}
		}

		return func(_ []byte) ([]tasks.Task, []byte, error) {
			// Clamp generated fire times to the first second so they stay in one window
			// however far the slice range has grown through merging.
			maxFire := r.ExclusiveMax.FireTime
			if maxFire.After(expSecEnd) {
				maxFire = expSecEnd
			}
			span := maxFire.Sub(r.InclusiveMin.FireTime)

			result := make([]tasks.Task, 0, spec.pageSize)
			for i := 0; i != spec.pageSize; i++ {
				offset := time.Duration(int64(span) * int64(i) / int64(spec.pageSize+1))
				key := tasks.NewKey(r.InclusiveMin.FireTime.Add(offset), int64(i+1))
				if !r.ContainsKey(key) {
					continue
				}
				mockTask := tasks.NewMockTask(s.controller)
				mockTask.EXPECT().GetKey().Return(key).AnyTimes()
				mockTask.EXPECT().GetNamespaceID().Return(uuid.NewString()).AnyTimes()
				mockTask.EXPECT().GetWorkflowID().Return(uuid.NewString()).AnyTimes()
				mockTask.EXPECT().GetVisibilityTime().Return(key.FireTime).AnyTimes()
				result = append(result, mockTask)
			}

			var token []byte
			if spec.unbounded {
				token = []byte{1}
			}
			return result, token, nil
		}
	}

	slices := make([]Slice, 0, len(specs))
	for _, spec := range specs {
		slices = append(slices, NewSlice(paginationFnProvider, s.executableFactory, s.monitor,
			spec.scope, GrouperNamespaceID{}, noPredicateSizeLimit, defaultMaxPendingKeys, metrics.NoopMetricsHandler))
	}

	s.mockScheduler.EXPECT().TrySubmit(gomock.Any()).Return(true).AnyTimes()
	s.mockRescheduler.EXPECT().Len().Return(0).AnyTimes()

	return NewReader(
		DefaultReaderId,
		slices,
		&ReaderOptions{
			BatchSize:            dynamicconfig.GetIntPropertyFn(10),
			MaxPendingTasksCount: dynamicconfig.GetIntPropertyFn(1000000),
			PollBackoffInterval:  dynamicconfig.GetDurationPropertyFn(time.Millisecond),
			MaxPredicateSize:     dynamicconfig.GetIntPropertyFn(10),
		},
		s.mockScheduler,
		s.mockRescheduler,
		clock.NewRealTimeSource(),
		NewReaderPriorityRateLimiter(func() float64 { return 1000000 }, 1),
		s.monitor,
		NoopReaderCompletionFn,
		s.logger,
		s.metricsHandler,
	)
}

func expScope(from, to time.Duration) Scope {
	return NewScope(NewRange(
		tasks.NewKey(expBase.Add(from), 0),
		tasks.NewKey(expBase.Add(to), 0),
	), predicates.Universal[tasks.Task]())
}

func (s *readerSuite) expAlerted() bool {
	select {
	case <-s.monitor.AlertCh():
		return true
	default:
		return false
	}
}

// Yichao's false positive: a burst of small slices all covering one fire time second.
// Every read drains its slice, so nothing is blocked even though every read reports the
// same truncated watermark.
func (s *readerSuite) TestExperiment_ManySmallSlicesInOneSecond() {
	const sliceCount = 8
	specs := make([]expSliceSpec, 0, sliceCount)
	for i := 0; i != sliceCount; i++ {
		specs = append(specs, expSliceSpec{
			scope:    expScope(time.Duration(i)*100*time.Millisecond, time.Duration(i+1)*100*time.Millisecond),
			pageSize: 3, // below batchSize, so each read drains its slice
		})
	}

	reader := s.newExpReader(specs)
	for i := 0; i != sliceCount*3; i++ {
		reader.loadAndSubmitTasks()
	}

	s.False(s.expAlerted(), "many small slices in one second must not look like a stuck reader")
}

// The condition we do want to catch: one second holding far more than a batch, so every
// read is cut short and work stays behind.
func (s *readerSuite) TestExperiment_OneDenseSecond() {
	reader := s.newExpReader([]expSliceSpec{{
		scope:     expScope(0, time.Second),
		pageSize:  10,
		unbounded: true,
	}})

	for i := 0; i != s.monitor.options.ReaderStuckCriticalAttempts(); i++ {
		reader.loadAndSubmitTasks()
	}

	s.True(s.expAlerted(), "a dense second that keeps cutting reads short must alert")
}

// The scenario per-slice attribution loses: the reader cannot get past one dense second
// while processNewRange keeps merging new ranges in above it. Uses only the public
// reader/monitor surface so it runs unchanged against either design.
func (s *readerSuite) TestExperiment_StuckAcrossMerges() {
	dense := expSliceSpec{scope: expScope(0, time.Second), pageSize: 10, unbounded: true}
	reader := s.newExpReader([]expSliceSpec{dense})

	rounds := s.monitor.options.ReaderStuckCriticalAttempts() * 3
	for i := 0; i != rounds; i++ {
		reader.loadAndSubmitTasks()

		// What processNewRange does on a shard that keeps generating timers: mint a
		// range above the frontier and merge it into the reader.
		above := expScope(time.Duration(i+1)*time.Second, time.Duration(i+2)*time.Second)
		reader.MergeSlices(NewSlice(
			func(Range) collection.PaginationFn[tasks.Task] {
				return func(_ []byte) ([]tasks.Task, []byte, error) { return nil, nil, nil }
			},
			s.executableFactory, s.monitor, above, GrouperNamespaceID{},
			noPredicateSizeLimit, defaultMaxPendingKeys, metrics.NoopMetricsHandler))
	}

	s.True(s.expAlerted(), "a reader stuck on one second must still alert while new ranges keep arriving")
}
