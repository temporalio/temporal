package queues

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
)

type (
	iteratorSuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller
	}
)

func TestIteratorSuite(t *testing.T) {
	s := new(iteratorSuite)
	suite.Run(t, s)
}

func (s *iteratorSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
}

func (s *iteratorSuite) TearDownSuite() {
	s.controller.Finish()
}

func (s *iteratorSuite) TestNext_IncreaseTaskKey() {
	r := NewRandomRange()

	taskKey := NewRandomKeyInRange(r)
	mockTask := tasks.NewMockTask(s.controller)
	mockTask.EXPECT().GetKey().Return(taskKey).Times(1)
	paginationFnProvider := func(paginationRange Range) collection.PaginationFn[tasks.Task] {
		s.Equal(r, paginationRange)
		return func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			return []tasks.Task{mockTask}, nil, nil
		}
	}

	iterator := NewIterator(paginationFnProvider, r)
	s.Equal(r, iterator.Range())

	s.True(iterator.HasNext())
	task, err := iterator.Next()
	s.NoError(err)
	s.Equal(mockTask, task)

	s.Equal(NewRange(taskKey.Next(), r.ExclusiveMax), iterator.Range())

	s.False(iterator.HasNext())
}

func (s *iteratorSuite) TestCanSplit() {
	r := NewRandomRange()

	iterator := NewIterator(nil, r)
	s.Equal(r, iterator.Range())

	s.True(iterator.CanSplit(r.InclusiveMin))
	s.True(iterator.CanSplit(r.ExclusiveMax))
	s.True(iterator.CanSplit(NewRandomKeyInRange(r)))

	s.False(iterator.CanSplit(tasks.NewKey(
		r.InclusiveMin.FireTime,
		r.InclusiveMin.TaskID-1,
	)))
	s.False(iterator.CanSplit(tasks.NewKey(
		r.ExclusiveMax.FireTime.Add(time.Nanosecond),
		r.ExclusiveMax.TaskID,
	)))
}

func (s *iteratorSuite) TestSplit() {
	r := NewRandomRange()
	paginationFnProvider := func(_ Range) collection.PaginationFn[tasks.Task] {
		return func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			return []tasks.Task{}, nil, nil
		}
	}

	iterator := NewIterator(paginationFnProvider, r)
	s.Equal(r, iterator.Range())

	splitKey := NewRandomKeyInRange(r)

	leftIterator, rightIterator := iterator.Split(splitKey)
	s.Equal(NewRange(r.InclusiveMin, splitKey), leftIterator.Range())
	s.Equal(NewRange(splitKey, r.ExclusiveMax), rightIterator.Range())
	s.False(leftIterator.HasNext())
	s.False(leftIterator.HasNext())
}

func (s *iteratorSuite) TestCanMerge() {
	r := NewRandomRange()
	iterator := NewIterator(nil, r)

	incomingIterator := NewIterator(nil, r)
	s.True(iterator.CanMerge(incomingIterator))

	incomingIterator = NewIterator(nil, NewRange(tasks.MinimumKey, r.InclusiveMin))
	s.True(iterator.CanMerge(incomingIterator))

	incomingIterator = NewIterator(nil, NewRange(r.ExclusiveMax, tasks.MaximumKey))
	s.True(iterator.CanMerge(incomingIterator))

	incomingIterator = NewIterator(nil, NewRange(tasks.MinimumKey, NewRandomKeyInRange(r)))
	s.True(iterator.CanMerge(incomingIterator))

	incomingIterator = NewIterator(nil, NewRange(NewRandomKeyInRange(r), tasks.MaximumKey))
	s.True(iterator.CanMerge(incomingIterator))

	incomingIterator = NewIterator(nil, NewRange(tasks.MinimumKey, tasks.MaximumKey))
	s.True(iterator.CanMerge(incomingIterator))

	incomingIterator = NewIterator(nil, NewRange(
		tasks.MinimumKey,
		tasks.NewKey(r.InclusiveMin.FireTime, r.InclusiveMin.TaskID-1),
	))
	s.False(iterator.CanMerge(incomingIterator))

	incomingIterator = NewIterator(nil, NewRange(
		tasks.NewKey(r.ExclusiveMax.FireTime, r.ExclusiveMax.TaskID+1),
		tasks.MaximumKey,
	))
	s.False(iterator.CanMerge(incomingIterator))
}

func (s *iteratorSuite) TestMerge() {
	r := NewRandomRange()

	numLoad := 0
	paginationFnProvider := func(_ Range) collection.PaginationFn[tasks.Task] {
		return func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			numLoad++
			return []tasks.Task{}, nil, nil
		}
	}

	iterator := NewIterator(paginationFnProvider, r)
	s.False(iterator.HasNext())

	incomingIterator := NewIterator(paginationFnProvider, r)
	mergedIterator := iterator.Merge(incomingIterator)
	s.Equal(r, mergedIterator.Range())
	s.False(mergedIterator.HasNext())

	incomingIterator = NewIterator(
		paginationFnProvider,
		NewRange(tasks.MinimumKey, r.InclusiveMin),
	)
	mergedIterator = iterator.Merge(incomingIterator)
	s.Equal(NewRange(tasks.MinimumKey, r.ExclusiveMax), mergedIterator.Range())
	s.False(mergedIterator.HasNext())

	incomingIterator = NewIterator(
		paginationFnProvider,
		NewRange(r.ExclusiveMax, tasks.MaximumKey),
	)
	mergedIterator = iterator.Merge(incomingIterator)
	s.Equal(NewRange(r.InclusiveMin, tasks.MaximumKey), mergedIterator.Range())
	s.False(mergedIterator.HasNext())

	incomingIterator = NewIterator(
		paginationFnProvider,
		NewRange(tasks.MinimumKey, NewRandomKeyInRange(r)),
	)
	mergedIterator = iterator.Merge(incomingIterator)
	s.Equal(NewRange(tasks.MinimumKey, r.ExclusiveMax), mergedIterator.Range())
	s.False(mergedIterator.HasNext())

	incomingIterator = NewIterator(
		paginationFnProvider,
		NewRange(NewRandomKeyInRange(r), tasks.MaximumKey),
	)
	mergedIterator = iterator.Merge(incomingIterator)
	s.Equal(NewRange(r.InclusiveMin, tasks.MaximumKey), mergedIterator.Range())
	s.False(mergedIterator.HasNext())

	incomingIterator = NewIterator(
		paginationFnProvider,
		NewRange(tasks.MinimumKey, tasks.MaximumKey),
	)
	mergedIterator = iterator.Merge(incomingIterator)
	s.Equal(NewRange(tasks.MinimumKey, tasks.MaximumKey), mergedIterator.Range())
	s.False(mergedIterator.HasNext())

	// test if Merge returns a new iterator
	s.Equal(7, numLoad)
}

func (s *iteratorSuite) TestRemaining() {
	r := NewRandomRange()
	r.InclusiveMin.FireTime = tasks.DefaultFireTime
	r.ExclusiveMax.FireTime = tasks.DefaultFireTime

	numLoad := 0
	taskKey := NewRandomKeyInRange(r)
	mockTask := tasks.NewMockTask(s.controller)
	mockTask.EXPECT().GetKey().Return(taskKey).Times(1)
	paginationFnProvider := func(paginationRange Range) collection.PaginationFn[tasks.Task] {
		return func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			numLoad++
			if paginationRange.ContainsKey(taskKey) {
				return []tasks.Task{mockTask}, nil, nil
			}
			return []tasks.Task{}, nil, nil
		}
	}

	iterator := NewIterator(paginationFnProvider, r)
	_, err := iterator.Next()
	s.NoError(err)
	s.False(iterator.HasNext())

	remaining := iterator.Remaining()
	s.Equal(iterator.Range(), remaining.Range())
	s.False(remaining.HasNext())

	// test if Remaining returns a new iterator
	s.Equal(2, numLoad)
}
