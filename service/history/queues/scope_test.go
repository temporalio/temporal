package queues

import (
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/predicates"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
)

type (
	scopeSuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller
	}
)

func TestScopeSuite(t *testing.T) {
	s := new(scopeSuite)
	suite.Run(t, s)
}

func (s *scopeSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
}

func (s *scopeSuite) TearDownSuite() {
	s.controller.Finish()
}

func (s *scopeSuite) TestContains() {
	r := NewRandomRange()
	namespaceIDs := []string{uuid.NewString(), uuid.NewString(), uuid.NewString()}
	predicate := tasks.NewNamespacePredicate(namespaceIDs)
	scope := NewScope(r, predicate)

	for _, namespaceID := range namespaceIDs {
		mockTask := tasks.NewMockTask(s.controller)
		mockTask.EXPECT().GetNamespaceID().Return(namespaceID).AnyTimes()

		mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(r)).Times(1)
		s.True(scope.Contains(mockTask))

		mockTask.EXPECT().GetKey().Return(tasks.NewKey(r.ExclusiveMax.FireTime, r.ExclusiveMax.TaskID+1)).Times(1)
		s.False(scope.Contains(mockTask))
	}

	mockTask := tasks.NewMockTask(s.controller)
	mockTask.EXPECT().GetNamespaceID().Return(uuid.NewString()).AnyTimes()
	mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(r)).MaxTimes(1)
	s.False(scope.Contains(mockTask))
}

func (s *scopeSuite) TestCanSplitByRange() {
	r := NewRandomRange()
	predicate := predicates.Universal[tasks.Task]()
	scope := NewScope(r, predicate)

	s.True(scope.CanSplitByRange(r.InclusiveMin))
	s.True(scope.CanSplitByRange(r.ExclusiveMax))
	s.True(scope.CanSplitByRange(NewRandomKeyInRange(r)))

	s.False(scope.CanSplitByRange(tasks.NewKey(
		r.InclusiveMin.FireTime,
		r.InclusiveMin.TaskID-1,
	)))
	s.False(scope.CanSplitByRange(tasks.NewKey(
		r.ExclusiveMax.FireTime.Add(time.Nanosecond),
		r.ExclusiveMax.TaskID,
	)))
}

func (s *scopeSuite) TestSplitByRange() {
	r := NewRandomRange()
	predicate := predicates.Universal[tasks.Task]()
	scope := NewScope(r, predicate)

	splitKey := NewRandomKeyInRange(r)

	leftScope, rightScope := scope.SplitByRange(splitKey)
	s.Equal(NewRange(r.InclusiveMin, splitKey), leftScope.Range)
	s.Equal(NewRange(splitKey, r.ExclusiveMax), rightScope.Range)
	s.Equal(predicate, leftScope.Predicate)
	s.Equal(predicate, rightScope.Predicate)
}

func (s *scopeSuite) TestSplitByPredicate_SamePredicateType() {
	r := NewRandomRange()
	namespaceIDs := []string{uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()}
	predicate := tasks.NewNamespacePredicate(namespaceIDs)
	scope := NewScope(r, predicate)

	splitNamespaceIDs := append(slices.Clone(namespaceIDs[:rand.Intn(len(namespaceIDs))]), uuid.NewString(), uuid.NewString())
	splitPredicate := tasks.NewNamespacePredicate(splitNamespaceIDs)
	passScope, failScope := scope.SplitByPredicate(splitPredicate)
	s.Equal(r, passScope.Range)
	s.Equal(r, failScope.Range)

	for _, namespaceID := range splitNamespaceIDs {
		mockTask := tasks.NewMockTask(s.controller)
		mockTask.EXPECT().GetNamespaceID().Return(namespaceID).AnyTimes()
		mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(r)).AnyTimes()

		if slices.Contains(namespaceIDs, namespaceID) {
			s.True(passScope.Contains(mockTask))
		} else {
			s.False(passScope.Contains(mockTask))
		}
		s.False(failScope.Contains(mockTask))
	}
	for _, namespaceID := range namespaceIDs {
		if slices.Contains(splitNamespaceIDs, namespaceID) {
			continue
		}

		mockTask := tasks.NewMockTask(s.controller)
		mockTask.EXPECT().GetNamespaceID().Return(namespaceID).AnyTimes()
		mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(r)).AnyTimes()

		s.False(passScope.Contains(mockTask))
		s.True(failScope.Contains(mockTask))
	}

	mockTask := tasks.NewMockTask(s.controller)
	mockTask.EXPECT().GetNamespaceID().Return(uuid.NewString()).AnyTimes()
	mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(r)).AnyTimes()
	s.False(passScope.Contains(mockTask))
	s.False(failScope.Contains(mockTask))
}

func (s *scopeSuite) TestSplitByPredicate_DifferentPredicateType() {
	r := NewRandomRange()
	namespaceIDs := []string{uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()}
	predicate := tasks.NewNamespacePredicate(namespaceIDs)
	scope := NewScope(r, predicate)

	splitTaskTypes := []enumsspb.TaskType{
		enumsspb.TaskType(rand.Intn(10)),
		enumsspb.TaskType(rand.Intn(10)),
		enumsspb.TaskType(rand.Intn(10)),
		enumsspb.TaskType(rand.Intn(10)),
	}
	splitPredicate := tasks.NewTypePredicate(splitTaskTypes)
	passScope, failScope := scope.SplitByPredicate(splitPredicate)
	s.Equal(r, passScope.Range)
	s.Equal(r, failScope.Range)

	for _, namespaceID := range namespaceIDs {
		mockTask := tasks.NewMockTask(s.controller)
		mockTask.EXPECT().GetNamespaceID().Return(namespaceID).AnyTimes()
		mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(r)).AnyTimes()

		for _, typeType := range splitTaskTypes {
			mockTask.EXPECT().GetType().Return(typeType).Times(2)

			s.True(passScope.Contains(mockTask))
			s.False(failScope.Contains(mockTask))
		}

		mockTask.EXPECT().GetType().Return(enumsspb.TaskType(rand.Intn(10) + 10)).Times(2)
		s.False(passScope.Contains(mockTask))
		s.True(failScope.Contains(mockTask))
	}

	mockTask := tasks.NewMockTask(s.controller)
	mockTask.EXPECT().GetNamespaceID().Return(uuid.NewString()).AnyTimes()
	mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(r)).AnyTimes()
	for _, typeType := range splitTaskTypes {
		mockTask.EXPECT().GetType().Return(typeType).MaxTimes(2)

		s.False(passScope.Contains(mockTask))
		s.False(failScope.Contains(mockTask))
	}

	mockTask.EXPECT().GetType().Return(enumsspb.TaskType(rand.Intn(10) + 10)).MaxTimes(2)
	s.False(passScope.Contains(mockTask))
	s.False(failScope.Contains(mockTask))
}

func (s *scopeSuite) TestCanMergeByRange() {
	r := NewRandomRange()
	namespaceIDs := []string{uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()}
	predicate := tasks.NewNamespacePredicate(namespaceIDs)
	scope := NewScope(r, predicate)

	testPredicates := []tasks.Predicate{
		predicate,
		tasks.NewNamespacePredicate(namespaceIDs),
		tasks.NewNamespacePredicate([]string{uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()}),
		tasks.NewTypePredicate([]enumsspb.TaskType{enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER}),
	}
	s.True(predicate.Equals(testPredicates[0]))
	s.True(predicate.Equals(testPredicates[1]))
	s.False(predicate.Equals(testPredicates[2]))
	s.False(predicate.Equals(testPredicates[3]))

	for _, mergePredicate := range testPredicates {
		canMerge := predicate.Equals(mergePredicate)

		incomingScope := NewScope(r, mergePredicate)
		s.Equal(canMerge, scope.CanMergeByRange(incomingScope))

		incomingScope = NewScope(NewRange(tasks.MinimumKey, r.InclusiveMin), mergePredicate)
		s.Equal(canMerge, scope.CanMergeByRange(incomingScope))

		incomingScope = NewScope(NewRange(r.ExclusiveMax, tasks.MaximumKey), mergePredicate)
		s.Equal(canMerge, scope.CanMergeByRange(incomingScope))

		incomingScope = NewScope(NewRange(tasks.MinimumKey, NewRandomKeyInRange(r)), mergePredicate)
		s.Equal(canMerge, scope.CanMergeByRange(incomingScope))

		incomingScope = NewScope(NewRange(NewRandomKeyInRange(r), tasks.MaximumKey), mergePredicate)
		s.Equal(canMerge, scope.CanMergeByRange(incomingScope))

		incomingScope = NewScope(NewRange(tasks.MinimumKey, tasks.MaximumKey), mergePredicate)
		s.Equal(canMerge, scope.CanMergeByRange(incomingScope))
	}

	incomingScope := NewScope(NewRange(
		tasks.MinimumKey,
		tasks.NewKey(r.InclusiveMin.FireTime, r.InclusiveMin.TaskID-1),
	), predicate)
	s.False(scope.CanMergeByRange(incomingScope))

	incomingScope = NewScope(NewRange(
		tasks.NewKey(r.ExclusiveMax.FireTime, r.ExclusiveMax.TaskID+1),
		tasks.MaximumKey,
	), predicate)
	s.False(scope.CanMergeByRange(incomingScope))
}

func (s *scopeSuite) TestMergeByRange() {
	r := NewRandomRange()
	predicate := predicates.Universal[tasks.Task]()
	scope := NewScope(r, predicate)

	mergeRange := r
	mergedScope := scope.MergeByRange(NewScope(mergeRange, predicate))
	s.Equal(predicate, mergedScope.Predicate)
	s.Equal(r, mergedScope.Range)

	mergeRange = NewRange(tasks.MinimumKey, r.InclusiveMin)
	mergedScope = scope.MergeByRange(NewScope(mergeRange, predicate))
	s.Equal(predicate, mergedScope.Predicate)
	s.Equal(NewRange(tasks.MinimumKey, r.ExclusiveMax), mergedScope.Range)

	mergeRange = NewRange(r.ExclusiveMax, tasks.MaximumKey)
	mergedScope = scope.MergeByRange(NewScope(mergeRange, predicate))
	s.Equal(predicate, mergedScope.Predicate)
	s.Equal(NewRange(r.InclusiveMin, tasks.MaximumKey), mergedScope.Range)

	mergeRange = NewRange(tasks.MinimumKey, NewRandomKeyInRange(r))
	mergedScope = scope.MergeByRange(NewScope(mergeRange, predicate))
	s.Equal(predicate, mergedScope.Predicate)
	s.Equal(NewRange(tasks.MinimumKey, r.ExclusiveMax), mergedScope.Range)

	mergeRange = NewRange(NewRandomKeyInRange(r), tasks.MaximumKey)
	mergedScope = scope.MergeByRange(NewScope(mergeRange, predicate))
	s.Equal(predicate, mergedScope.Predicate)
	s.Equal(NewRange(r.InclusiveMin, tasks.MaximumKey), mergedScope.Range)

	mergeRange = NewRange(tasks.MinimumKey, tasks.MaximumKey)
	mergedScope = scope.MergeByRange(NewScope(mergeRange, predicate))
	s.Equal(predicate, mergedScope.Predicate)
	s.Equal(mergeRange, mergedScope.Range)
}

func (s *scopeSuite) TestCanMergeByPredicate() {
	r := NewRandomRange()
	namespaceIDs := []string{uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()}
	predicate := tasks.NewNamespacePredicate(namespaceIDs)
	scope := NewScope(r, predicate)

	s.True(scope.CanMergeByPredicate(scope))
	s.True(scope.CanMergeByPredicate(NewScope(r, predicate)))
	s.True(scope.CanMergeByPredicate(NewScope(r, tasks.NewTypePredicate([]enumsspb.TaskType{}))))

	s.False(scope.CanMergeByPredicate(NewScope(NewRandomRange(), predicate)))
	s.False(scope.CanMergeByPredicate(NewScope(NewRandomRange(), predicates.Universal[tasks.Task]())))
}

func (s *scopeSuite) TestMergeByPredicate_SamePredicateType() {
	r := NewRandomRange()
	namespaceIDs := []string{uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()}
	predicate := tasks.NewNamespacePredicate(namespaceIDs)
	scope := NewScope(r, predicate)

	mergeNamespaceIDs := append(slices.Clone(namespaceIDs[:rand.Intn(len(namespaceIDs))]), uuid.NewString(), uuid.NewString())
	mergePredicate := tasks.NewNamespacePredicate(mergeNamespaceIDs)
	mergedScope := scope.MergeByPredicate(NewScope(r, mergePredicate))
	s.Equal(r, mergedScope.Range)

	for _, namespaceID := range namespaceIDs {
		mockTask := tasks.NewMockTask(s.controller)
		mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(r)).AnyTimes()
		mockTask.EXPECT().GetNamespaceID().Return(namespaceID).MaxTimes(2)

		s.True(mergedScope.Contains(mockTask))
	}
	for _, namespaceID := range mergeNamespaceIDs {
		mockTask := tasks.NewMockTask(s.controller)
		mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(r)).AnyTimes()
		mockTask.EXPECT().GetNamespaceID().Return(namespaceID).MaxTimes(2)

		s.True(mergedScope.Contains(mockTask))
	}

	mockTask := tasks.NewMockTask(s.controller)
	mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(r)).AnyTimes()
	mockTask.EXPECT().GetNamespaceID().Return(uuid.NewString()).AnyTimes()
	s.False(mergedScope.Contains(mockTask))
}

func (s *scopeSuite) TestMergeByPredicate_DifferentPredicateType() {
	r := NewRandomRange()
	namespaceIDs := []string{uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()}
	predicate := tasks.NewNamespacePredicate(namespaceIDs)
	scope := NewScope(r, predicate)

	mergeTaskTypes := []enumsspb.TaskType{
		enumsspb.TaskType(rand.Intn(10)),
		enumsspb.TaskType(rand.Intn(10)),
		enumsspb.TaskType(rand.Intn(10)),
		enumsspb.TaskType(rand.Intn(10)),
	}
	mergePredicate := tasks.NewTypePredicate(mergeTaskTypes)
	mergedScope := scope.MergeByPredicate(NewScope(r, mergePredicate))
	s.Equal(r, mergedScope.Range)

	for _, namespaceID := range namespaceIDs {
		mockTask := tasks.NewMockTask(s.controller)
		mockTask.EXPECT().GetNamespaceID().Return(namespaceID).AnyTimes()
		mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(r)).AnyTimes()

		for _, typeType := range mergeTaskTypes {
			mockTask.EXPECT().GetType().Return(typeType).MaxTimes(1)
			s.True(mergedScope.Contains(mockTask))
		}

		mockTask.EXPECT().GetType().Return(enumsspb.TaskType(rand.Intn(10) + 10)).MaxTimes(1)
		s.True(mergedScope.Contains(mockTask))
	}

	mockTask := tasks.NewMockTask(s.controller)
	mockTask.EXPECT().GetNamespaceID().Return(uuid.NewString()).AnyTimes()
	mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(r)).AnyTimes()
	for _, typeType := range mergeTaskTypes {
		mockTask.EXPECT().GetType().Return(typeType).MaxTimes(1)

		s.True(mergedScope.Contains(mockTask))
	}

	mockTask.EXPECT().GetType().Return(enumsspb.TaskType(rand.Intn(10) + 10)).Times(1)
	s.False(mergedScope.Contains(mockTask))
}
