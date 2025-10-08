package tasks

import (
	"math/rand"
	"slices"
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/predicates"
	"go.uber.org/mock/gomock"
)

type (
	predicatesSuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller
	}
)

func TestPredicateSuite(t *testing.T) {
	s := new(predicatesSuite)
	suite.Run(t, s)
}

func (s *predicatesSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
}

func (s *predicatesSuite) TestNamespacePredicate_Test() {
	namespaceIDs := []string{uuid.New(), uuid.New()}

	p := NewNamespacePredicate(namespaceIDs)
	for _, id := range namespaceIDs {
		mockTask := NewMockTask(s.controller)
		mockTask.EXPECT().GetNamespaceID().Return(id).Times(1)
		s.True(p.Test(mockTask))
	}

	mockTask := NewMockTask(s.controller)
	mockTask.EXPECT().GetNamespaceID().Return(uuid.New()).Times(1)
	s.False(p.Test(mockTask))
}

func (s *predicatesSuite) TestNamespacePredicate_Equals() {
	namespaceIDs := []string{uuid.New(), uuid.New()}

	p := NewNamespacePredicate(namespaceIDs)

	s.True(p.Equals(p))
	s.True(p.Equals(NewNamespacePredicate(namespaceIDs)))
	rand.Shuffle(
		len(namespaceIDs),
		func(i, j int) {
			namespaceIDs[i], namespaceIDs[j] = namespaceIDs[j], namespaceIDs[i]
		},
	)
	s.True(p.Equals(NewNamespacePredicate(namespaceIDs)))

	s.False(p.Equals(NewNamespacePredicate([]string{uuid.New(), uuid.New()})))
	s.False(p.Equals(NewTypePredicate([]enumsspb.TaskType{enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER})))
	s.False(p.Equals(predicates.Universal[Task]()))
}

func (s *predicatesSuite) TestNamespacePredicate_Size() {
	namespaceIDs := []string{uuid.New(), uuid.New()}

	p := NewNamespacePredicate(namespaceIDs)

	// UUID length is 36 and 4 bytes accounted for proto overhead.
	s.Equal(76, p.Size())
}

func (s *predicatesSuite) TestTypePredicate_Test() {
	types := []enumsspb.TaskType{
		enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER,
		enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK,
		enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY,
		enumsspb.TASK_TYPE_VISIBILITY_CLOSE_EXECUTION,
	}

	p := NewTypePredicate(types)
	for _, taskType := range types {
		mockTask := NewMockTask(s.controller)
		mockTask.EXPECT().GetType().Return(taskType).Times(1)
		s.True(p.Test(mockTask))
	}

	for _, taskType := range enumsspb.TaskType_value {
		if slices.Index(types, enumsspb.TaskType(taskType)) != -1 {
			continue
		}

		mockTask := NewMockTask(s.controller)
		mockTask.EXPECT().GetType().Return(enumsspb.TaskType(taskType)).Times(1)
		s.False(p.Test(mockTask))
	}
}

func (s *predicatesSuite) TestTypePredicate_Equals() {
	types := []enumsspb.TaskType{
		enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER,
		enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK,
		enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY,
		enumsspb.TASK_TYPE_VISIBILITY_CLOSE_EXECUTION,
	}

	p := NewTypePredicate(types)

	s.True(p.Equals(p))
	s.True(p.Equals(NewTypePredicate(types)))
	rand.Shuffle(
		len(types),
		func(i, j int) {
			types[i], types[j] = types[j], types[i]
		},
	)
	s.True(p.Equals(NewTypePredicate(types)))

	s.False(p.Equals(NewTypePredicate([]enumsspb.TaskType{
		enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK,
		enumsspb.TASK_TYPE_VISIBILITY_CLOSE_EXECUTION,
		enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT,
		enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
	})))
	s.False(p.Equals(NewNamespacePredicate([]string{uuid.New(), uuid.New()})))
	s.False(p.Equals(predicates.Universal[Task]()))
}

func (s *predicatesSuite) TestTypePredicate_Size() {
	types := []enumsspb.TaskType{
		enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK,
		enumsspb.TASK_TYPE_VISIBILITY_CLOSE_EXECUTION,
	}
	p := NewTypePredicate(types)

	// enum size is 4 and 4 bytes accounted for proto overhead.
	s.Equal(12, p.Size())
}

func (s *predicatesSuite) TestDestinationPredicate_Test() {
	destinations := []string{uuid.New(), uuid.New()}

	p := NewDestinationPredicate(destinations)
	for _, dest := range destinations {
		mockTask := &StateMachineOutboundTask{Destination: dest}
		s.True(p.Test(mockTask))
	}

	mockTask := &StateMachineOutboundTask{Destination: uuid.New()}
	s.False(p.Test(mockTask))
}

func (s *predicatesSuite) TestDestinationPredicate_Equals() {
	destinations := []string{uuid.New(), uuid.New()}

	p := NewDestinationPredicate(destinations)

	s.True(p.Equals(p))
	s.True(p.Equals(NewDestinationPredicate(destinations)))
	rand.Shuffle(
		len(destinations),
		func(i, j int) {
			destinations[i], destinations[j] = destinations[j], destinations[i]
		},
	)
	s.True(p.Equals(NewDestinationPredicate(destinations)))

	s.False(p.Equals(NewDestinationPredicate([]string{uuid.New(), uuid.New()})))
	s.False(p.Equals(NewTypePredicate([]enumsspb.TaskType{enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER})))
	s.False(p.Equals(predicates.Universal[Task]()))
}

func (s *predicatesSuite) TestDestinationPredicate_Size() {
	destinations := []string{uuid.New(), uuid.New()}

	p := NewDestinationPredicate(destinations)

	// UUID length is 36 and 4 bytes accounted for proto overhead.
	s.Equal(76, p.Size())
}

func (s *predicatesSuite) TestOutboundTaskGroupPredicate_Test() {
	groups := []string{"1", "2"}

	p := NewOutboundTaskGroupPredicate(groups)
	for _, t := range groups {
		mockTask := &StateMachineOutboundTask{StateMachineTask: StateMachineTask{Info: &persistencespb.StateMachineTaskInfo{Type: t}}}
		s.True(p.Test(mockTask))
	}

	mockTask := &StateMachineOutboundTask{StateMachineTask: StateMachineTask{Info: &persistencespb.StateMachineTaskInfo{Type: "3"}}}
	s.False(p.Test(mockTask))
}

func (s *predicatesSuite) TestOutboundTaskGroupPredicate_Equals() {
	groups := []string{"1", "2"}

	p := NewOutboundTaskGroupPredicate(groups)

	s.True(p.Equals(p))
	s.True(p.Equals(NewOutboundTaskGroupPredicate(groups)))
	rand.Shuffle(
		len(groups),
		func(i, j int) {
			groups[i], groups[j] = groups[j], groups[i]
		},
	)
	s.True(p.Equals(NewOutboundTaskGroupPredicate(groups)))

	s.False(p.Equals(NewOutboundTaskGroupPredicate([]string{"3", "4"})))
	s.False(p.Equals(NewTypePredicate([]enumsspb.TaskType{enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER})))
	s.False(p.Equals(predicates.Universal[Task]()))
}

func (s *predicatesSuite) TestOutboundTaskGroupPredicate_Size() {
	groups := []string{uuid.New(), uuid.New()}

	p := NewOutboundTaskGroupPredicate(groups)

	// UUID length is 36 and 4 bytes accounted for proto overhead.
	s.Equal(76, p.Size())
}

func (s *predicatesSuite) TestOutboundTaskPredicate_Test() {
	groups := []TaskGroupNamespaceIDAndDestination{
		{"g1", "n1", "d1"},
		{"g2", "n2", "d2"},
	}

	p := NewOutboundTaskPredicate(groups)
	for _, g := range groups {
		mockTask := &StateMachineOutboundTask{
			StateMachineTask: StateMachineTask{
				Info:        &persistencespb.StateMachineTaskInfo{Type: g.TaskGroup},
				WorkflowKey: definition.NewWorkflowKey(g.NamespaceID, "", ""),
			},
			Destination: g.Destination,
		}
		s.True(p.Test(mockTask))
	}

	// Verify any field mismatch fails Test().
	mockTask := &StateMachineOutboundTask{
		StateMachineTask: StateMachineTask{
			Info:        &persistencespb.StateMachineTaskInfo{Type: "g1"},
			WorkflowKey: definition.NewWorkflowKey("n1", "", ""),
		},
		Destination: "d3",
	}
	s.False(p.Test(mockTask))
	mockTask = &StateMachineOutboundTask{
		StateMachineTask: StateMachineTask{
			Info:        &persistencespb.StateMachineTaskInfo{Type: "g3"},
			WorkflowKey: definition.NewWorkflowKey("n1", "", ""),
		},
		Destination: "d1",
	}
	s.False(p.Test(mockTask))
	mockTask = &StateMachineOutboundTask{
		StateMachineTask: StateMachineTask{
			Info:        &persistencespb.StateMachineTaskInfo{Type: "g1"},
			WorkflowKey: definition.NewWorkflowKey("n3", "", ""),
		},
		Destination: "d1",
	}
	s.False(p.Test(mockTask))
}

func (s *predicatesSuite) TestOutboundTaskPredicate_Equals() {
	groups := []TaskGroupNamespaceIDAndDestination{
		{"g1", "n1", "d1"},
		{"g2", "n2", "d2"},
	}

	p := NewOutboundTaskPredicate(groups)

	s.True(p.Equals(p))
	s.True(p.Equals(NewOutboundTaskPredicate(groups)))
	rand.Shuffle(
		len(groups),
		func(i, j int) {
			groups[i], groups[j] = groups[j], groups[i]
		},
	)
	s.True(p.Equals(NewOutboundTaskPredicate(groups)))

	s.False(p.Equals(NewOutboundTaskPredicate([]TaskGroupNamespaceIDAndDestination{
		{"g1", "n1", "d3"},
		{"g2", "n2", "d4"},
	})))
	s.False(p.Equals(NewTypePredicate([]enumsspb.TaskType{enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER})))
	s.False(p.Equals(predicates.Universal[Task]()))
}

func (s *predicatesSuite) TestOutboundTaskPredicate_Size() {
	groups := []TaskGroupNamespaceIDAndDestination{
		{"g1", "n1", "d1"},
		{"g2", "n2", "d2"},
	}
	p := NewOutboundTaskPredicate(groups)

	s.Equal(16, p.Size())
}

func (s *predicatesSuite) TestAndPredicates() {
	testCases := []struct {
		predicateA     Predicate
		predicateB     Predicate
		expectedResult Predicate
	}{
		{
			predicateA:     NewNamespacePredicate([]string{"namespace1", "namespace2"}),
			predicateB:     NewNamespacePredicate([]string{"namespace2", "namespace3"}),
			expectedResult: NewNamespacePredicate([]string{"namespace2"}),
		},
		{
			predicateA: NewTypePredicate([]enumsspb.TaskType{
				enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
			}),
			predicateB: NewTypePredicate([]enumsspb.TaskType{
				enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
				enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT,
			}),
			expectedResult: NewTypePredicate([]enumsspb.TaskType{
				enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
			}),
		},
		{
			predicateA:     NewDestinationPredicate([]string{"dest1", "dest2"}),
			predicateB:     NewDestinationPredicate([]string{"dest2", "dest3"}),
			expectedResult: NewDestinationPredicate([]string{"dest2"}),
		},
		{
			predicateA:     NewNamespacePredicate([]string{"namespace1", "namespace2"}),
			predicateB:     NewNamespacePredicate([]string{"namespace3"}),
			expectedResult: predicates.Empty[Task](),
		},
		{
			predicateA:     NewOutboundTaskGroupPredicate([]string{"g1", "g2"}),
			predicateB:     NewOutboundTaskGroupPredicate([]string{"g3"}),
			expectedResult: predicates.Empty[Task](),
		},
		{
			predicateA: NewOutboundTaskPredicate([]TaskGroupNamespaceIDAndDestination{
				{"g1", "n1", "d1"},
				{"g2", "n2", "d2"},
			}),
			predicateB: NewOutboundTaskPredicate([]TaskGroupNamespaceIDAndDestination{
				{"g3", "n3", "d3"},
			}),
			expectedResult: predicates.Empty[Task](),
		},
		{
			predicateA: NewNamespacePredicate([]string{"namespace1"}),
			predicateB: NewTypePredicate([]enumsspb.TaskType{
				enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
			}),
			expectedResult: predicates.And(
				NewNamespacePredicate([]string{"namespace1"}),
				NewTypePredicate([]enumsspb.TaskType{
					enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
				}),
			),
		},
		{
			predicateA:     NewNamespacePredicate([]string{"namespace1", "namespace2"}),
			predicateB:     predicates.Not(NewNamespacePredicate([]string{"namespace2", "namespace3"})),
			expectedResult: NewNamespacePredicate([]string{"namespace1"}),
		},
		{
			predicateA:     predicates.Not(NewNamespacePredicate([]string{"namespace2", "namespace3"})),
			predicateB:     NewNamespacePredicate([]string{"namespace1", "namespace2"}),
			expectedResult: NewNamespacePredicate([]string{"namespace1"}),
		},
		{
			predicateA:     predicates.Not(NewNamespacePredicate([]string{"namespace1", "namespace2"})),
			predicateB:     predicates.Not(NewNamespacePredicate([]string{"namespace2", "namespace3"})),
			expectedResult: predicates.Not(NewNamespacePredicate([]string{"namespace1", "namespace2", "namespace3"})),
		},
		{
			predicateA:     NewNamespacePredicate([]string{"namespace1", "namespace2"}),
			predicateB:     predicates.Not(NewNamespacePredicate([]string{"namespace1", "namespace2", "namespace3"})),
			expectedResult: predicates.Empty[Task](),
		},
	}

	for _, tc := range testCases {
		s.Equal(tc.expectedResult, AndPredicates(tc.predicateA, tc.predicateB))
	}
}

func (s *predicatesSuite) TestOrPredicates() {
	testCases := []struct {
		predicateA     Predicate
		predicateB     Predicate
		expectedResult Predicate
	}{
		{
			predicateA:     NewNamespacePredicate([]string{"namespace1", "namespace2"}),
			predicateB:     NewNamespacePredicate([]string{"namespace2", "namespace3"}),
			expectedResult: NewNamespacePredicate([]string{"namespace1", "namespace2", "namespace3"}),
		},
		{
			predicateA: NewTypePredicate([]enumsspb.TaskType{
				enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
			}),
			predicateB: NewTypePredicate([]enumsspb.TaskType{
				enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
				enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT,
			}),
			expectedResult: NewTypePredicate([]enumsspb.TaskType{
				enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
				enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT,
			}),
		},
		{
			predicateA:     NewDestinationPredicate([]string{"dest1", "dest2"}),
			predicateB:     NewDestinationPredicate([]string{"dest2", "dest3"}),
			expectedResult: NewDestinationPredicate([]string{"dest1", "dest2", "dest3"}),
		},
		{
			predicateA:     NewOutboundTaskGroupPredicate([]string{"g1", "g2"}),
			predicateB:     NewOutboundTaskGroupPredicate([]string{"g2", "g3"}),
			expectedResult: NewOutboundTaskGroupPredicate([]string{"g1", "g2", "g3"}),
		},
		{
			predicateA: NewOutboundTaskPredicate([]TaskGroupNamespaceIDAndDestination{
				{"g1", "n1", "d1"},
				{"g2", "n2", "d2"},
			}),
			predicateB: NewOutboundTaskPredicate([]TaskGroupNamespaceIDAndDestination{
				{"g3", "n3", "d3"},
			}),
			expectedResult: NewOutboundTaskPredicate([]TaskGroupNamespaceIDAndDestination{
				{"g1", "n1", "d1"},
				{"g2", "n2", "d2"},
				{"g3", "n3", "d3"},
			}),
		},
		{
			predicateA: NewNamespacePredicate([]string{"namespace1"}),
			predicateB: NewTypePredicate([]enumsspb.TaskType{
				enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
			}),
			expectedResult: predicates.Or(
				NewNamespacePredicate([]string{"namespace1"}),
				NewTypePredicate([]enumsspb.TaskType{
					enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
				}),
			),
		},
		{
			predicateA:     NewNamespacePredicate([]string{"namespace1", "namespace2"}),
			predicateB:     predicates.Not(NewNamespacePredicate([]string{"namespace2", "namespace3"})),
			expectedResult: predicates.Not(NewNamespacePredicate([]string{"namespace3"})),
		},
		{
			predicateA:     predicates.Not(NewNamespacePredicate([]string{"namespace2", "namespace3"})),
			predicateB:     NewNamespacePredicate([]string{"namespace1", "namespace2"}),
			expectedResult: predicates.Not(NewNamespacePredicate([]string{"namespace3"})),
		},
		{
			predicateA:     predicates.Not(NewNamespacePredicate([]string{"namespace1", "namespace2"})),
			predicateB:     predicates.Not(NewNamespacePredicate([]string{"namespace2", "namespace3"})),
			expectedResult: predicates.Not(NewNamespacePredicate([]string{"namespace2"})),
		},
	}

	for _, tc := range testCases {
		s.Equal(tc.expectedResult, OrPredicates(tc.predicateA, tc.predicateB))
	}
}
