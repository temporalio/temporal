package queues

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/tasks"
	"go.uber.org/mock/gomock"
)

type (
	priorityAssignerSuite struct {
		*require.Assertions
		suite.Suite

		controller *gomock.Controller
		nsRegistry *namespace.MockRegistry

		priorityAssigner *priorityAssignerImpl
	}
)

func TestPriorityAssignerSuite(t *testing.T) {
	s := new(priorityAssignerSuite)
	suite.Run(t, s)
}

func (s *priorityAssignerSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.nsRegistry = namespace.NewMockRegistry(s.controller)

	s.priorityAssigner = NewPriorityAssigner(
		s.nsRegistry,
		"current",
	).(*priorityAssignerImpl)
}

func (s *priorityAssignerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *priorityAssignerSuite) TestAssign_SelectedTaskTypes() {
	mockExecutable := NewMockExecutable(s.controller)
	mockExecutable.EXPECT().GetNamespaceID().Return("test-namespace-id").Times(1)
	mockExecutable.EXPECT().GetType().Return(enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT).Times(1)

	// Create a local namespace that is active in "current" cluster
	ns := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: "test-namespace-id"},
		nil,
		"current",
	)
	s.nsRegistry.EXPECT().GetNamespaceByID(namespace.ID("test-namespace-id")).Return(ns, nil).Times(1)

	s.Equal(tasks.PriorityPreemptable, s.priorityAssigner.Assign(mockExecutable))
}

func (s *priorityAssignerSuite) TestAssign_UnknownTaskTypes() {
	mockExecutable := NewMockExecutable(s.controller)
	mockExecutable.EXPECT().GetNamespaceID().Return("test-namespace-id").Times(1)
	mockExecutable.EXPECT().GetType().Return(enumsspb.TaskType(1234)).Times(1)

	// Create a local namespace that is active in "current" cluster
	ns := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: "test-namespace-id"},
		nil,
		"current",
	)
	s.nsRegistry.EXPECT().GetNamespaceByID(namespace.ID("test-namespace-id")).Return(ns, nil).Times(1)

	s.Equal(tasks.PriorityPreemptable, s.priorityAssigner.Assign(mockExecutable))
}

func (s *priorityAssignerSuite) TestAssign_HighPriorityTaskTypes() {
	for _, taskType := range []enumsspb.TaskType{
		enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER,
		enumsspb.TASK_TYPE_USER_TIMER,
		enumsspb.TASK_TYPE_WORKFLOW_BACKOFF_TIMER,
		enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK,
		enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK,
	} {
		mockExecutable := NewMockExecutable(s.controller)
		mockExecutable.EXPECT().GetNamespaceID().Return("test-namespace-id").Times(1)
		mockExecutable.EXPECT().GetType().Return(taskType).Times(1)

		// Create a local namespace that is active in "current" cluster
		ns := namespace.NewLocalNamespaceForTest(
			&persistencespb.NamespaceInfo{Id: "test-namespace-id"},
			nil,
			"current",
		)
		s.nsRegistry.EXPECT().GetNamespaceByID(namespace.ID("test-namespace-id")).Return(ns, nil).Times(1)

		s.Equal(tasks.PriorityHigh, s.priorityAssigner.Assign(mockExecutable))
	}
}

func (s *priorityAssignerSuite) TestAssign_BackgroundPriorityTaskTypes() {
	for _, taskType := range []enumsspb.TaskType{
		enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT,
		enumsspb.TASK_TYPE_TRANSFER_DELETE_EXECUTION,
		enumsspb.TASK_TYPE_VISIBILITY_DELETE_EXECUTION,
		enumsspb.TASK_TYPE_ARCHIVAL_ARCHIVE_EXECUTION,
		enumsspb.TASK_TYPE_UNSPECIFIED,
	} {
		mockExecutable := NewMockExecutable(s.controller)
		mockExecutable.EXPECT().GetNamespaceID().Return("test-namespace-id").Times(1)
		mockExecutable.EXPECT().GetType().Return(taskType).Times(1)

		// Create a local namespace that is active in "current" cluster
		ns := namespace.NewLocalNamespaceForTest(
			&persistencespb.NamespaceInfo{Id: "test-namespace-id"},
			nil,
			"current",
		)
		s.nsRegistry.EXPECT().GetNamespaceByID(namespace.ID("test-namespace-id")).Return(ns, nil).Times(1)

		s.Equal(tasks.PriorityPreemptable, s.priorityAssigner.Assign(mockExecutable))
	}
}

func (s *priorityAssignerSuite) TestAssign_LowPriorityTaskTypes() {
	for _, taskType := range []enumsspb.TaskType{
		enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
		enumsspb.TASK_TYPE_WORKFLOW_TASK_TIMEOUT,
		enumsspb.TASK_TYPE_WORKFLOW_RUN_TIMEOUT,
		enumsspb.TASK_TYPE_WORKFLOW_EXECUTION_TIMEOUT,
	} {
		mockExecutable := NewMockExecutable(s.controller)
		mockExecutable.EXPECT().GetNamespaceID().Return("test-namespace-id").Times(1)
		mockExecutable.EXPECT().GetType().Return(taskType).Times(1)

		// Create a local namespace that is active in "current" cluster
		ns := namespace.NewLocalNamespaceForTest(
			&persistencespb.NamespaceInfo{Id: "test-namespace-id"},
			nil,
			"current",
		)
		s.nsRegistry.EXPECT().GetNamespaceByID(namespace.ID("test-namespace-id")).Return(ns, nil).Times(1)

		s.Equal(tasks.PriorityLow, s.priorityAssigner.Assign(mockExecutable))
	}
}

func (s *priorityAssignerSuite) TestAssign_StandbyNamespace() {
	mockExecutable := NewMockExecutable(s.controller)
	mockExecutable.EXPECT().GetNamespaceID().Return("test-namespace-id").Times(1)

	// Create a global namespace with active cluster as "other", not "current"
	ns := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: "test-namespace-id"},
		nil,
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: "other",
			Clusters:          []string{"current", "other"},
		},
		1,
	)
	s.nsRegistry.EXPECT().GetNamespaceByID(namespace.ID("test-namespace-id")).Return(ns, nil).Times(1)

	s.Equal(tasks.PriorityPreemptable, s.priorityAssigner.Assign(mockExecutable))
}

func (s *priorityAssignerSuite) TestAssign_NamespaceNotFound() {
	mockExecutable := NewMockExecutable(s.controller)
	mockExecutable.EXPECT().GetNamespaceID().Return("test-namespace-id").Times(1)
	mockExecutable.EXPECT().GetType().Return(enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK).Times(1)

	s.nsRegistry.EXPECT().GetNamespaceByID(namespace.ID("test-namespace-id")).Return(nil, nil).Times(1)

	s.Equal(tasks.PriorityHigh, s.priorityAssigner.Assign(mockExecutable))
}

func (s *priorityAssignerSuite) TestAssign_ActiveNamespaceHighPriority() {
	mockExecutable := NewMockExecutable(s.controller)
	mockExecutable.EXPECT().GetNamespaceID().Return("test-namespace-id").Times(1)
	mockExecutable.EXPECT().GetType().Return(enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK).Times(1)

	// Create a global namespace with active cluster as "current"
	ns := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: "test-namespace-id"},
		nil,
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: "current",
			Clusters:          []string{"current", "other"},
		},
		1,
	)
	s.nsRegistry.EXPECT().GetNamespaceByID(namespace.ID("test-namespace-id")).Return(ns, nil).Times(1)

	s.Equal(tasks.PriorityHigh, s.priorityAssigner.Assign(mockExecutable))
}
