package replication

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/definition"
	"go.uber.org/mock/gomock"
)

type (
	sequentialQueueSuite struct {
		suite.Suite
		*require.Assertions
		seqQController *gomock.Controller
	}
)

func TestSequentialQueueSuite(t *testing.T) {
	suite.Run(t, new(sequentialQueueSuite))
}

func (s *sequentialQueueSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.seqQController = gomock.NewController(s.T())
}

func (s *sequentialQueueSuite) seqQNewTask(taskID int64) *MockTrackableExecutableTask {
	task := NewMockTrackableExecutableTask(s.seqQController)
	task.EXPECT().TaskID().Return(taskID).AnyTimes()
	return task
}

func (s *sequentialQueueSuite) TestNewSequentialTaskQueue_UsesTaskQueueID() {
	queueID := "seqq-queue-id"
	task := s.seqQNewTask(1)
	task.EXPECT().QueueID().Return(queueID).Times(1)

	queue := NewSequentialTaskQueue(task)
	s.Equal(queueID, queue.ID())
	s.True(queue.IsEmpty())
	s.Equal(0, queue.Len())
}

func (s *sequentialQueueSuite) TestNewSequentialTaskQueueWithID() {
	queueID := "seqq-explicit-id"
	queue := NewSequentialTaskQueueWithID(queueID)
	s.Equal(queueID, queue.ID())
	s.True(queue.IsEmpty())
	s.Equal(0, queue.Len())
}

func (s *sequentialQueueSuite) TestAddPeekRemoveOrdering() {
	queue := NewSequentialTaskQueueWithID("seqq-order")

	task3 := s.seqQNewTask(3)
	task1 := s.seqQNewTask(1)
	task2 := s.seqQNewTask(2)

	queue.Add(task3)
	queue.Add(task1)
	queue.Add(task2)

	s.False(queue.IsEmpty())
	s.Equal(3, queue.Len())

	// Peek should return the smallest TaskID without removing it.
	concreteQueue, ok := queue.(*SequentialTaskQueue)
	s.True(ok)
	s.Equal(int64(1), concreteQueue.Peek().TaskID())
	s.Equal(3, queue.Len())

	s.Equal(int64(1), queue.Remove().TaskID())
	s.Equal(int64(2), queue.Remove().TaskID())
	s.Equal(int64(3), queue.Remove().TaskID())

	s.True(queue.IsEmpty())
	s.Equal(0, queue.Len())
}

func (s *sequentialQueueSuite) TestSequentialTaskQueueCompareLess() {
	task1 := s.seqQNewTask(1)
	task2 := s.seqQNewTask(2)

	s.True(SequentialTaskQueueCompareLess(task1, task2))
	s.False(SequentialTaskQueueCompareLess(task2, task1))
	s.False(SequentialTaskQueueCompareLess(task1, task1))
}

func (s *sequentialQueueSuite) TestWorkflowKeyHashFn_WorkflowKey() {
	key := definition.NewWorkflowKey("namespace-id", "workflow-id", "run-id")
	h1 := WorkflowKeyHashFn(key)
	h2 := WorkflowKeyHashFn(key)
	// Deterministic for the same key.
	s.Equal(h1, h2)

	other := definition.NewWorkflowKey("namespace-id", "workflow-id", "different-run-id")
	s.NotEqual(h1, WorkflowKeyHashFn(other))
}

func (s *sequentialQueueSuite) TestWorkflowKeyHashFn_NonWorkflowKey() {
	s.Equal(uint32(0), WorkflowKeyHashFn("not-a-workflow-key"))
	s.Equal(uint32(0), WorkflowKeyHashFn(42))
}
