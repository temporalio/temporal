package matching

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/common/tqid"
)

type AckManagerTestSuite struct {
	suite.Suite
	logger *testlogger.TestLogger
}

func TestAckManagerTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, &AckManagerTestSuite{})
}

func (s *AckManagerTestSuite) SetupTest() {
	s.logger = testlogger.NewTestLogger(s.T(), testlogger.FailOnAnyUnexpectedError)
}

func (s *AckManagerTestSuite) AddingTasksIncreasesBacklogCounter() {
	ackMgr := newTestAckMgr(s.logger)

	ackMgr.addTask(1)
	s.Equal(ackMgr.getBacklogCountHint(), int64(1))

	ackMgr.addTask(12)
	s.Equal(ackMgr.getBacklogCountHint(), int64(2))
}

func (s *AckManagerTestSuite) CompleteTaskMovesAckLevelUpToGap() {
	ackMgr := newTestAckMgr(s.logger)

	_, err := ackMgr.db.RenewLease(context.Background())
	s.NoError(err)

	ackMgr.addTask(1)
	ackMgr.db.updateBacklogStats(1, time.Time{}) // increment the backlog so that we don't under-count
	s.Equal(int64(-1), ackMgr.getAckLevel(), "should only move ack level on completion")

	ackLevel, numAcked := ackMgr.completeTask(1)
	s.Equal(int64(1), ackLevel, "should move ack level on completion")
	s.Equal(int64(1), numAcked, "should move ack level on completion")

	ackMgr.addTask(2)
	ackMgr.addTask(3)
	ackMgr.addTask(12)
	ackMgr.db.updateBacklogStats(3, time.Time{})

	ackLevel, numAcked = ackMgr.completeTask(3)
	s.Equal(int64(1), ackLevel, "task 2 is not complete, we should not move ack level")
	s.Equal(int64(0), numAcked, "task 2 is not complete, we should not move ack level")

	ackLevel, numAcked = ackMgr.completeTask(2)
	s.Equal(int64(3), ackLevel, "both tasks 2 and 3 are complete")
	s.Equal(int64(2), numAcked, "both tasks 2 and 3 are complete")
}

func (s *AckManagerTestSuite) TestAckManager() {
	ackMgr := newTestAckMgr(s.logger)

	_, err := ackMgr.db.RenewLease(context.Background())
	s.NoError(err)

	ackMgr.setAckLevel(100)
	s.EqualValues(100, ackMgr.getAckLevel())
	s.EqualValues(100, ackMgr.getReadLevel())
	const t1 = 200
	const t2 = 220
	const t3 = 320
	const t4 = 340
	const t5 = 360
	const t6 = 380

	ackMgr.addTask(t1)
	// Increment the backlog so that we don't under-count
	// this happens since we decrease the counter on completion of a task
	ackMgr.db.updateBacklogStats(1, time.Time{})
	s.EqualValues(100, ackMgr.getAckLevel())
	s.EqualValues(t1, ackMgr.getReadLevel())

	ackMgr.addTask(t2)
	ackMgr.db.updateBacklogStats(1, time.Time{})
	s.EqualValues(100, ackMgr.getAckLevel())
	s.EqualValues(t2, ackMgr.getReadLevel())

	ackMgr.completeTask(t2)
	s.EqualValues(100, ackMgr.getAckLevel())
	s.EqualValues(t2, ackMgr.getReadLevel())

	ackMgr.completeTask(t1)
	s.EqualValues(t2, ackMgr.getAckLevel())
	s.EqualValues(t2, ackMgr.getReadLevel())

	ackMgr.setAckLevel(300)
	s.EqualValues(300, ackMgr.getAckLevel())
	s.EqualValues(300, ackMgr.getReadLevel())

	ackMgr.addTask(t3)
	ackMgr.db.updateBacklogStats(1, time.Time{})
	s.EqualValues(300, ackMgr.getAckLevel())
	s.EqualValues(t3, ackMgr.getReadLevel())

	ackMgr.addTask(t4)
	ackMgr.db.updateBacklogStats(1, time.Time{})
	s.EqualValues(300, ackMgr.getAckLevel())
	s.EqualValues(t4, ackMgr.getReadLevel())

	ackMgr.completeTask(t3)
	s.EqualValues(t3, ackMgr.getAckLevel())
	s.EqualValues(t4, ackMgr.getReadLevel())

	ackMgr.completeTask(t4)
	s.EqualValues(t4, ackMgr.getAckLevel())
	s.EqualValues(t4, ackMgr.getReadLevel())

	ackMgr.setReadLevel(t5)
	s.EqualValues(t5, ackMgr.getReadLevel())

	ackMgr.setAckLevel(t5)
	ackMgr.setReadLevelAfterGap(t6)
	s.EqualValues(t6, ackMgr.getReadLevel())
	s.EqualValues(t6, ackMgr.getAckLevel())
}

func (s *AckManagerTestSuite) Sort() {
	ackMgr := newTestAckMgr(s.logger)

	_, err := ackMgr.db.RenewLease(context.Background())
	s.NoError(err)

	const t0 = 100
	ackMgr.setAckLevel(t0)
	s.EqualValues(t0, ackMgr.getAckLevel())
	s.EqualValues(t0, ackMgr.getReadLevel())
	const t1 = 200
	const t2 = 220
	const t3 = 320
	const t4 = 340
	const t5 = 360

	ackMgr.addTask(t1)
	ackMgr.addTask(t2)
	ackMgr.addTask(t3)
	ackMgr.addTask(t4)
	ackMgr.addTask(t5)

	// Increment the backlog so that we don't under-count
	// this happens since we decrease the counter on completion of a task
	ackMgr.db.updateBacklogStats(5, time.Time{})

	ackMgr.completeTask(t2)
	s.EqualValues(t0, ackMgr.getAckLevel())

	ackMgr.completeTask(t1)
	s.EqualValues(t2, ackMgr.getAckLevel())

	ackMgr.completeTask(t5)
	s.EqualValues(t2, ackMgr.getAckLevel())

	ackMgr.completeTask(t4)
	s.EqualValues(t2, ackMgr.getAckLevel())

	ackMgr.completeTask(t3)
	s.EqualValues(t5, ackMgr.getAckLevel())
}

func BenchmarkAckManager_AddTask(b *testing.B) {
	ackMgr := newTestAckMgr(log.NewTestLogger())

	tasks := make([]int, 1000)
	for i := 0; i < len(tasks); i++ {
		tasks[i] = i
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Add 1000 tasks in order and complete them in a random order.
		// This will cause our ack level to jump as we complete them
		b.StopTimer()
		rand.Shuffle(len(tasks), func(i, j int) {
			tasks[i], tasks[j] = tasks[j], tasks[i]
		})
		b.StartTimer()
		for i := 0; i < len(tasks); i++ {
			tasks[i] = i
			ackMgr.addTask(int64(i))
		}
	}
}

func BenchmarkAckManager_CompleteTask(b *testing.B) {
	ackMgr := newTestAckMgr(log.NewTestLogger())

	tasks := make([]int, 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Add 1000 tasks in order and complete them in a random order.
		// This will cause our ack level to jump as we complete them
		b.StopTimer()
		for i := 0; i < len(tasks); i++ {
			tasks[i] = i
			ackMgr.addTask(int64(i))
			ackMgr.db.updateBacklogStats(1, time.Time{}) // Increment the backlog so that we don't under-count
		}
		rand.Shuffle(len(tasks), func(i, j int) {
			tasks[i], tasks[j] = tasks[j], tasks[i]
		})
		b.StartTimer()

		for i := 0; i < len(tasks); i++ {
			ackMgr.completeTask(int64(i))
		}
	}
}

func newTestAckMgr(logger log.Logger) *ackManager {
	tm := newTestTaskManager(logger)
	cfg := NewConfig(dynamicconfig.NewNoopCollection())
	f, _ := tqid.NewTaskQueueFamily("", "test-queue")
	prtn := f.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).NormalPartition(0)
	tlCfg := newTaskQueueConfig(prtn.TaskQueue(), cfg, "test-namespace")
	db := newTaskQueueDB(tlCfg, tm, UnversionedQueueKey(prtn), logger, metrics.NoopMetricsHandler, false)
	return newAckManager(db, logger)
}
