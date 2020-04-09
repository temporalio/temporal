package executor

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/metrics"
)

type (
	ExecutorTestSuite struct {
		suite.Suite
	}
	testTask struct {
		next    TaskStatus
		counter *int64
	}
)

func TestExecutionTestSuite(t *testing.T) {
	suite.Run(t, new(ExecutorTestSuite))
}

func (s *ExecutorTestSuite) TestStartStop() {
	e := NewFixedSizePoolExecutor(
		4, 4, metrics.NewClient(tally.NoopScope, metrics.Worker), metrics.TaskListScavengerScope)
	e.Start()
	e.Stop()
}

func (s *ExecutorTestSuite) TestTaskExecution() {
	e := NewFixedSizePoolExecutor(
		32, 100, metrics.NewClient(tally.NoopScope, metrics.Worker), metrics.TaskListScavengerScope)
	e.Start()
	var runCounter int64
	var startWG sync.WaitGroup
	for i := 0; i < 5; i++ {
		startWG.Add(1)
		go func() {
			defer startWG.Done()
			for i := 0; i < 20; i++ {
				if i%2 == 0 {
					e.Submit(&testTask{TaskStatusDefer, &runCounter})
					continue
				}
				e.Submit(&testTask{TaskStatusDone, &runCounter})
			}
		}()
	}
	s.True(common.AwaitWaitGroup(&startWG, time.Second*10))
	s.True(s.awaitCompletion(e))
	s.Equal(int64(150), runCounter)
	e.Stop()
}

func (s *ExecutorTestSuite) awaitCompletion(e Executor) bool {
	expiry := time.Now().Add(time.Second * 10)
	for time.Now().Before(expiry) {
		if e.TaskCount() == 0 {
			return true
		}
		time.Sleep(time.Millisecond * 50)
	}
	return false
}

func (tt *testTask) Run() TaskStatus {
	atomic.AddInt64(tt.counter, 1)
	status := tt.next
	if status == TaskStatusDefer {
		tt.next = TaskStatusDone
	}
	return status
}
