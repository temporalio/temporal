package executions

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/service/worker/scanner/executor"
	"go.uber.org/mock/gomock"
)

type taskTestSuite struct {
	suite.Suite
	controller       *gomock.Controller
	executionManager *persistence.MockExecutionManager
	rateLimiter      *quotas.MockRateLimiter
}

func TestTaskTestSuite(t *testing.T) {
	suite.Run(t, new(taskTestSuite))
}

func (s *taskTestSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.executionManager = persistence.NewMockExecutionManager(s.controller)
	s.rateLimiter = quotas.NewMockRateLimiter(s.controller)
}

func (s *taskTestSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *taskTestSuite) createTask() *task {
	return &task{
		shardID:          1,
		executionManager: s.executionManager,
		metricsHandler:   metrics.NoopMetricsHandler,
		logger:           log.NewNoopLogger(),
		scavenger:        &Scavenger{numHistoryShards: 4},
		ctx:              context.Background(),
		rateLimiter:      s.rateLimiter,
	}
}

func (s *taskTestSuite) TestRun_Success_EmptyResults() {
	task := s.createTask()

	s.executionManager.EXPECT().
		ListConcreteExecutions(gomock.Any(), gomock.Any()).
		Return(&persistence.ListConcreteExecutionsResponse{}, nil)

	status := task.Run()
	assert.Equal(s.T(), executor.TaskStatusDone, status)
}

func (s *taskTestSuite) TestRun_PaginationError() {
	task := s.createTask()

	s.rateLimiter.EXPECT().Wait(gomock.Any()).Return(nil)
	s.executionManager.EXPECT().
		ListConcreteExecutions(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("pagination error"))

	status := task.Run()
	assert.Equal(s.T(), executor.TaskStatusDefer, status)
}
