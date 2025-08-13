package retention_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/retention"
	"go.uber.org/mock/gomock"
)

type retentionSuite struct {
	suite.Suite
	*require.Assertions
	controller *gomock.Controller

	mockCtx *chasm.MockMutableContext
	now     time.Time
}

func (s *retentionSuite) SetupSuite() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
	s.mockCtx = chasm.NewMockMutableContext(s.controller)

	s.now = time.Now()
	s.mockCtx.EXPECT().Now(gomock.Any()).Return(s.now).AnyTimes()
}

func TestRetention(t *testing.T) {
	suite.Run(t, &retentionSuite{})
}

func (s *retentionSuite) TestNewRetentionValidation() {
	_, err := retention.NewRetention(s.mockCtx, 1*time.Hour, chasm.LifecycleStateRunning)
	s.ErrorContains(err, "terminalState isn't a 'Closed' state: Running")
}

func (s *retentionSuite) TestNewRetentionTask() {
	duration := 1 * time.Hour
	expectedAt := s.now.Add(duration)
	expectedState := chasm.LifecycleStateCompleted

	s.mockCtx.EXPECT().AddTask(gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(_ chasm.Component, attrs chasm.TaskAttributes, _ *persistencespb.RetentionTask) {
			s.Equal(expectedAt, attrs.ScheduledTime)
		}).
		Times(1)

	// Create the retention component, which should create the timer task.
	r, err := retention.NewRetention(s.mockCtx, 1*time.Hour, expectedState)
	s.NoError(err)
	s.Equal(chasm.LifecycleStateRunning, r.LifecycleState(s.mockCtx))

	// Execute task.
	executor := retention.NewRetentionTaskExecutor()

	valid, err := executor.Validate(s.mockCtx, r, chasm.TaskAttributes{}, nil)
	s.True(valid)
	s.NoError(err)

	err = executor.Execute(s.mockCtx, r, chasm.TaskAttributes{}, nil)
	s.NoError(err)
	s.True(r.Closed)
	s.Equal(expectedState, r.LifecycleState(s.mockCtx))

	valid, err = executor.Validate(s.mockCtx, r, chasm.TaskAttributes{}, nil)
	s.False(valid)
	s.NoError(err)
}
