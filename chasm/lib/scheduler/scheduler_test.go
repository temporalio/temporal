package scheduler_test

import (
	"testing"

	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/service/history/tasks"
)

type schedulerTestSuite struct {
	schedulerSuite
}

func TestSchedulerSuite(t *testing.T) {
	suite.Run(t, &schedulerTestSuite{})
}

func (s *schedulerTestSuite) TestGetListInfo() {
	ctx := s.newMutableContext()
	now := s.timeSource.Now()

	listInfo, err := s.scheduler.GetListInfo(ctx, s.specProcessor)
	s.NoError(err)

	// Should return a populated info block.
	s.NotNil(listInfo)
	s.NotNil(listInfo.Spec)
	s.NotNil(listInfo.WorkflowType)
	s.NotEmpty(listInfo.FutureActionTimes)

	for _, timestamp := range listInfo.FutureActionTimes {
		s.True(timestamp.AsTime().After(now))
	}
}

func (s *schedulerTestSuite) TestUpdateVisibility_TaskGeneration() {
	ctx := s.newMutableContext()

	// Custom search attributes should generate a visibility task.
	customAttrs := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"custom_field": {Data: []byte("test_value")},
		},
	}

	err := s.scheduler.UpdateVisibility(ctx, s.specProcessor, customAttrs)
	s.NoError(err)

	_, err = s.node.CloseTransaction()
	s.NoError(err)

	s.True(s.hasTask(&tasks.ChasmTask{}, chasm.TaskScheduledTimeImmediate))
}

func (s *schedulerTestSuite) TestUpdateVisibility_NoTaskWhenUnchanged() {
	ctx := s.newMutableContext()

	// Set up Visibility to already be up-to-date.
	err := s.scheduler.UpdateVisibility(ctx, s.specProcessor, nil)
	s.NoError(err)

	_, err = s.node.CloseTransaction()
	s.NoError(err)
	s.True(s.hasTask(&tasks.ChasmTask{}, chasm.TaskScheduledTimeImmediate))

	// TODO - fix this with unit testing task generation hooks. Manually verified
	// for now.
	s.addedTasks = make([]tasks.Task, 0)
	// s.node.RefreshTasks()

	// No custom search attributes and no changes should not generate a task.
	err = s.scheduler.UpdateVisibility(ctx, s.specProcessor, nil)
	s.NoError(err)

	_, err = s.node.CloseTransaction()
	s.NoError(err)
	s.Empty(s.addedTasks)
}
