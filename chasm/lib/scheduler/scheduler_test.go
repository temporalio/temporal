package scheduler_test

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type schedulerTestSuite struct {
	schedulerSuite
}

func TestSchedulerSuite(t *testing.T) {
	suite.Run(t, &schedulerTestSuite{})
}

func (s *schedulerTestSuite) TestGetListInfo() {
	ctx := s.newMutableContext()

	// Generator maintains the FutureActionTimes list.
	generator, err := s.scheduler.Generator.Get(ctx)
	s.NoError(err)
	expectedFutureTimes := []*timestamppb.Timestamp{timestamppb.Now(), timestamppb.Now()}
	generator.FutureActionTimes = expectedFutureTimes

	listInfo, err := s.scheduler.GetListInfo(ctx)
	s.NoError(err)

	// Should return a populated info block.
	s.NotNil(listInfo)
	s.NotNil(listInfo.Spec)
	s.NotEmpty(listInfo.Spec.Interval)
	s.ProtoEqual(listInfo.Spec.Interval[0], s.scheduler.Schedule.Spec.Interval[0])
	s.NotNil(listInfo.WorkflowType)
	s.NotEmpty(listInfo.FutureActionTimes)
	s.Equal(expectedFutureTimes, listInfo.FutureActionTimes)
}
