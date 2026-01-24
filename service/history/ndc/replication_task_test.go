package ndc

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.uber.org/mock/gomock"
)

type (
	replicationTaskSuite struct {
		suite.Suite
		*require.Assertions

		controller      *gomock.Controller
		clusterMetadata *cluster.MockMetadata
	}
)

func TestReplicationTaskSuite(t *testing.T) {
	s := new(replicationTaskSuite)
	suite.Run(t, s)
}

func (s *replicationTaskSuite) SetupSuite() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
	s.clusterMetadata = cluster.NewMockMetadata(s.controller)
	s.clusterMetadata.EXPECT().ClusterNameForFailoverVersion(gomock.Any(), gomock.Any()).Return("some random cluster name").AnyTimes()
}

func (s *replicationTaskSuite) TearDownSuite() {

}

func (s *replicationTaskSuite) TestValidateEventsSlice() {
	eS1 := []*historypb.HistoryEvent{
		historypb.HistoryEvent_builder{
			EventId: 1,
			Version: 2,
		}.Build(),
		historypb.HistoryEvent_builder{
			EventId: 2,
			Version: 2,
		}.Build(),
	}
	eS2 := []*historypb.HistoryEvent{
		historypb.HistoryEvent_builder{
			EventId: 3,
			Version: 2,
		}.Build(),
	}

	eS3 := []*historypb.HistoryEvent{
		historypb.HistoryEvent_builder{
			EventId: 4,
			Version: 2,
		}.Build(),
	}

	v, err := validateEventsSlice(eS1, eS2)
	s.Equal(int64(2), v)
	s.Nil(err)

	v, err = validateEventsSlice(eS1, eS3)
	s.Equal(int64(0), v)
	s.IsType(ErrEventSlicesNotConsecutive, err)

	v, err = validateEventsSlice(eS1, nil)
	s.Equal(int64(0), v)
	s.IsType(ErrEmptyEventSlice, err)
}

func (s *replicationTaskSuite) TestValidateEvents() {
	eS1 := []*historypb.HistoryEvent{
		historypb.HistoryEvent_builder{
			EventId: 1,
			Version: 2,
		}.Build(),
		historypb.HistoryEvent_builder{
			EventId: 2,
			Version: 2,
		}.Build(),
	}

	eS2 := []*historypb.HistoryEvent{
		historypb.HistoryEvent_builder{
			EventId: 1,
			Version: 2,
		}.Build(),
		historypb.HistoryEvent_builder{
			EventId: 3,
			Version: 2,
		}.Build(),
	}

	eS3 := []*historypb.HistoryEvent{
		historypb.HistoryEvent_builder{
			EventId: 1,
			Version: 1,
		}.Build(),
		historypb.HistoryEvent_builder{
			EventId: 2,
			Version: 2,
		}.Build(),
	}

	v, err := validateEvents(eS1)
	s.Nil(err)
	s.Equal(int64(2), v)

	v, err = validateEvents(eS2)
	s.Equal(int64(0), v)
	s.IsType(ErrEventIDMismatch, err)

	v, err = validateEvents(eS3)
	s.Equal(int64(0), v)
	s.IsType(ErrEventVersionMismatch, err)
}

func (s *replicationTaskSuite) TestSkipDuplicatedEvents_ValidInput_SkipEvents() {
	workflowKey := definition.WorkflowKey{
		WorkflowID: uuid.NewString(),
		RunID:      uuid.NewString(),
	}
	slice1 := []*historypb.HistoryEvent{
		historypb.HistoryEvent_builder{
			EventId: 11,
		}.Build(),
		historypb.HistoryEvent_builder{
			EventId: 12,
		}.Build(),
	}
	slice2 := []*historypb.HistoryEvent{
		historypb.HistoryEvent_builder{
			EventId: 13,
		}.Build(),
		historypb.HistoryEvent_builder{
			EventId: 14,
		}.Build(),
	}

	task, _ := newReplicationTask(
		s.clusterMetadata,
		nil,
		workflowKey,
		nil,
		nil,
		[][]*historypb.HistoryEvent{slice1, slice2},
		nil,
		"",
		nil,
		false,
	)
	err := task.skipDuplicatedEvents(1)
	s.NoError(err)
	s.Equal(1, len(task.getEvents()))
	s.Equal(slice2, task.getEvents()[0])
	s.Equal(int64(13), task.getFirstEvent().GetEventId())
	s.Equal(int64(14), task.getLastEvent().GetEventId())
}

func (s *replicationTaskSuite) TestSkipDuplicatedEvents_InvalidInput_ErrorOut() {
	workflowKey := definition.WorkflowKey{
		WorkflowID: uuid.NewString(),
		RunID:      uuid.NewString(),
	}
	slice1 := []*historypb.HistoryEvent{
		historypb.HistoryEvent_builder{
			EventId: 11,
		}.Build(),
		historypb.HistoryEvent_builder{
			EventId: 12,
		}.Build(),
	}
	slice2 := []*historypb.HistoryEvent{
		historypb.HistoryEvent_builder{
			EventId: 13,
		}.Build(),
		historypb.HistoryEvent_builder{
			EventId: 14,
		}.Build(),
	}

	task, _ := newReplicationTask(
		s.clusterMetadata,
		nil,
		workflowKey,
		nil,
		nil,
		[][]*historypb.HistoryEvent{slice1, slice2},
		nil,
		"",
		nil,
		false,
	)
	err := task.skipDuplicatedEvents(2)
	s.Error(err)
}

func (s *replicationTaskSuite) TestSkipDuplicatedEvents_ZeroInput_DoNothing() {
	workflowKey := definition.WorkflowKey{
		WorkflowID: uuid.NewString(),
		RunID:      uuid.NewString(),
	}
	slice1 := []*historypb.HistoryEvent{
		historypb.HistoryEvent_builder{
			EventId: 11,
		}.Build(),
		historypb.HistoryEvent_builder{
			EventId: 12,
		}.Build(),
	}
	slice2 := []*historypb.HistoryEvent{
		historypb.HistoryEvent_builder{
			EventId: 13,
		}.Build(),
		historypb.HistoryEvent_builder{
			EventId: 14,
		}.Build(),
	}

	task, _ := newReplicationTask(
		s.clusterMetadata,
		nil,
		workflowKey,
		nil,
		nil,
		[][]*historypb.HistoryEvent{slice1, slice2},
		nil,
		"",
		nil,
		false,
	)
	err := task.skipDuplicatedEvents(0)
	s.NoError(err)
	s.Equal(2, len(task.getEvents()))
	s.Equal(slice1, task.getEvents()[0])
	s.Equal(slice2, task.getEvents()[1])
}

func (s *replicationTaskSuite) TestResetInfo() {
	workflowKey := definition.WorkflowKey{
		WorkflowID: uuid.NewString(),
		RunID:      uuid.NewString(),
	}
	slice1 := []*historypb.HistoryEvent{
		historypb.HistoryEvent_builder{
			EventId:   13,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED,
		}.Build(),
		historypb.HistoryEvent_builder{
			EventId: 14,
		}.Build(),
	}

	task, _ := newReplicationTask(
		s.clusterMetadata,
		nil,
		workflowKey,
		nil,
		nil,
		[][]*historypb.HistoryEvent{slice1},
		nil,
		"",
		nil,
		false,
	)
	info := task.getBaseWorkflowInfo()
	s.Nil(info)
	s.False(task.isWorkflowReset())
}
