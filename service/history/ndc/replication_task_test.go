package ndc

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/consts"
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
		{
			EventId: 1,
			Version: 2,
		},
		{
			EventId: 2,
			Version: 2,
		},
	}
	eS2 := []*historypb.HistoryEvent{
		{
			EventId: 3,
			Version: 2,
		},
	}

	eS3 := []*historypb.HistoryEvent{
		{
			EventId: 4,
			Version: 2,
		},
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

	// first slice invalid (event id mismatch within slice)
	bad := []*historypb.HistoryEvent{{EventId: 1, Version: 2}, {EventId: 3, Version: 2}}
	v, err = validateEventsSlice(bad)
	s.Equal(int64(0), v)
	s.Equal(ErrEventIDMismatch, err)

	// second slice invalid (internal event id mismatch)
	v, err = validateEventsSlice(eS1, []*historypb.HistoryEvent{{EventId: 3, Version: 2}, {EventId: 5, Version: 2}})
	s.Equal(int64(0), v)
	s.Equal(ErrEventIDMismatch, err)

	// version mismatch between slices
	v, err = validateEventsSlice(eS1, []*historypb.HistoryEvent{{EventId: 3, Version: 99}})
	s.Equal(int64(0), v)
	s.Equal(ErrEventVersionMismatch, err)
}

func (s *replicationTaskSuite) TestValidateEvents() {
	eS1 := []*historypb.HistoryEvent{
		{
			EventId: 1,
			Version: 2,
		},
		{
			EventId: 2,
			Version: 2,
		},
	}

	eS2 := []*historypb.HistoryEvent{
		{
			EventId: 1,
			Version: 2,
		},
		{
			EventId: 3,
			Version: 2,
		},
	}

	eS3 := []*historypb.HistoryEvent{
		{
			EventId: 1,
			Version: 1,
		},
		{
			EventId: 2,
			Version: 2,
		},
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
		{
			EventId: 11,
		},
		{
			EventId: 12,
		},
	}
	slice2 := []*historypb.HistoryEvent{
		{
			EventId: 13,
		},
		{
			EventId: 14,
		},
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
	s.Equal(int64(13), task.getFirstEvent().EventId)
	s.Equal(int64(14), task.getLastEvent().EventId)
}

func (s *replicationTaskSuite) TestSkipDuplicatedEvents_InvalidInput_ErrorOut() {
	workflowKey := definition.WorkflowKey{
		WorkflowID: uuid.NewString(),
		RunID:      uuid.NewString(),
	}
	slice1 := []*historypb.HistoryEvent{
		{
			EventId: 11,
		},
		{
			EventId: 12,
		},
	}
	slice2 := []*historypb.HistoryEvent{
		{
			EventId: 13,
		},
		{
			EventId: 14,
		},
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

	err = task.skipDuplicatedEvents(-1)
	s.Error(err)
}

func (s *replicationTaskSuite) TestSkipDuplicatedEvents_WithNewEvents() {
	workflowKey := definition.WorkflowKey{
		WorkflowID: uuid.NewString(),
		RunID:      uuid.NewString(),
	}
	newEventTime := time.Now().UTC()
	slice1 := []*historypb.HistoryEvent{{EventId: 11, Version: 1}}
	slice2 := []*historypb.HistoryEvent{{EventId: 12, Version: 1}}
	newEvents := []*historypb.HistoryEvent{
		{EventId: 1, Version: 1, EventTime: timestamp.TimePtr(newEventTime)},
	}
	task := s.replTaskNewTask(
		workflowKey,
		nil,
		nil,
		[][]*historypb.HistoryEvent{slice1, slice2},
		newEvents,
		uuid.NewString(),
		nil,
		false,
	)
	err := task.skipDuplicatedEvents(1)
	s.NoError(err)
	s.Equal(newEventTime, task.getEventTime())
}

func (s *replicationTaskSuite) TestSkipDuplicatedEvents_ZeroInput_DoNothing() {
	workflowKey := definition.WorkflowKey{
		WorkflowID: uuid.NewString(),
		RunID:      uuid.NewString(),
	}
	slice1 := []*historypb.HistoryEvent{
		{
			EventId: 11,
		},
		{
			EventId: 12,
		},
	}
	slice2 := []*historypb.HistoryEvent{
		{
			EventId: 13,
		},
		{
			EventId: 14,
		},
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
		{
			EventId:   13,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED,
		},
		{
			EventId: 14,
		},
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

func (s *replicationTaskSuite) replTaskNewTask(
	workflowKey definition.WorkflowKey,
	baseInfo *workflowspb.BaseExecutionInfo,
	versionHistoryItems []*historyspb.VersionHistoryItem,
	eventsSlice [][]*historypb.HistoryEvent,
	newEvents []*historypb.HistoryEvent,
	newRunID string,
	versionedTransition *persistencespb.VersionedTransition,
	isStateBased bool,
) *replicationTaskImpl {
	task, err := newReplicationTask(
		s.clusterMetadata,
		nil,
		workflowKey,
		baseInfo,
		versionHistoryItems,
		eventsSlice,
		newEvents,
		newRunID,
		versionedTransition,
		isStateBased,
	)
	s.NoError(err)
	return task
}

func (s *replicationTaskSuite) TestGetters() {
	workflowKey := definition.WorkflowKey{
		NamespaceID: uuid.NewString(),
		WorkflowID:  uuid.NewString(),
		RunID:       uuid.NewString(),
	}
	eventTime := time.Now().UTC()
	slice1 := []*historypb.HistoryEvent{
		{
			EventId:   11,
			Version:   7,
			EventTime: timestamp.TimePtr(eventTime),
		},
		{
			EventId: 12,
			Version: 7,
		},
	}
	newEvents := []*historypb.HistoryEvent{
		{
			EventId: 1,
			Version: 7,
		},
	}
	versionedTransition := &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: 7,
		TransitionCount:          3,
	}
	versionHistoryItems := []*historyspb.VersionHistoryItem{{EventId: 12, Version: 7}}
	newRunID := uuid.NewString()

	task := s.replTaskNewTask(
		workflowKey,
		nil,
		versionHistoryItems,
		[][]*historypb.HistoryEvent{slice1},
		newEvents,
		newRunID,
		versionedTransition,
		true,
	)

	s.Equal(workflowKey.NamespaceID, task.getNamespaceID().String())
	s.Equal(workflowKey.WorkflowID, task.getWorkflowID())
	s.Equal(workflowKey.RunID, task.getRunID())
	s.Equal(&commonpb.WorkflowExecution{
		WorkflowId: workflowKey.WorkflowID,
		RunId:      workflowKey.RunID,
	}, task.getExecution())
	s.Equal(int64(7), task.getVersion())
	s.Equal("some random cluster name", task.getSourceCluster())
	s.Equal(eventTime, task.getEventTime())
	s.Equal(newEvents, task.getNewEvents())
	s.Equal(newRunID, task.getNewRunID())
	s.Equal(versionedTransition, task.getVersionedTransition())
	s.NotNil(task.getVersionHistory())
	s.Equal(versionHistoryItems, task.getVersionHistory().Items)
	s.True(task.stateBased())
	s.NotNil(task.getLogger())
}

func (s *replicationTaskSuite) TestIsWorkflowReset_True() {
	workflowKey := definition.WorkflowKey{
		WorkflowID: uuid.NewString(),
		RunID:      uuid.NewString(),
	}
	slice1 := []*historypb.HistoryEvent{
		{
			EventId: 10,
			Version: 1,
		},
	}
	baseInfo := &workflowspb.BaseExecutionInfo{
		LowestCommonAncestorEventId: 9,
	}
	task := s.replTaskNewTask(
		workflowKey,
		baseInfo,
		nil,
		[][]*historypb.HistoryEvent{slice1},
		nil,
		"",
		nil,
		false,
	)
	s.Equal(baseInfo, task.getBaseWorkflowInfo())
	s.True(task.isWorkflowReset())
}

func (s *replicationTaskSuite) TestNewReplicationTask_BackwardCompatNewRunID() {
	workflowKey := definition.WorkflowKey{
		WorkflowID: uuid.NewString(),
		RunID:      uuid.NewString(),
	}
	expectedNewRunID := uuid.NewString()
	slice1 := []*historypb.HistoryEvent{
		{
			EventId:   10,
			Version:   1,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{
				WorkflowExecutionContinuedAsNewEventAttributes: &historypb.WorkflowExecutionContinuedAsNewEventAttributes{
					NewExecutionRunId: expectedNewRunID,
				},
			},
		},
	}
	newEvents := []*historypb.HistoryEvent{
		{
			EventId: 1,
			Version: 1,
		},
	}
	// newRunID empty -> derived from continued-as-new attributes
	task := s.replTaskNewTask(
		workflowKey,
		nil,
		nil,
		[][]*historypb.HistoryEvent{slice1},
		newEvents,
		"",
		nil,
		false,
	)
	s.Equal(expectedNewRunID, task.getNewRunID())
}

func (s *replicationTaskSuite) TestNewReplicationTask_BackwardCompatNewRunID_Error() {
	workflowKey := definition.WorkflowKey{
		WorkflowID: uuid.NewString(),
		RunID:      uuid.NewString(),
	}
	slice1 := []*historypb.HistoryEvent{
		{
			EventId:   10,
			Version:   1,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		},
	}
	newEvents := []*historypb.HistoryEvent{
		{
			EventId: 1,
			Version: 1,
		},
	}
	_, err := newReplicationTask(
		s.clusterMetadata,
		nil,
		workflowKey,
		nil,
		nil,
		[][]*historypb.HistoryEvent{slice1},
		newEvents,
		"",
		nil,
		false,
	)
	s.Equal(ErrNoNewRunID, err)
}

func (s *replicationTaskSuite) TestSplitTask_Success() {
	workflowKey := definition.WorkflowKey{
		NamespaceID: uuid.NewString(),
		WorkflowID:  uuid.NewString(),
		RunID:       uuid.NewString(),
	}
	slice1 := []*historypb.HistoryEvent{
		{
			EventId: 10,
			Version: 5,
		},
	}
	newEventTime := time.Now().UTC()
	newEvents := []*historypb.HistoryEvent{
		{
			EventId:   1,
			Version:   5,
			EventTime: timestamp.TimePtr(newEventTime),
		},
		{
			EventId: 2,
			Version: 5,
		},
	}
	newRunID := uuid.NewString()
	task := s.replTaskNewTask(
		workflowKey,
		nil,
		nil,
		[][]*historypb.HistoryEvent{slice1},
		newEvents,
		newRunID,
		nil,
		true,
	)

	currentTask, newRunTask, err := task.splitTask()
	s.NoError(err)
	s.Equal(task, currentTask)
	s.Empty(currentTask.getNewEvents())
	s.Empty(currentTask.getNewRunID())

	s.Equal(newRunID, newRunTask.getRunID())
	s.Equal(workflowKey.WorkflowID, newRunTask.getWorkflowID())
	s.Equal(newEvents, newRunTask.getEvents()[0])
	s.Equal(int64(1), newRunTask.getFirstEvent().GetEventId())
	s.Equal(int64(2), newRunTask.getLastEvent().GetEventId())
	s.Equal(newEventTime, newRunTask.getEventTime())
	s.True(newRunTask.stateBased())
	s.NotNil(newRunTask.getVersionedTransition())
	s.Equal(int64(5), newRunTask.getVersionedTransition().NamespaceFailoverVersion)
	s.Nil(newRunTask.getBaseWorkflowInfo())
}

func (s *replicationTaskSuite) TestSplitTask_NoNewEvents() {
	workflowKey := definition.WorkflowKey{
		WorkflowID: uuid.NewString(),
		RunID:      uuid.NewString(),
	}
	slice1 := []*historypb.HistoryEvent{{EventId: 10, Version: 5}}
	task := s.replTaskNewTask(
		workflowKey,
		nil,
		nil,
		[][]*historypb.HistoryEvent{slice1},
		nil,
		"",
		nil,
		false,
	)
	_, _, err := task.splitTask()
	s.Equal(ErrNoNewRunHistory, err)
}

func (s *replicationTaskSuite) TestSplitTask_NoNewRunID() {
	workflowKey := definition.WorkflowKey{
		WorkflowID: uuid.NewString(),
		RunID:      uuid.NewString(),
	}
	slice1 := []*historypb.HistoryEvent{
		{
			EventId:   10,
			Version:   5,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{
				WorkflowExecutionContinuedAsNewEventAttributes: &historypb.WorkflowExecutionContinuedAsNewEventAttributes{
					NewExecutionRunId: "", // empty so derived run id stays empty
				},
			},
		},
	}
	newEvents := []*historypb.HistoryEvent{{EventId: 1, Version: 5}}
	// Force a task with newEvents but empty newRunID by bypassing constructor validation.
	task := &replicationTaskImpl{
		workflowKey: workflowKey,
		version:     5,
		firstEvent:  slice1[0],
		lastEvent:   slice1[0],
		events:      [][]*historypb.HistoryEvent{slice1},
		newEvents:   newEvents,
		newRunID:    "",
	}
	_, _, err := task.splitTask()
	s.Equal(ErrNoNewRunID, err)
}

func (s *replicationTaskSuite) TestNewReplicationTaskFromBatch_Success() {
	workflowKey := definition.WorkflowKey{
		WorkflowID: uuid.NewString(),
		RunID:      uuid.NewString(),
	}
	slice1 := []*historypb.HistoryEvent{
		{EventId: 1, Version: 3},
		{EventId: 2, Version: 3},
	}
	newEvents := []*historypb.HistoryEvent{
		{EventId: 1, Version: 3},
	}
	task, err := newReplicationTaskFromBatch(
		s.clusterMetadata,
		nil,
		workflowKey,
		nil,
		nil,
		[][]*historypb.HistoryEvent{slice1},
		newEvents,
		uuid.NewString(),
		nil,
		false,
	)
	s.NoError(err)
	s.NotNil(task)
}

func (s *replicationTaskSuite) TestNewReplicationTaskFromBatch_EmptyEventsSlice() {
	_, err := newReplicationTaskFromBatch(
		s.clusterMetadata,
		nil,
		definition.WorkflowKey{},
		nil,
		nil,
		[][]*historypb.HistoryEvent{},
		nil,
		"",
		nil,
		false,
	)
	s.Equal(consts.ErrEmptyHistoryRawEventBatch, err)
}

func (s *replicationTaskSuite) TestNewReplicationTaskFromBatch_InvalidEventsSlice() {
	slice1 := []*historypb.HistoryEvent{
		{EventId: 1, Version: 3},
		{EventId: 3, Version: 3}, // gap -> ErrEventIDMismatch
	}
	_, err := newReplicationTaskFromBatch(
		s.clusterMetadata,
		nil,
		definition.WorkflowKey{},
		nil,
		nil,
		[][]*historypb.HistoryEvent{slice1},
		nil,
		"",
		nil,
		false,
	)
	s.Equal(ErrEventIDMismatch, err)
}

func (s *replicationTaskSuite) TestNewReplicationTaskFromBatch_InvalidNewEvents() {
	slice1 := []*historypb.HistoryEvent{{EventId: 1, Version: 3}}
	newEvents := []*historypb.HistoryEvent{
		{EventId: 1, Version: 3},
		{EventId: 3, Version: 3}, // gap
	}
	_, err := newReplicationTaskFromBatch(
		s.clusterMetadata,
		nil,
		definition.WorkflowKey{},
		nil,
		nil,
		[][]*historypb.HistoryEvent{slice1},
		newEvents,
		"",
		nil,
		false,
	)
	s.Equal(ErrEventIDMismatch, err)
}

func (s *replicationTaskSuite) TestNewReplicationTaskFromBatch_NewEventsVersionMismatch() {
	slice1 := []*historypb.HistoryEvent{{EventId: 1, Version: 3}}
	newEvents := []*historypb.HistoryEvent{{EventId: 1, Version: 4}}
	_, err := newReplicationTaskFromBatch(
		s.clusterMetadata,
		nil,
		definition.WorkflowKey{},
		nil,
		nil,
		[][]*historypb.HistoryEvent{slice1},
		newEvents,
		"",
		nil,
		false,
	)
	s.Equal(ErrEventVersionMismatch, err)
}

func (s *replicationTaskSuite) replTaskSerialize(events []*historypb.HistoryEvent) *commonpb.DataBlob {
	serializer := serialization.NewSerializer()
	blob, err := serializer.SerializeEvents(events)
	s.NoError(err)
	return blob
}

func (s *replicationTaskSuite) TestNewReplicationTaskFromRequest_Success() {
	events := []*historypb.HistoryEvent{
		{EventId: 1, Version: 3},
		{EventId: 2, Version: 3},
	}
	newEvents := []*historypb.HistoryEvent{
		{EventId: 1, Version: 3},
	}
	request := &historyservice.ReplicateEventsV2Request{
		NamespaceId: uuid.NewString(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: uuid.NewString(),
			RunId:      uuid.NewString(),
		},
		Events:       s.replTaskSerialize(events),
		NewRunEvents: s.replTaskSerialize(newEvents),
		NewRunId:     uuid.NewString(),
	}
	task, err := newReplicationTaskFromRequest(
		s.clusterMetadata,
		serialization.NewSerializer(),
		nil,
		request,
	)
	s.NoError(err)
	s.NotNil(task)
	s.Len(task.getNewEvents(), 1)
}

func (s *replicationTaskSuite) TestNewReplicationTaskFromRequest_Error() {
	request := &historyservice.ReplicateEventsV2Request{
		NamespaceId: "not-a-uuid",
	}
	_, err := newReplicationTaskFromRequest(
		s.clusterMetadata,
		serialization.NewSerializer(),
		nil,
		request,
	)
	s.Equal(ErrInvalidNamespaceID, err)
}

func (s *replicationTaskSuite) TestValidateReplicateEventsRequest() {
	serializer := serialization.NewSerializer()
	validExec := &commonpb.WorkflowExecution{
		WorkflowId: uuid.NewString(),
		RunId:      uuid.NewString(),
	}
	events := []*historypb.HistoryEvent{{EventId: 1, Version: 3}}
	eventsBlob := s.replTaskSerialize(events)

	// invalid namespace id
	_, _, err := validateReplicateEventsRequest(serializer, &historyservice.ReplicateEventsV2Request{
		NamespaceId: "bad",
	})
	s.Equal(ErrInvalidNamespaceID, err)

	// nil execution
	_, _, err = validateReplicateEventsRequest(serializer, &historyservice.ReplicateEventsV2Request{
		NamespaceId: uuid.NewString(),
	})
	s.Equal(ErrInvalidExecution, err)

	// invalid run id
	_, _, err = validateReplicateEventsRequest(serializer, &historyservice.ReplicateEventsV2Request{
		NamespaceId: uuid.NewString(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: uuid.NewString(),
			RunId:      "bad",
		},
	})
	s.Equal(ErrInvalidRunID, err)

	// empty events (nil blob)
	_, _, err = validateReplicateEventsRequest(serializer, &historyservice.ReplicateEventsV2Request{
		NamespaceId:       uuid.NewString(),
		WorkflowExecution: validExec,
	})
	s.Equal(consts.ErrEmptyHistoryRawEventBatch, err)

	// malformed events blob -> deserialize error
	_, _, err = validateReplicateEventsRequest(serializer, &historyservice.ReplicateEventsV2Request{
		NamespaceId:       uuid.NewString(),
		WorkflowExecution: validExec,
		Events: &commonpb.DataBlob{
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			Data:         []byte("garbage"),
		},
	})
	s.Error(err)

	// malformed new run events blob -> deserialize error
	_, _, err = validateReplicateEventsRequest(serializer, &historyservice.ReplicateEventsV2Request{
		NamespaceId:       uuid.NewString(),
		WorkflowExecution: validExec,
		Events:            eventsBlob,
		NewRunEvents: &commonpb.DataBlob{
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			Data:         []byte("garbage"),
		},
	})
	s.Error(err)

	// events deserialize OK but invalid (event id mismatch)
	_, _, err = validateReplicateEventsRequest(serializer, &historyservice.ReplicateEventsV2Request{
		NamespaceId:       uuid.NewString(),
		WorkflowExecution: validExec,
		Events: s.replTaskSerialize([]*historypb.HistoryEvent{
			{EventId: 1, Version: 3},
			{EventId: 3, Version: 3},
		}),
	})
	s.Equal(ErrEventIDMismatch, err)

	// new run events deserialize OK but invalid
	_, _, err = validateReplicateEventsRequest(serializer, &historyservice.ReplicateEventsV2Request{
		NamespaceId:       uuid.NewString(),
		WorkflowExecution: validExec,
		Events:            eventsBlob,
		NewRunEvents: s.replTaskSerialize([]*historypb.HistoryEvent{
			{EventId: 1, Version: 3},
			{EventId: 3, Version: 3},
		}),
	})
	s.Equal(ErrEventIDMismatch, err)

	// no new run events -> returns events, nil
	evts, newEvts, err := validateReplicateEventsRequest(serializer, &historyservice.ReplicateEventsV2Request{
		NamespaceId:       uuid.NewString(),
		WorkflowExecution: validExec,
		Events:            eventsBlob,
	})
	s.NoError(err)
	s.Len(evts, 1)
	s.Nil(newEvts)

	// new run events version mismatch
	_, _, err = validateReplicateEventsRequest(serializer, &historyservice.ReplicateEventsV2Request{
		NamespaceId:       uuid.NewString(),
		WorkflowExecution: validExec,
		Events:            eventsBlob,
		NewRunEvents:      s.replTaskSerialize([]*historypb.HistoryEvent{{EventId: 1, Version: 99}}),
	})
	s.Equal(ErrEventVersionMismatch, err)

	// happy path with new run events
	evts, newEvts, err = validateReplicateEventsRequest(serializer, &historyservice.ReplicateEventsV2Request{
		NamespaceId:       uuid.NewString(),
		WorkflowExecution: validExec,
		Events:            eventsBlob,
		NewRunEvents:      s.replTaskSerialize([]*historypb.HistoryEvent{{EventId: 1, Version: 3}}),
	})
	s.NoError(err)
	s.Len(evts, 1)
	s.Len(newEvts, 1)
}

func (s *replicationTaskSuite) TestValidateUUID() {
	s.True(validateUUID(uuid.NewString()))
	s.False(validateUUID("not-a-uuid"))
}

func (s *replicationTaskSuite) TestDeserializeBlob() {
	serializer := serialization.NewSerializer()
	// nil blob
	events, err := deserializeBlob(serializer, nil)
	s.NoError(err)
	s.Nil(events)

	// valid blob, with encoding defaulting
	blob := s.replTaskSerialize([]*historypb.HistoryEvent{{EventId: 1, Version: 3}})
	blob.EncodingType = enumspb.ENCODING_TYPE_UNSPECIFIED
	events, err = deserializeBlob(serializer, blob)
	s.NoError(err)
	s.Len(events, 1)
}

func (s *replicationTaskSuite) TestDeserializeBlobs() {
	serializer := serialization.NewSerializer()
	// nil blobs
	batches, err := DeserializeBlobs(serializer, nil)
	s.NoError(err)
	s.Empty(batches)

	// valid blobs
	blob1 := s.replTaskSerialize([]*historypb.HistoryEvent{{EventId: 1, Version: 3}})
	blob1.EncodingType = enumspb.ENCODING_TYPE_UNSPECIFIED
	blob2 := s.replTaskSerialize([]*historypb.HistoryEvent{{EventId: 2, Version: 3}})
	batches, err = DeserializeBlobs(serializer, []*commonpb.DataBlob{blob1, blob2})
	s.NoError(err)
	s.Len(batches, 2)

	// invalid blob -> error
	_, err = DeserializeBlobs(serializer, []*commonpb.DataBlob{{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         []byte("garbage"),
	}})
	s.Error(err)
}
