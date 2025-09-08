package tests

import (
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/testing/fakedata"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func RandomShardInfo(
	shardID int32,
	rangeID int64,
) *persistencespb.ShardInfo {
	var shardInfo persistencespb.ShardInfo
	_ = fakedata.FakeStruct(&shardInfo)
	shardInfo.ShardId = shardID
	shardInfo.RangeId = rangeID
	return &shardInfo
}

func RandomSnapshot(
	t *testing.T,
	namespaceID string,
	workflowID string,
	runID string,
	eventID int64,
	lastWriteVersion int64,
	state enumsspb.WorkflowExecutionState,
	status enumspb.WorkflowExecutionStatus,
	dbRecordVersion int64,
	branchToken []byte,
) (*p.WorkflowSnapshot, []*p.WorkflowEvents) {
	snapshot := &p.WorkflowSnapshot{
		ExecutionInfo:  RandomExecutionInfo(namespaceID, workflowID, eventID, lastWriteVersion, branchToken),
		ExecutionState: RandomExecutionState(runID, state, status, lastWriteVersion),

		NextEventID: eventID + 1, // NOTE: RandomSnapshot generates a single history event, hence NextEventID is plus 1

		ActivityInfos:       RandomInt64ActivityInfoMap(),
		TimerInfos:          RandomStringTimerInfoMap(),
		ChildExecutionInfos: RandomInt64ChildExecutionInfoMap(),
		RequestCancelInfos:  RandomInt64RequestCancelInfoMap(),
		SignalInfos:         RandomInt64SignalInfoMap(),
		SignalRequestedIDs:  map[string]struct{}{uuid.New().String(): {}},
		ChasmNodes:          RandomChasmNodeMap(),

		Tasks: map[tasks.Category][]tasks.Task{
			tasks.CategoryTransfer:    {},
			tasks.CategoryTimer:       {},
			tasks.CategoryReplication: {},
			tasks.CategoryVisibility:  {},
		},

		Condition:       rand.Int63(),
		DBRecordVersion: dbRecordVersion,
	}

	if branchToken == nil {
		return snapshot, nil
	}

	return snapshot, []*p.WorkflowEvents{{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		BranchToken: branchToken,
		Events:      []*historypb.HistoryEvent{RandomHistoryEvent(eventID, lastWriteVersion)},
	}}
}

func RandomMutation(
	t *testing.T,
	namespaceID string,
	workflowID string,
	runID string,
	eventID int64,
	lastWriteVersion int64,
	state enumsspb.WorkflowExecutionState,
	status enumspb.WorkflowExecutionStatus,
	dbRecordVersion int64,
	branchToken []byte,
) (*p.WorkflowMutation, []*p.WorkflowEvents) {
	mutation := &p.WorkflowMutation{
		ExecutionInfo:  RandomExecutionInfo(namespaceID, workflowID, eventID, lastWriteVersion, branchToken),
		ExecutionState: RandomExecutionState(runID, state, status, lastWriteVersion),

		NextEventID: eventID + 1, // NOTE: RandomMutation generates a single history event, hence NextEventID is plus 1

		UpsertActivityInfos:       RandomInt64ActivityInfoMap(),
		DeleteActivityInfos:       map[int64]struct{}{rand.Int63(): {}},
		UpsertTimerInfos:          RandomStringTimerInfoMap(),
		DeleteTimerInfos:          map[string]struct{}{uuid.New().String(): {}},
		UpsertChildExecutionInfos: RandomInt64ChildExecutionInfoMap(),
		DeleteChildExecutionInfos: map[int64]struct{}{rand.Int63(): {}},
		UpsertRequestCancelInfos:  RandomInt64RequestCancelInfoMap(),
		DeleteRequestCancelInfos:  map[int64]struct{}{rand.Int63(): {}},
		UpsertSignalInfos:         RandomInt64SignalInfoMap(),
		DeleteSignalInfos:         map[int64]struct{}{rand.Int63(): {}},
		UpsertSignalRequestedIDs:  map[string]struct{}{uuid.New().String(): {}},
		DeleteSignalRequestedIDs:  map[string]struct{}{uuid.New().String(): {}},
		UpsertChasmNodes:          RandomChasmNodeMap(),
		DeleteChasmNodes:          map[string]struct{}{uuid.New().String(): {}},
		// NewBufferedEvents: see below
		// ClearBufferedEvents: see below

		Tasks: map[tasks.Category][]tasks.Task{
			tasks.CategoryTransfer:    {},
			tasks.CategoryTimer:       {},
			tasks.CategoryReplication: {},
			tasks.CategoryVisibility:  {},
		},

		Condition:       rand.Int63(),
		DBRecordVersion: dbRecordVersion,
	}

	if branchToken == nil {
		return mutation, nil
	}

	switch rand.Int63() % 3 {
	case 0:
		mutation.ClearBufferedEvents = true
		mutation.NewBufferedEvents = nil
	case 1:
		mutation.ClearBufferedEvents = false
		mutation.NewBufferedEvents = nil
	case 2:
		mutation.ClearBufferedEvents = false
		mutation.NewBufferedEvents = []*historypb.HistoryEvent{RandomHistoryEvent(eventID, lastWriteVersion)}
	default:
		panic("broken test")
	}

	return mutation, []*p.WorkflowEvents{{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		BranchToken: branchToken,
		Events:      []*historypb.HistoryEvent{RandomHistoryEvent(eventID, lastWriteVersion)},
	}}
}

func RandomChasmNodeMap() map[string]*persistencespb.ChasmNode {
	return map[string]*persistencespb.ChasmNode{
		uuid.New().String(): RandomChasmNode(),
	}
}

func RandomChasmNode() *persistencespb.ChasmNode {
	// Some arbitrary random data to ensure the chasm node's attributes are preserved.
	var blobInfo persistencespb.WorkflowExecutionInfo
	_ = fakedata.FakeStruct(&blobInfo)
	blob, _ := serialization.ProtoEncode(&blobInfo)

	var versionedTransition persistencespb.VersionedTransition
	_ = fakedata.FakeStruct(&versionedTransition)

	return &persistencespb.ChasmNode{
		Metadata: &persistencespb.ChasmNodeMetadata{
			InitialVersionedTransition:    &versionedTransition,
			LastUpdateVersionedTransition: &versionedTransition,
			Attributes:                    &persistencespb.ChasmNodeMetadata_DataAttributes{},
		},
		Data: blob,
	}
}

func RandomExecutionInfo(
	namespaceID string,
	workflowID string,
	eventID int64,
	lastWriteVersion int64,
	branchToken []byte,
) *persistencespb.WorkflowExecutionInfo {
	var executionInfo persistencespb.WorkflowExecutionInfo
	_ = fakedata.FakeStruct(&executionInfo)
	executionInfo.NamespaceId = namespaceID
	executionInfo.WorkflowId = workflowID

	if branchToken != nil {
		executionInfo.VersionHistories = RandomVersionHistory(eventID, lastWriteVersion, branchToken)
	} else {
		executionInfo.VersionHistories = versionhistory.NewVersionHistories(&historyspb.VersionHistory{})
	}
	executionInfo.TransitionHistory = []*persistencespb.VersionedTransition{{
		NamespaceFailoverVersion: lastWriteVersion,
		TransitionCount:          rand.Int63(),
	}}
	return &executionInfo
}

func RandomExecutionState(
	runID string,
	state enumsspb.WorkflowExecutionState,
	status enumspb.WorkflowExecutionStatus,
	lastWriteVersion int64,
) *persistencespb.WorkflowExecutionState {
	createRequestID := uuid.NewString()
	return &persistencespb.WorkflowExecutionState{
		CreateRequestId: createRequestID,
		RunId:           runID,
		State:           state,
		Status:          status,
		LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: lastWriteVersion,
			TransitionCount:          rand.Int63(),
		},
		RequestIds: map[string]*persistencespb.RequestIDInfo{
			createRequestID: {
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				EventId:   common.FirstEventID,
			},
			uuid.NewString(): {
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
				EventId:   common.BufferedEventID,
			},
		},
	}
}

func RandomInt64ActivityInfoMap() map[int64]*persistencespb.ActivityInfo {
	return map[int64]*persistencespb.ActivityInfo{
		rand.Int63(): RandomActivityInfo(),
	}
}

func RandomStringTimerInfoMap() map[string]*persistencespb.TimerInfo {
	return map[string]*persistencespb.TimerInfo{
		uuid.New().String(): RandomTimerInfo(),
	}
}

func RandomInt64ChildExecutionInfoMap() map[int64]*persistencespb.ChildExecutionInfo {
	return map[int64]*persistencespb.ChildExecutionInfo{
		rand.Int63(): RandomChildExecutionInfo(),
	}
}

func RandomInt64RequestCancelInfoMap() map[int64]*persistencespb.RequestCancelInfo {
	return map[int64]*persistencespb.RequestCancelInfo{
		rand.Int63(): RandomRequestCancelInfo(),
	}
}

func RandomInt64SignalInfoMap() map[int64]*persistencespb.SignalInfo {
	return map[int64]*persistencespb.SignalInfo{
		rand.Int63(): RandomSignalInfo(),
	}
}

func RandomActivityInfo() *persistencespb.ActivityInfo {
	var activityInfo persistencespb.ActivityInfo
	_ = fakedata.FakeStruct(&activityInfo)
	return &activityInfo
}

func RandomTimerInfo() *persistencespb.TimerInfo {
	var timerInfo persistencespb.TimerInfo
	_ = fakedata.FakeStruct(&timerInfo)
	return &timerInfo
}

func RandomChildExecutionInfo() *persistencespb.ChildExecutionInfo {
	var childExecutionInfo persistencespb.ChildExecutionInfo
	_ = fakedata.FakeStruct(&childExecutionInfo)
	return &childExecutionInfo
}

func RandomRequestCancelInfo() *persistencespb.RequestCancelInfo {
	var requestCancelInfo persistencespb.RequestCancelInfo
	_ = fakedata.FakeStruct(&requestCancelInfo)
	return &requestCancelInfo
}

func RandomSignalInfo() *persistencespb.SignalInfo {
	var signalInfo persistencespb.SignalInfo
	_ = fakedata.FakeStruct(&signalInfo)
	return &signalInfo
}

func RandomHistoryEvent(eventID int64, version int64) *historypb.HistoryEvent {
	var historyEvent historypb.HistoryEvent
	_ = fakedata.FakeStruct(&historyEvent)
	historyEvent.EventId = eventID
	historyEvent.Version = version
	return &historyEvent
}

func RandomResetPoints() *workflowpb.ResetPoints {
	return &workflowpb.ResetPoints{Points: []*workflowpb.ResetPointInfo{{
		BinaryChecksum:               uuid.New().String(),
		RunId:                        uuid.New().String(),
		FirstWorkflowTaskCompletedId: rand.Int63(),
		CreateTime:                   RandomTime(),
		ExpireTime:                   RandomTime(),
		Resettable:                   rand.Int31()%2 == 0,
	}}}
}

func RandomStringPayloadMap() map[string]*commonpb.Payload {
	return map[string]*commonpb.Payload{
		uuid.New().String(): RandomPayload(),
	}
}

func RandomPayload() *commonpb.Payload {
	var payload commonpb.Payload
	_ = fakedata.FakeStruct(&payload)
	return &payload
}

func RandomVersionHistory(
	eventID int64,
	lastWriteVersion int64,
	branchToken []byte,
) *historyspb.VersionHistories {
	return &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{{
			BranchToken: branchToken,
			Items: []*historyspb.VersionHistoryItem{{
				EventId: eventID,
				Version: lastWriteVersion,
			}},
		}},
	}
}

func RandomBranchToken(
	namespaceID string,
	workflowID string,
	runID string,
	historyBranchUtil p.HistoryBranchUtil,
) []byte {
	branchToken, _ := historyBranchUtil.NewHistoryBranch(
		namespaceID,
		workflowID,
		runID,
		uuid.NewString(),
		nil,
		nil,
		0,
		0,
		0,
	)
	return branchToken
}

func RandomTime() *timestamppb.Timestamp {
	return timestamppb.New(time.Unix(0, rand.Int63()))
}

func RandomDuration() *durationpb.Duration {
	return durationpb.New(time.Duration(rand.Int63()))
}
