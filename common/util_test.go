// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package common

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/primitives/timestamp"
	"google.golang.org/protobuf/testing/protopack"
)

func TestIsContextDeadlineExceededErr(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Millisecond))
	defer cancel()
	require.True(t, IsContextDeadlineExceededErr(ctx.Err()))
	require.True(t, IsContextDeadlineExceededErr(serviceerror.NewDeadlineExceeded("something")))

	require.False(t, IsContextDeadlineExceededErr(errors.New("some random error")))

	ctx, cancel = context.WithCancel(context.Background())
	cancel()
	require.False(t, IsContextDeadlineExceededErr(ctx.Err()))
}

func TestIsContextCanceledErr(t *testing.T) {
	require.True(t, IsContextCanceledErr(serviceerror.NewCanceled("something")))
	require.False(t, IsContextCanceledErr(errors.New("some random error")))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.True(t, IsContextCanceledErr(ctx.Err()))
}

func TestOverrideWorkflowRunTimeout_InfiniteRunTimeout_InfiniteExecutionTimeout(t *testing.T) {
	runTimeout := time.Duration(0)
	executionTimeout := time.Duration(0)
	require.Equal(t, time.Duration(0), OverrideWorkflowRunTimeout(runTimeout, executionTimeout))
}

func TestOverrideWorkflowRunTimeout_FiniteRunTimeout_InfiniteExecutionTimeout(t *testing.T) {
	runTimeout := time.Duration(10)
	executionTimeout := time.Duration(0)
	require.Equal(t, time.Duration(10), OverrideWorkflowRunTimeout(runTimeout, executionTimeout))
}

func TestOverrideWorkflowRunTimeout_InfiniteRunTimeout_FiniteExecutionTimeout(t *testing.T) {
	runTimeout := time.Duration(0)
	executionTimeout := time.Duration(10)
	require.Equal(t, time.Duration(10), OverrideWorkflowRunTimeout(runTimeout, executionTimeout))
}

func TestOverrideWorkflowRunTimeout_FiniteRunTimeout_FiniteExecutionTimeout(t *testing.T) {
	runTimeout := time.Duration(100)
	executionTimeout := time.Duration(10)
	require.Equal(t, time.Duration(10), OverrideWorkflowRunTimeout(runTimeout, executionTimeout))

	runTimeout = time.Duration(10)
	executionTimeout = time.Duration(100)
	require.Equal(t, time.Duration(10), OverrideWorkflowRunTimeout(runTimeout, executionTimeout))
}

func TestOverrideWorkflowTaskTimeout_Infinite(t *testing.T) {
	taskTimeout := time.Duration(0)
	runTimeout := time.Duration(100)
	defaultTimeout := time.Duration(20)
	defaultTimeoutFn := dynamicconfig.GetDurationPropertyFnFilteredByNamespace(defaultTimeout)
	require.Equal(t, time.Duration(20), OverrideWorkflowTaskTimeout("random domain", taskTimeout, runTimeout, defaultTimeoutFn))

	taskTimeout = time.Duration(0)
	runTimeout = time.Duration(10)
	defaultTimeout = time.Duration(20)
	defaultTimeoutFn = dynamicconfig.GetDurationPropertyFnFilteredByNamespace(defaultTimeout)
	require.Equal(t, time.Duration(10), OverrideWorkflowTaskTimeout("random domain", taskTimeout, runTimeout, defaultTimeoutFn))

	taskTimeout = time.Duration(0)
	runTimeout = time.Duration(0)
	defaultTimeout = time.Duration(30)
	defaultTimeoutFn = dynamicconfig.GetDurationPropertyFnFilteredByNamespace(defaultTimeout)
	require.Equal(t, time.Duration(30), OverrideWorkflowTaskTimeout("random domain", taskTimeout, runTimeout, defaultTimeoutFn))

	taskTimeout = time.Duration(0)
	runTimeout = time.Duration(0)
	defaultTimeout = MaxWorkflowTaskStartToCloseTimeout + time.Duration(1)
	defaultTimeoutFn = dynamicconfig.GetDurationPropertyFnFilteredByNamespace(defaultTimeout)
	require.Equal(t, MaxWorkflowTaskStartToCloseTimeout, OverrideWorkflowTaskTimeout("random domain", taskTimeout, runTimeout, defaultTimeoutFn))
}

func TestOverrideWorkflowTaskTimeout_Finite(t *testing.T) {
	taskTimeout := time.Duration(10)
	runTimeout := MaxWorkflowTaskStartToCloseTimeout - time.Duration(1)
	defaultTimeout := time.Duration(20)
	defaultTimeoutFn := dynamicconfig.GetDurationPropertyFnFilteredByNamespace(defaultTimeout)
	require.Equal(t, time.Duration(10), OverrideWorkflowTaskTimeout("random domain", taskTimeout, runTimeout, defaultTimeoutFn))

	taskTimeout = MaxWorkflowTaskStartToCloseTimeout - time.Duration(1)
	runTimeout = time.Duration(10)
	defaultTimeout = time.Duration(20)
	defaultTimeoutFn = dynamicconfig.GetDurationPropertyFnFilteredByNamespace(defaultTimeout)
	require.Equal(t, time.Duration(10), OverrideWorkflowTaskTimeout("random domain", taskTimeout, runTimeout, defaultTimeoutFn))

	taskTimeout = time.Duration(10)
	runTimeout = MaxWorkflowTaskStartToCloseTimeout + time.Duration(1)
	defaultTimeout = time.Duration(20)
	defaultTimeoutFn = dynamicconfig.GetDurationPropertyFnFilteredByNamespace(defaultTimeout)
	require.Equal(t, time.Duration(10), OverrideWorkflowTaskTimeout("random domain", taskTimeout, runTimeout, defaultTimeoutFn))

	taskTimeout = MaxWorkflowTaskStartToCloseTimeout + time.Duration(1)
	runTimeout = MaxWorkflowTaskStartToCloseTimeout + time.Duration(1)
	defaultTimeout = time.Duration(20)
	defaultTimeoutFn = dynamicconfig.GetDurationPropertyFnFilteredByNamespace(defaultTimeout)
	require.Equal(t, MaxWorkflowTaskStartToCloseTimeout, OverrideWorkflowTaskTimeout("random domain", taskTimeout, runTimeout, defaultTimeoutFn))
}

func TestMapShardID_ByNamespaceWorkflow_4And16(t *testing.T) {
	namespaceID := uuid.New()
	workflowID := uuid.New()
	shardID4 := WorkflowIDToHistoryShard(namespaceID, workflowID, 4)
	shardID16 := WorkflowIDToHistoryShard(namespaceID, workflowID, 16)

	targetShardIDs := MapShardID(16, 4, shardID16)
	require.Equal(t, []int32{
		shardID4,
	}, targetShardIDs)

	targetShardIDs = MapShardID(4, 16, shardID4)
	found := false
	for _, targetShardID := range targetShardIDs {
		if shardID16 == targetShardID {
			found = true
			break
		}
	}
	require.True(t, found)
}

func TestMapShardID_1To4(t *testing.T) {
	sourceShardCount := int32(1)
	targetShardCount := int32(4)

	targetShards := MapShardID(sourceShardCount, targetShardCount, 1)
	require.Equal(t, []int32{
		1, 2, 3, 4,
	}, targetShards)
}

func TestMapShardID_4To1(t *testing.T) {
	sourceShardCount := int32(4)
	targetShardCount := int32(1)

	targetShards := MapShardID(sourceShardCount, targetShardCount, 4)
	require.Equal(t, []int32{1}, targetShards)

	targetShards = MapShardID(sourceShardCount, targetShardCount, 3)
	require.Equal(t, []int32{1}, targetShards)

	targetShards = MapShardID(sourceShardCount, targetShardCount, 2)
	require.Equal(t, []int32{1}, targetShards)

	targetShards = MapShardID(sourceShardCount, targetShardCount, 1)
	require.Equal(t, []int32{1}, targetShards)
}

func TestMapShardID_4To16(t *testing.T) {
	sourceShardCount := int32(4)
	targetShardCount := int32(16)

	targetShards := MapShardID(sourceShardCount, targetShardCount, 1)
	require.Equal(t, []int32{
		1, 5, 9, 13,
	}, targetShards)

	targetShards = MapShardID(sourceShardCount, targetShardCount, 2)
	require.Equal(t, []int32{
		2, 6, 10, 14,
	}, targetShards)

	targetShards = MapShardID(sourceShardCount, targetShardCount, 3)
	require.Equal(t, []int32{
		3, 7, 11, 15,
	}, targetShards)

	targetShards = MapShardID(sourceShardCount, targetShardCount, 4)
	require.Equal(t, []int32{
		4, 8, 12, 16,
	}, targetShards)
}

func TestMapShardID_16To4(t *testing.T) {
	sourceShardCount := int32(16)
	targetShardCount := int32(4)

	targetShards := MapShardID(sourceShardCount, targetShardCount, 16)
	require.Equal(t, []int32{4}, targetShards)

	targetShards = MapShardID(sourceShardCount, targetShardCount, 15)
	require.Equal(t, []int32{3}, targetShards)

	targetShards = MapShardID(sourceShardCount, targetShardCount, 14)
	require.Equal(t, []int32{2}, targetShards)

	targetShards = MapShardID(sourceShardCount, targetShardCount, 13)
	require.Equal(t, []int32{1}, targetShards)

	targetShards = MapShardID(sourceShardCount, targetShardCount, 12)
	require.Equal(t, []int32{4}, targetShards)

	targetShards = MapShardID(sourceShardCount, targetShardCount, 11)
	require.Equal(t, []int32{3}, targetShards)

	targetShards = MapShardID(sourceShardCount, targetShardCount, 10)
	require.Equal(t, []int32{2}, targetShards)

	targetShards = MapShardID(sourceShardCount, targetShardCount, 9)
	require.Equal(t, []int32{1}, targetShards)

	targetShards = MapShardID(sourceShardCount, targetShardCount, 8)
	require.Equal(t, []int32{4}, targetShards)

	targetShards = MapShardID(sourceShardCount, targetShardCount, 7)
	require.Equal(t, []int32{3}, targetShards)

	targetShards = MapShardID(sourceShardCount, targetShardCount, 6)
	require.Equal(t, []int32{2}, targetShards)

	targetShards = MapShardID(sourceShardCount, targetShardCount, 5)
	require.Equal(t, []int32{1}, targetShards)

	targetShards = MapShardID(sourceShardCount, targetShardCount, 4)
	require.Equal(t, []int32{4}, targetShards)

	targetShards = MapShardID(sourceShardCount, targetShardCount, 3)
	require.Equal(t, []int32{3}, targetShards)

	targetShards = MapShardID(sourceShardCount, targetShardCount, 2)
	require.Equal(t, []int32{2}, targetShards)

	targetShards = MapShardID(sourceShardCount, targetShardCount, 1)
	require.Equal(t, []int32{1}, targetShards)
}

func TestVerifyShardIDMapping_1VS4(t *testing.T) {
	require.NoError(t, VerifyShardIDMapping(1, 4, 1, 1))
	require.NoError(t, VerifyShardIDMapping(1, 4, 1, 2))
	require.NoError(t, VerifyShardIDMapping(1, 4, 1, 3))
	require.NoError(t, VerifyShardIDMapping(1, 4, 1, 4))
}

func TestVerifyShardIDMapping_2VS4(t *testing.T) {
	require.NoError(t, VerifyShardIDMapping(2, 4, 1, 1))
	require.Error(t, VerifyShardIDMapping(2, 4, 1, 2))
	require.NoError(t, VerifyShardIDMapping(2, 4, 1, 3))
	require.Error(t, VerifyShardIDMapping(2, 4, 1, 4))

	require.Error(t, VerifyShardIDMapping(2, 4, 2, 1))
	require.NoError(t, VerifyShardIDMapping(2, 4, 2, 2))
	require.Error(t, VerifyShardIDMapping(2, 4, 2, 3))
	require.NoError(t, VerifyShardIDMapping(2, 4, 2, 4))
}

func TestIsServiceClientTransientError_ResourceExhausted(t *testing.T) {
	require.False(t, IsServiceClientTransientError(
		&serviceerror.ResourceExhausted{
			Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT,
			Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
			Message: "Namespace RPS limit exceeded",
		},
	))
	require.False(t, IsServiceClientTransientError(
		&serviceerror.ResourceExhausted{
			Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT,
			Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
			Message: "Max number of conconcurrent pollers/updates/batch operation reached.",
		},
	))
	require.False(t, IsServiceClientTransientError(
		&serviceerror.ResourceExhausted{
			Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT,
			Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
			Message: "Namespace persistence RPS reached.",
		},
	))
	require.False(t, IsServiceClientTransientError(
		&serviceerror.ResourceExhausted{
			Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_BUSY_WORKFLOW,
			Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
			Message: "Workflow is busy.",
		},
	))
	require.False(t, IsServiceClientTransientError(
		&serviceerror.ResourceExhausted{
			Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT,
			Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
			Message: "APS limit exceeded",
		},
	))

	require.True(t, IsServiceClientTransientError(
		&serviceerror.ResourceExhausted{
			Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT,
			Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM,
			Message: "System level RPS limit exceeded",
		},
	))
	require.True(t, IsServiceClientTransientError(
		&serviceerror.ResourceExhausted{
			Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT,
			Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM,
			Message: "System level persistence RPS reached.",
		},
	))
	require.True(t, IsServiceClientTransientError(
		&serviceerror.ResourceExhausted{
			Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED,
			Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM,
			Message: "Mutable state cache is full",
		},
	))

}

func TestDiscardUnknownProto(t *testing.T) {
	msRecord := &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId: uuid.New(),
			WorkflowId:  uuid.New(),
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId: uuid.New(),
			State: enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		},
		TimerInfos: map[string]*persistencespb.TimerInfo{
			"timer1": {
				Version:        123,
				StartedEventId: 10,
			},
		},
		BufferedEvents: []*historypb.HistoryEvent{
			{
				EventId: -123,
				Version: 123,
				Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{
					ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
						ScheduledEventId: 14,
						StartedEventId:   15,
					},
				},
			},
		},
		NextEventId: 101,
	}

	data, err := msRecord.Marshal()
	require.NoError(t, err)

	// now add some unknown fields to the record
	msRecord.ProtoReflect().SetUnknown(
		protopack.Message{
			protopack.Tag{Number: 1000, Type: protopack.BytesType},
		}.Marshal(),
	)
	msRecord.ExecutionInfo.ProtoReflect().SetUnknown(
		protopack.Message{
			protopack.Int32(-1),
		}.Marshal(),
	)
	msRecord.TimerInfos["timer1"].ProtoReflect().SetUnknown(
		protopack.Message{
			protopack.String("unknown string"),
		}.Marshal(),
	)
	msRecord.BufferedEvents[0].ProtoReflect().SetUnknown(
		protopack.Message{
			protopack.Bool(true),
		}.Marshal(),
	)

	dataWithUnknown, err := msRecord.Marshal()
	require.NoError(t, err)
	require.NotEqual(t, data, dataWithUnknown)

	// discard unknown fields
	err = DiscardUnknownProto(msRecord)
	require.NoError(t, err)

	dataWithoutUnknown, err := msRecord.Marshal()
	require.NoError(t, err)
	require.Equal(t, data, dataWithoutUnknown)
}

func generateExecutionInfo() (a, b *persistencespb.WorkflowExecutionInfo) {
	a = &persistencespb.WorkflowExecutionInfo{
		NamespaceId:                             "deadbeef-0123-4567-890a-bcdef0123456",
		WorkflowId:                              "wId",
		TaskQueue:                               "testTaskQueue",
		WorkflowTypeName:                        "wType",
		WorkflowRunTimeout:                      timestamp.DurationPtr(time.Second * 200),
		DefaultWorkflowTaskTimeout:              timestamp.DurationPtr(time.Second * 100),
		LastCompletedWorkflowTaskStartedEventId: 99,
		WorkflowTaskVersion:                     1234,
		WorkflowTaskScheduledEventId:            101,
		WorkflowTaskStartedEventId:              102,
		WorkflowTaskTimeout:                     timestamp.DurationPtr(time.Second * 100),
		WorkflowTaskAttempt:                     1,
		WorkflowTaskType:                        enumsspb.WORKFLOW_TASK_TYPE_NORMAL,
		VersionHistories: &historyspb.VersionHistories{
			Histories: []*historyspb.VersionHistory{
				{
					BranchToken: []byte("token#1"),
					Items: []*historyspb.VersionHistoryItem{
						{EventId: 102, Version: 1234},
					},
				},
			},
		},
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 1234, TransitionCount: 1024},
			{TransitionCount: 1025},
		},
		SignalRequestIdsLastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1025},
	}

	b = &persistencespb.WorkflowExecutionInfo{
		NamespaceId:                             "deadbeef-0123-4567-890a-bcdef0123456",
		WorkflowId:                              "wId",
		TaskQueue:                               "testTaskQueue",
		WorkflowTypeName:                        "wType",
		WorkflowRunTimeout:                      timestamp.DurationPtr(time.Second * 200),
		DefaultWorkflowTaskTimeout:              timestamp.DurationPtr(time.Second*100 + time.Second),
		LastCompletedWorkflowTaskStartedEventId: 99,
		WorkflowTaskVersion:                     1234 + 1,
		WorkflowTaskScheduledEventId:            101 + 1,
		WorkflowTaskStartedEventId:              102 + 1,
		WorkflowTaskTimeout:                     timestamp.DurationPtr(time.Second * 100),
		WorkflowTaskAttempt:                     1,
		WorkflowTaskType:                        enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE,
		VersionHistories: &historyspb.VersionHistories{
			Histories: []*historyspb.VersionHistory{
				{
					BranchToken: []byte("token#1"),
					Items: []*historyspb.VersionHistoryItem{
						{EventId: 102, Version: 1234},
					},
				},
			},
		},
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 1234, TransitionCount: 1024},
			{TransitionCount: 1025},
		},
		SignalRequestIdsLastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1025},
	}
	return
}

func TestMergeProtoExcludingFields(t *testing.T) {
	source := &persistencespb.WorkflowExecutionInfo{
		NamespaceId: uuid.New(),
		WorkflowId:  uuid.New(),
	}

	target := &persistencespb.WorkflowExecutionInfo{
		NamespaceId: source.NamespaceId + "_target",
		WorkflowId:  source.WorkflowId + "_target",
	}

	doNotSync := func(v any) []interface{} {
		info, ok := v.(*persistencespb.WorkflowExecutionInfo)
		if !ok || info == nil {
			return nil
		}
		return []interface{}{
			&info.NamespaceId,
		}
	}

	err := MergeProtoExcludingFields(target, source, doNotSync)
	require.NoError(t, err)

	require.NotEqual(t, source.NamespaceId, target.NamespaceId)
	require.Equal(t, source.WorkflowId, target.WorkflowId)

	msRecord := &persistencespb.WorkflowMutableState{}
	err = MergeProtoExcludingFields(target, msRecord, doNotSync)
	require.Error(t, err)

	msRecord = &persistencespb.WorkflowMutableState{}
	err = MergeProtoExcludingFields(target, msRecord, doNotSync)
	require.Error(t, err)

	source, target = generateExecutionInfo()
	doNotSync = func(v any) []interface{} {
		info, ok := v.(*persistencespb.WorkflowExecutionInfo)
		if !ok || info == nil {
			return nil
		}
		return []interface{}{
			&info.WorkflowTaskVersion,
			&info.WorkflowTaskScheduledEventId,
			&info.WorkflowTaskStartedEventId,
			&info.WorkflowTaskRequestId,
			&info.WorkflowTaskTimeout,
			&info.WorkflowTaskAttempt,
			&info.WorkflowTaskStartedTime,
			&info.WorkflowTaskScheduledTime,
			&info.WorkflowTaskOriginalScheduledTime,
			&info.WorkflowTaskType,
			&info.WorkflowTaskSuggestContinueAsNew,
			&info.WorkflowTaskHistorySizeBytes,
			&info.WorkflowTaskBuildId,
			&info.WorkflowTaskBuildIdRedirectCounter,
			&info.VersionHistories,
			&info.ExecutionStats,
			&info.LastFirstEventTxnId,
			&info.ParentClock,
			&info.CloseTransferTaskId,
			&info.CloseVisibilityTaskId,
			&info.RelocatableAttributesRemoved,
			&info.WorkflowExecutionTimerTaskStatus,
			&info.SubStateMachinesByType,
			&info.StateMachineTimers,
			&info.TaskGenerationShardClockTimestamp,
			&info.UpdateInfos,
		}
	}
	err = MergeProtoExcludingFields(target, source, doNotSync)
	require.NoError(t, err)

	require.NotEqual(t, source.WorkflowTaskVersion, target.WorkflowTaskVersion)
	require.Equal(t, source.WorkflowId, target.WorkflowId)
}
