package getworkflowexecutionrawhistoryv2

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/api/adminservice/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/persistence/versionhistory"
)

// Test_SetRequestDefaultValueAndGetTargetVersionHistory_ExclusiveEndEventOnNonCurrentBranch
// tests the scenario where:
// - EndEventId is exclusive (this API uses exclusive-exclusive semantics)
// - The actual last event (EndEventId - 1) exists on a non-current branch
// - EndEventId itself doesn't exist at that version (because the branch ended before it)
func Test_SetRequestDefaultValueAndGetTargetVersionHistory_ExclusiveEndEventOnNonCurrentBranch(t *testing.T) {
	// Setup:
	// Branch 1 (non-current): events 1-17 at version 1
	// Branch 2 (current): events 1-16 at version 1, then events 17-19 at version 2
	//
	// Request asks for events up to 18 (exclusive) at version 1, i.e., events 1-17 at version 1.
	// Branch 1 should be selected since it contains event 17 at version 1.

	branch1Item := versionhistory.NewVersionHistoryItem(int64(17), int64(1))
	branch1 := versionhistory.NewVersionHistory([]byte("branch1-token"), []*historyspb.VersionHistoryItem{branch1Item})

	branch2Item1 := versionhistory.NewVersionHistoryItem(int64(16), int64(1))
	branch2Item2 := versionhistory.NewVersionHistoryItem(int64(19), int64(2))
	branch2 := versionhistory.NewVersionHistory([]byte("branch2-token"), []*historyspb.VersionHistoryItem{branch2Item1, branch2Item2})

	versionHistories := versionhistory.NewVersionHistories(branch1)
	_, _, err := versionhistory.AddAndSwitchVersionHistory(versionHistories, branch2)
	require.NoError(t, err)

	currentBranch, err := versionhistory.GetCurrentVersionHistory(versionHistories)
	require.NoError(t, err)
	lastItem, err := versionhistory.GetLastVersionHistoryItem(currentBranch)
	require.NoError(t, err)
	assert.Equal(t, int64(19), lastItem.GetEventId())
	assert.Equal(t, int64(2), lastItem.GetVersion())

	request := &historyservice.GetWorkflowExecutionRawHistoryV2Request{
		NamespaceId: uuid.NewString(),
		Request: &adminservice.GetWorkflowExecutionRawHistoryV2Request{
			NamespaceId: uuid.NewString(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: "test-workflow",
				RunId:      uuid.NewString(),
			},
			StartEventId:      0, // exclusive, means start from event 1
			StartEventVersion: 1,
			EndEventId:        18, // exclusive, means get up to event 17
			EndEventVersion:   1,
			MaximumPageSize:   100,
		},
	}

	targetVersionHistory, err := SetRequestDefaultValueAndGetTargetVersionHistory(
		request,
		versionHistories,
	)
	require.NoError(t, err)
	assert.Equal(t, branch1, targetVersionHistory)
}

// Test_SetRequestDefaultValueAndGetTargetVersionHistory_EndEventExistsAtDifferentVersionThanPrevEvent
// tests the scenario where EndEventId exists at EndEventVersion, but EndEventId-1 is at a different version.
// This ensures we use {EndEventId, EndEventVersion} directly and don't incorrectly fall back to {EndEventId-1, EndEventVersion}.
func Test_SetRequestDefaultValueAndGetTargetVersionHistory_EndEventExistsAtDifferentVersionThanPrevEvent(t *testing.T) {
	// Setup:
	// Branch 1: events 1-14 at version 22, events 15-20 at version 31
	// Branch 2 (current): events 1-14 at version 22, events 15-16 at version 32, event 17 at version 33
	//
	// Request: EndEventId=17, EndEventVersion=33
	// Event 17 IS at version 33, so {17, 33} should be found directly.
	// Event 16 is at version 32 (not 33), so {16, 33} would NOT be found.

	branch1Item1 := versionhistory.NewVersionHistoryItem(int64(14), int64(22))
	branch1Item2 := versionhistory.NewVersionHistoryItem(int64(20), int64(31))
	branch1 := versionhistory.NewVersionHistory([]byte("branch1-token"), []*historyspb.VersionHistoryItem{branch1Item1, branch1Item2})

	branch2Item1 := versionhistory.NewVersionHistoryItem(int64(14), int64(22))
	branch2Item2 := versionhistory.NewVersionHistoryItem(int64(16), int64(32))
	branch2Item3 := versionhistory.NewVersionHistoryItem(int64(17), int64(33))
	branch2 := versionhistory.NewVersionHistory([]byte("branch2-token"), []*historyspb.VersionHistoryItem{branch2Item1, branch2Item2, branch2Item3})

	versionHistories := versionhistory.NewVersionHistories(branch1)
	_, _, err := versionhistory.AddAndSwitchVersionHistory(versionHistories, branch2)
	require.NoError(t, err)

	request := &historyservice.GetWorkflowExecutionRawHistoryV2Request{
		NamespaceId: uuid.NewString(),
		Request: &adminservice.GetWorkflowExecutionRawHistoryV2Request{
			NamespaceId: uuid.NewString(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: "test-workflow",
				RunId:      uuid.NewString(),
			},
			StartEventId:      16,
			StartEventVersion: 31,
			EndEventId:        17,
			EndEventVersion:   33,
			MaximumPageSize:   100,
		},
	}

	targetVersionHistory, err := SetRequestDefaultValueAndGetTargetVersionHistory(
		request,
		versionHistories,
	)
	require.NoError(t, err)
	assert.Equal(t, branch2, targetVersionHistory)
}

// Test_SetRequestDefaultValueAndGetTargetVersionHistory_EndEventNotFound tests the case
// where neither {EndEventId, EndEventVersion} nor {EndEventId-1, EndEventVersion} exist.
func Test_SetRequestDefaultValueAndGetTargetVersionHistory_EndEventNotFound(t *testing.T) {
	// Branch has events 1-10 at version 1
	branch := versionhistory.NewVersionHistory([]byte("token"), []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(int64(10), int64(1)),
	})
	versionHistories := versionhistory.NewVersionHistories(branch)

	// Request events at version 2, which doesn't exist
	request := &historyservice.GetWorkflowExecutionRawHistoryV2Request{
		NamespaceId: uuid.NewString(),
		Request: &adminservice.GetWorkflowExecutionRawHistoryV2Request{
			NamespaceId: uuid.NewString(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: "test-workflow",
				RunId:      uuid.NewString(),
			},
			StartEventId:      0,
			StartEventVersion: 2,
			EndEventId:        5,
			EndEventVersion:   2,
			MaximumPageSize:   100,
		},
	}

	_, err := SetRequestDefaultValueAndGetTargetVersionHistory(
		request,
		versionHistories,
	)
	assert.Error(t, err)
}
