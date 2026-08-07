package api

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

func TestResolveDuplicateWorkflowStart(t *testing.T) {
	timeSource := clock.NewEventTimeSource()
	now := timeSource.Now()

	testCases := []struct {
		gracePeriod          time.Duration
		currentWorkflowStart time.Time
		expectError          bool
	}{
		{
			gracePeriod:          time.Duration(0 * time.Second),
			currentWorkflowStart: now,
			expectError:          false,
		},
		{
			gracePeriod:          time.Duration(1 * time.Second),
			currentWorkflowStart: now,
			expectError:          true,
		},
		{
			gracePeriod:          time.Duration(1 * time.Second),
			currentWorkflowStart: now.Add(-2 * time.Second),
			expectError:          false,
		},
	}

	config := tests.NewDynamicConfig()

	mockShard := shard.NewTestContextWithTimeSource(
		gomock.NewController(t),
		&persistencespb.ShardInfo{RangeId: 1},
		config,
		timeSource,
	)

	namespaceEntry := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{},
		&persistencespb.NamespaceConfig{},
		"target_cluster",
	)

	for _, tc := range testCases {
		config.WorkflowIdReuseMinimalInterval = dynamicconfig.GetDurationPropertyFnFilteredByNamespace(tc.gracePeriod)
		workflowKey := definition.WorkflowKey{
			NamespaceID: uuid.New().String(),
			WorkflowID:  "workflowID",
			RunID:       "oldRunID",
		}
		_, err := resolveDuplicateWorkflowStart(mockShard, tc.currentWorkflowStart, workflowKey, namespaceEntry, "newRunID", nil, false)

		if tc.expectError {
			assert.Error(t, err)
			var resourceErr *serviceerror.ResourceExhausted
			assert.ErrorAs(t, err, &resourceErr)

		} else {
			assert.NoError(t, err)
		}
	}
}

func TestZombifyConflictingChildAction(t *testing.T) {
	const (
		parentNamespaceID = "parent-namespace"
		parentWorkflowID  = "parent-workflow"
		parentRunID       = "parent-run"
		childRunID        = "child-run"
	)
	parentExecutionInfo := &workflowspb.ParentExecutionInfo{
		NamespaceId: parentNamespaceID,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: parentWorkflowID,
			RunId:      parentRunID,
		},
		InitiatedId:      75,
		InitiatedVersion: 925,
	}

	testCases := []struct {
		name            string
		running         bool
		parent          *workflowspb.ParentExecutionInfo
		mutateInfo      func(*persistencespb.WorkflowExecutionInfo)
		firstRunID      string
		conflictPolicy  enumspb.WorkflowIdConflictPolicy
		reusePolicy     enumspb.WorkflowIdReusePolicy
		expectCompleted bool
		expectConflict  bool
	}{
		{name: "same parent different initiation", running: true, parent: parentExecutionInfo},
		{
			name:           "unspecified reuse policy",
			running:        true,
			parent:         parentExecutionInfo,
			reusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_UNSPECIFIED,
			conflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
		},
		{
			name:           "reuse policy rejects duplicates",
			running:        true,
			parent:         parentExecutionInfo,
			reusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			expectConflict: true,
		},
		{
			name:           "reuse policy allows failed duplicates only",
			running:        true,
			parent:         parentExecutionInfo,
			reusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
			expectConflict: true,
		},
		{
			name:           "conflict policy would not fail the start",
			running:        true,
			parent:         parentExecutionInfo,
			conflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING,
			expectConflict: true,
		},
		{
			name:            "closed child",
			parent:          parentExecutionInfo,
			expectCompleted: true,
		},
		{name: "missing parent", running: true, expectConflict: true},
		{
			name:    "different parent namespace",
			running: true,
			parent:  parentExecutionInfo,
			mutateInfo: func(info *persistencespb.WorkflowExecutionInfo) {
				info.ParentNamespaceId = "other-namespace"
			},
			expectConflict: true,
		},
		{
			name:    "different parent workflow",
			running: true,
			parent:  parentExecutionInfo,
			mutateInfo: func(info *persistencespb.WorkflowExecutionInfo) {
				info.ParentWorkflowId = "other-workflow"
			},
			expectConflict: true,
		},
		{
			name:    "different parent run",
			running: true,
			parent:  parentExecutionInfo,
			mutateInfo: func(info *persistencespb.WorkflowExecutionInfo) {
				info.ParentRunId = "other-run"
			},
			expectConflict: true,
		},
		{
			name:    "exact initiation is not relinked or replaced",
			running: true,
			parent:  parentExecutionInfo,
			mutateInfo: func(info *persistencespb.WorkflowExecutionInfo) {
				info.ParentInitiatedId = parentExecutionInfo.GetInitiatedId()
				info.ParentInitiatedVersion = parentExecutionInfo.GetInitiatedVersion()
			},
			expectConflict: true,
		},
		{
			name:       "successor child run can be zombified",
			running:    true,
			parent:     parentExecutionInfo,
			firstRunID: "first-child-run",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			executionInfo := &persistencespb.WorkflowExecutionInfo{
				ParentNamespaceId:      parentNamespaceID,
				ParentWorkflowId:       parentWorkflowID,
				ParentRunId:            parentRunID,
				ParentInitiatedId:      72,
				ParentInitiatedVersion: 892,
			}
			if tc.mutateInfo != nil {
				tc.mutateInfo(executionInfo)
			}
			firstRunID := tc.firstRunID
			if firstRunID == "" {
				firstRunID = childRunID
			}
			executionState := &persistencespb.WorkflowExecutionState{
				RunId:               childRunID,
				FirstExecutionRunId: firstRunID,
			}
			workflowKey := definition.NewWorkflowKey("child-namespace", "child-workflow", childRunID)

			mutableState := historyi.NewMockMutableState(gomock.NewController(t))
			mutableState.EXPECT().IsWorkflowExecutionRunning().Return(tc.running)
			mutableState.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
			mutableState.EXPECT().GetExecutionState().Return(executionState).AnyTimes()
			mutableState.EXPECT().GetWorkflowKey().Return(workflowKey).AnyTimes()
			if !tc.expectCompleted && !tc.expectConflict {
				mutableState.EXPECT().UpdateWorkflowStateStatus(
					enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
					enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				).Return(true, nil)
			}

			conflictPolicy := tc.conflictPolicy
			if conflictPolicy == enumspb.WORKFLOW_ID_CONFLICT_POLICY_UNSPECIFIED {
				// What startChildWorkflow always sends on this path.
				conflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL
			}
			action, err := ZombifyConflictingChildAction(
				tc.parent, conflictPolicy, tc.reusePolicy, log.NewTestLogger(),
			)(
				NewWorkflowLease(nil, nil, mutableState),
			)
			switch {
			case tc.expectCompleted:
				require.ErrorIs(t, err, consts.ErrWorkflowCompleted)
			case tc.expectConflict:
				var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
				require.ErrorAs(t, err, &alreadyStarted)
			default:
				require.NoError(t, err)
				require.Same(t, UpdateWorkflowTerminate, action)
			}
		})
	}
}

func TestMigrateWorkflowIDReusePolicyForRunningWorkflow(t *testing.T) {
	//nolint:staticcheck // SA1019: intentional migration coverage for deprecated policy
	reusePolicy := enumspb.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING
	conflictPolicy := enumspb.WORKFLOW_ID_CONFLICT_POLICY_UNSPECIFIED

	MigrateWorkflowIDReusePolicyForRunningWorkflow(&reusePolicy, &conflictPolicy)

	require.Equal(t, enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE, reusePolicy)
	require.Equal(t, enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING, conflictPolicy)
}
