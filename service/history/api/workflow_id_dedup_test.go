package api

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow/update"
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

func TestOrphanedChildReplacementDoesNotReplaceUnrelatedWorkflow(t *testing.T) {
	parentExecutionInfo := &workflowspb.ParentExecutionInfo{
		NamespaceId: "parent-namespace",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "parent-workflow",
			RunId:      "parent-run",
		},
		InitiatedId:      75,
		InitiatedVersion: 925,
	}
	replacementInfo := &historyservice.OrphanedChildReplacementInfo{
		ParentCurrentVersionHistoryItems: []*historyspb.VersionHistoryItem{
			{EventId: 100, Version: 925},
		},
	}

	// A standalone workflow has no parent initiation to prove belongs to the requesting parent.
	require.False(t, isOrphanedChildOnLosingBranch(
		&persistencespb.WorkflowExecutionInfo{},
		parentExecutionInfo,
		replacementInfo,
	))
}

func TestOrphanedChildReplacementAcceptsUnstartedWorkflowTask(t *testing.T) {
	const childRunID = "child-run"
	controller := gomock.NewController(t)
	mutableState := historyi.NewMockMutableState(controller)
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		FirstExecutionRunId: childRunID,
		State:               enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
	})
	mutableState.EXPECT().GetWorkflowKey().Return(definition.NewWorkflowKey("namespace", "child", childRunID))
	mutableState.EXPECT().GetNextEventID().Return(common.FirstEventID + 2)
	mutableState.EXPECT().HasCompletedAnyWorkflowTask().Return(false)
	mutableState.EXPECT().GetPendingWorkflowTask().Return(&historyi.WorkflowTaskInfo{
		ScheduledEventID: common.FirstEventID + 1,
		StartedEventID:   common.EmptyEventID,
	})

	require.True(t, isOrphanedChildWithoutProgress(mutableState))
}

func TestReplaceOrphanedChildAction(t *testing.T) {
	const (
		parentNamespaceID = "parent-namespace"
		parentWorkflowID  = "parent-workflow"
		parentRunID       = "parent-run"
		childRunID        = "child-run"
		newRunID          = "replacement-run"
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
	validRecoveryInfo := &historyservice.OrphanedChildReplacementInfo{
		ParentCurrentVersionHistoryItems: []*historyspb.VersionHistoryItem{
			{EventId: 100, Version: 925},
		},
	}

	testCases := []struct {
		name            string
		closed          bool
		state           enumsspb.WorkflowExecutionState
		withoutParent   bool
		recoveryInfo    *historyservice.OrphanedChildReplacementInfo
		mutateInfo      func(*persistencespb.WorkflowExecutionInfo)
		firstRunID      string
		nextEventID     int64
		admittedUpdate  bool
		workflowTask    *historyi.WorkflowTaskInfo
		completedWFT    bool
		expectedOutcome string
	}{
		{name: "start-only child on losing branch"},
		{
			name:        "first workflow task scheduled but not started",
			state:       enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			nextEventID: common.FirstEventID + 2,
			workflowTask: &historyi.WorkflowTaskInfo{
				ScheduledEventID: common.FirstEventID + 1,
				StartedEventID:   common.EmptyEventID,
			},
		},
		{
			name:            "running without first workflow task",
			state:           enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			expectedOutcome: orphanedChildLocalProgress,
		},
		{
			name:        "first workflow task started",
			state:       enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			nextEventID: common.FirstEventID + 2,
			workflowTask: &historyi.WorkflowTaskInfo{
				ScheduledEventID: common.FirstEventID + 1,
				StartedEventID:   common.FirstEventID + 2,
			},
			expectedOutcome: orphanedChildLocalProgress,
		},
		{
			name:            "workflow task completed",
			state:           enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			nextEventID:     common.FirstEventID + 2,
			completedWFT:    true,
			expectedOutcome: orphanedChildLocalProgress,
		},
		{
			name:            "signal after first workflow task scheduled",
			state:           enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			nextEventID:     common.FirstEventID + 3,
			expectedOutcome: orphanedChildLocalProgress,
		},
		{
			name:            "created child has additional history",
			nextEventID:     common.FirstEventID + 2,
			expectedOutcome: orphanedChildLocalProgress,
		},
		{
			name:            "successor child run",
			firstRunID:      "first-child-run",
			expectedOutcome: orphanedChildLocalProgress,
		},
		{
			name:            "created child has admitted update",
			admittedUpdate:  true,
			expectedOutcome: orphanedChildLocalProgress,
		},
		{
			name: "existing initiation is on current branch",
			recoveryInfo: &historyservice.OrphanedChildReplacementInfo{
				ParentCurrentVersionHistoryItems: []*historyspb.VersionHistoryItem{
					{EventId: 74, Version: 892},
					{EventId: 100, Version: 925},
				},
			},
			expectedOutcome: orphanedChildNotLosingBranch,
		},
		{
			name:            "empty recovery info",
			recoveryInfo:    &historyservice.OrphanedChildReplacementInfo{},
			expectedOutcome: orphanedChildNotLosingBranch,
		},
		{
			name: "different parent",
			mutateInfo: func(info *persistencespb.WorkflowExecutionInfo) {
				info.ParentRunId = "other-parent-run"
			},
			expectedOutcome: orphanedChildNotLosingBranch,
		},
		{
			name:            "missing request parent",
			withoutParent:   true,
			expectedOutcome: orphanedChildNotLosingBranch,
		},
		{
			name: "same parent initiation",
			mutateInfo: func(info *persistencespb.WorkflowExecutionInfo) {
				info.ParentInitiatedId = parentExecutionInfo.GetInitiatedId()
				info.ParentInitiatedVersion = parentExecutionInfo.GetInitiatedVersion()
			},
			expectedOutcome: orphanedChildNotLosingBranch,
		},
		{
			name:            "closed after snapshot",
			closed:          true,
			expectedOutcome: orphanedChildRaceClosed,
		},
		{
			name: "zero-version branch prefix",
			recoveryInfo: &historyservice.OrphanedChildReplacementInfo{
				ParentCurrentVersionHistoryItems: []*historyspb.VersionHistoryItem{
					{EventId: 50, Version: common.EmptyVersion},
					{EventId: 100, Version: 925},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
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
			state := tc.state
			if state == enumsspb.WORKFLOW_EXECUTION_STATE_UNSPECIFIED {
				state = enumsspb.WORKFLOW_EXECUTION_STATE_CREATED
			}
			executionState := &persistencespb.WorkflowExecutionState{
				RunId:               childRunID,
				FirstExecutionRunId: firstRunID,
				State:               state,
			}
			workflowKey := definition.NewWorkflowKey("child-namespace", "child-workflow", childRunID)
			nextEventID := tc.nextEventID
			if nextEventID == 0 {
				nextEventID = common.FirstEventID + 1
			}
			controller := gomock.NewController(t)
			mutableState := historyi.NewMockMutableState(controller)
			mutableState.EXPECT().IsWorkflowExecutionRunning().Return(!tc.closed)
			mutableState.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
			mutableState.EXPECT().GetExecutionState().Return(executionState).AnyTimes()
			mutableState.EXPECT().GetWorkflowKey().Return(workflowKey).AnyTimes()
			mutableState.EXPECT().GetNextEventID().Return(nextEventID).AnyTimes()
			if state == enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING && nextEventID == common.FirstEventID+2 {
				mutableState.EXPECT().HasCompletedAnyWorkflowTask().Return(tc.completedWFT)
				if !tc.completedWFT {
					mutableState.EXPECT().GetPendingWorkflowTask().Return(tc.workflowTask)
				}
			}
			mutableState.EXPECT().GetCurrentVersion().Return(int64(1))
			mutableState.EXPECT().VisitUpdates(gomock.Any())
			updateRegistry := update.NewRegistry(mutableState)
			if tc.admittedUpdate {
				mutableState.EXPECT().GetUpdateOutcome(gomock.Any(), "update-id").Return(
					nil,
					serviceerror.NewNotFound("update not found"),
				)
				_, _, err := updateRegistry.FindOrCreate(ctx, "update-id")
				require.NoError(t, err)
			}
			workflowContext := historyi.NewMockWorkflowContext(controller)
			workflowContext.EXPECT().UpdateRegistry(gomock.Any()).Return(updateRegistry).AnyTimes()

			expectReplacement := tc.expectedOutcome == ""
			if expectReplacement {
				// An untouched child has no started workflow task to fail first.
				mutableState.EXPECT().GetStartedWorkflowTask().Return(nil)
				mutableState.EXPECT().AddWorkflowExecutionTerminatedEvent(
					terminateOrphanedChildReason,
					gomock.Any(),
					consts.IdentityHistoryService,
					false,
					nil,
				).Return(nil, nil)
			}

			metricsHandler := metricstest.NewCaptureHandler()
			capture := metricsHandler.StartCapture()
			defer metricsHandler.StopCapture(capture)
			recoveryInfo := tc.recoveryInfo
			if recoveryInfo == nil {
				recoveryInfo = validRecoveryInfo
			}
			parent := parentExecutionInfo
			if tc.withoutParent {
				parent = nil
			}
			action, err := ReplaceOrphanedChildAction(
				ctx,
				parent, recoveryInfo, newRunID,
				metricsHandler,
			)(
				NewWorkflowLease(workflowContext, nil, mutableState),
			)
			switch tc.expectedOutcome {
			case orphanedChildRaceClosed:
				require.ErrorIs(t, err, consts.ErrWorkflowCompleted)
			case "":
				require.NoError(t, err)
				require.Same(t, UpdateWorkflowTerminate, action)
			default:
				var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
				require.ErrorAs(t, err, &alreadyStarted)
			}

			recordings := capture.Snapshot()[metrics.OrphanedChildWorkflowReplacement.Name()]
			if expectReplacement {
				require.Empty(t, recordings, "success must not be recorded before persistence commits")
			} else {
				require.Len(t, recordings, 1)
				require.Equal(t, int64(1), recordings[0].Value)
				require.Equal(t, tc.expectedOutcome, recordings[0].Tags["outcome"])
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
