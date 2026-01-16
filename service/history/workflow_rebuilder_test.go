package history

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
)

func TestWorkflowRebuilderImpl_RebuildableCheck(t *testing.T) {
	var validVersionHistoryForRebuild = &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("valid-branch-token"),
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: 10,
						Version: 1,
					},
				},
			},
		},
	}
	var validWorkflowChasmNodes = map[string]*persistencespb.ChasmNode{
		"": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						TypeId: uint32(chasm.WorkflowArchetypeID),
					},
				},
			},
		},
	}
	rebuilder := &workflowRebuilderImpl{
		logger: log.NewTestLogger(),
	}

	tests := []struct {
		name          string
		mutableState  *persistencespb.WorkflowMutableState
		expectError   bool
		errorContains string
	}{
		{
			name: "nil version histories - should fail",
			mutableState: &persistencespb.WorkflowMutableState{
				ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
					VersionHistories: nil,
				},
				ChasmNodes: validWorkflowChasmNodes,
			},
			expectError:   true,
			errorContains: "version histories is nil, cannot be rebuilt",
		},
		{
			name: "workflow archetype - should pass",
			mutableState: &persistencespb.WorkflowMutableState{
				ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
					VersionHistories: validVersionHistoryForRebuild,
				},
				ChasmNodes: validWorkflowChasmNodes,
			},
			expectError: false,
		},
		{
			name: "unspecified archetype - should fail",
			mutableState: &persistencespb.WorkflowMutableState{
				ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
					VersionHistories: validVersionHistoryForRebuild,
				},
				ChasmNodes: map[string]*persistencespb.ChasmNode{
					"": {
						Metadata: &persistencespb.ChasmNodeMetadata{
							Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
								ComponentAttributes: &persistencespb.ChasmComponentAttributes{
									TypeId: uint32(chasm.UnspecifiedArchetypeID),
								},
							},
						},
					},
				},
			},
			expectError:   true,
			errorContains: "only supports workflow executions",
		},
		{
			name: "scheduler archetype - should fail",
			mutableState: &persistencespb.WorkflowMutableState{
				ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
					VersionHistories: validVersionHistoryForRebuild,
				},
				ChasmNodes: map[string]*persistencespb.ChasmNode{
					"": {
						Metadata: &persistencespb.ChasmNodeMetadata{
							Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
								ComponentAttributes: &persistencespb.ChasmComponentAttributes{
									TypeId: uint32(chasm.SchedulerArchetypeID),
								},
							},
						},
					},
				},
			},
			expectError:   true,
			errorContains: "only supports workflow executions",
		},
		{
			name: "empty chasm nodes - should pass (old workflow format)",
			mutableState: &persistencespb.WorkflowMutableState{
				ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
					VersionHistories: validVersionHistoryForRebuild,
				},
				ChasmNodes: map[string]*persistencespb.ChasmNode{},
			},
			expectError: false,
		},
		{
			name: "nil chasm nodes - should pass (old workflow format)",
			mutableState: &persistencespb.WorkflowMutableState{
				ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
					VersionHistories: validVersionHistoryForRebuild,
				},
				ChasmNodes: nil,
			},
			expectError: false,
		},
		{
			name: "chasm nodes without root node - should fail",
			mutableState: &persistencespb.WorkflowMutableState{
				ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
					VersionHistories: validVersionHistoryForRebuild,
				},
				ChasmNodes: map[string]*persistencespb.ChasmNode{
					"some-other-key": {
						Metadata: &persistencespb.ChasmNodeMetadata{},
					},
				},
			},
			expectError:   true,
			errorContains: "only supports workflow executions",
		},
		{
			name: "root node with no component attributes - should fail",
			mutableState: &persistencespb.WorkflowMutableState{
				ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
					VersionHistories: validVersionHistoryForRebuild,
				},
				ChasmNodes: map[string]*persistencespb.ChasmNode{
					"": {
						Metadata: &persistencespb.ChasmNodeMetadata{
							Attributes: nil,
						},
					},
				},
			},
			expectError:   true,
			errorContains: "only supports workflow executions",
		},
		{
			name: "root node with no metadata - should fail",
			mutableState: &persistencespb.WorkflowMutableState{
				ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
					VersionHistories: validVersionHistoryForRebuild,
				},
				ChasmNodes: map[string]*persistencespb.ChasmNode{
					"": {
						Metadata: nil,
					},
				},
			},
			expectError:   true,
			errorContains: "only supports workflow executions",
		},
		{
			name: "other archetype ID (e.g., PayloadStore) - should fail",
			mutableState: &persistencespb.WorkflowMutableState{
				ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
					VersionHistories: validVersionHistoryForRebuild,
				},
				ChasmNodes: map[string]*persistencespb.ChasmNode{
					"": {
						Metadata: &persistencespb.ChasmNodeMetadata{
							Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
								ComponentAttributes: &persistencespb.ChasmComponentAttributes{
									TypeId: uint32(chasm.SchedulerArchetypeID),
								},
							},
						},
					},
				},
			},
			expectError:   true,
			errorContains: "only supports workflow executions",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := rebuilder.rebuildableCheck(tt.mutableState)
			if tt.expectError {
				require.Error(t, err)
				if tt.errorContains != "" {
					require.Contains(t, err.Error(), tt.errorContains)
				}
				require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
			} else {
				require.NoError(t, err)
			}
		})
	}
}
