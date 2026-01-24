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
	var validVersionHistoryForRebuild = historyspb.VersionHistories_builder{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			historyspb.VersionHistory_builder{
				BranchToken: []byte("valid-branch-token"),
				Items: []*historyspb.VersionHistoryItem{
					historyspb.VersionHistoryItem_builder{
						EventId: 10,
						Version: 1,
					}.Build(),
				},
			}.Build(),
		},
	}.Build()
	var validWorkflowChasmNodes = map[string]*persistencespb.ChasmNode{
		"": persistencespb.ChasmNode_builder{
			Metadata: persistencespb.ChasmNodeMetadata_builder{
				ComponentAttributes: persistencespb.ChasmComponentAttributes_builder{
					TypeId: uint32(chasm.WorkflowArchetypeID),
				}.Build(),
			}.Build(),
		}.Build(),
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
			mutableState: persistencespb.WorkflowMutableState_builder{
				ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
					VersionHistories: nil,
				}.Build(),
				ChasmNodes: validWorkflowChasmNodes,
			}.Build(),
			expectError:   true,
			errorContains: "version histories is nil, cannot be rebuilt",
		},
		{
			name: "workflow archetype - should pass",
			mutableState: persistencespb.WorkflowMutableState_builder{
				ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
					VersionHistories: validVersionHistoryForRebuild,
				}.Build(),
				ChasmNodes: validWorkflowChasmNodes,
			}.Build(),
			expectError: false,
		},
		{
			name: "unspecified archetype - should fail",
			mutableState: persistencespb.WorkflowMutableState_builder{
				ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
					VersionHistories: validVersionHistoryForRebuild,
				}.Build(),
				ChasmNodes: map[string]*persistencespb.ChasmNode{
					"": persistencespb.ChasmNode_builder{
						Metadata: persistencespb.ChasmNodeMetadata_builder{
							ComponentAttributes: persistencespb.ChasmComponentAttributes_builder{
								TypeId: uint32(chasm.UnspecifiedArchetypeID),
							}.Build(),
						}.Build(),
					}.Build(),
				},
			}.Build(),
			expectError:   true,
			errorContains: "only supports workflow executions",
		},
		{
			name: "scheduler archetype - should fail",
			mutableState: persistencespb.WorkflowMutableState_builder{
				ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
					VersionHistories: validVersionHistoryForRebuild,
				}.Build(),
				ChasmNodes: map[string]*persistencespb.ChasmNode{
					"": persistencespb.ChasmNode_builder{
						Metadata: persistencespb.ChasmNodeMetadata_builder{
							ComponentAttributes: persistencespb.ChasmComponentAttributes_builder{
								TypeId: uint32(chasm.SchedulerArchetypeID),
							}.Build(),
						}.Build(),
					}.Build(),
				},
			}.Build(),
			expectError:   true,
			errorContains: "only supports workflow executions",
		},
		{
			name: "empty chasm nodes - should pass (old workflow format)",
			mutableState: persistencespb.WorkflowMutableState_builder{
				ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
					VersionHistories: validVersionHistoryForRebuild,
				}.Build(),
				ChasmNodes: map[string]*persistencespb.ChasmNode{},
			}.Build(),
			expectError: false,
		},
		{
			name: "nil chasm nodes - should pass (old workflow format)",
			mutableState: persistencespb.WorkflowMutableState_builder{
				ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
					VersionHistories: validVersionHistoryForRebuild,
				}.Build(),
				ChasmNodes: nil,
			}.Build(),
			expectError: false,
		},
		{
			name: "chasm nodes without root node - should fail",
			mutableState: persistencespb.WorkflowMutableState_builder{
				ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
					VersionHistories: validVersionHistoryForRebuild,
				}.Build(),
				ChasmNodes: map[string]*persistencespb.ChasmNode{
					"some-other-key": persistencespb.ChasmNode_builder{
						Metadata: &persistencespb.ChasmNodeMetadata{},
					}.Build(),
				},
			}.Build(),
			expectError:   true,
			errorContains: "only supports workflow executions",
		},
		{
			name: "root node with no component attributes - should fail",
			mutableState: persistencespb.WorkflowMutableState_builder{
				ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
					VersionHistories: validVersionHistoryForRebuild,
				}.Build(),
				ChasmNodes: map[string]*persistencespb.ChasmNode{
					"": persistencespb.ChasmNode_builder{
						Metadata: persistencespb.ChasmNodeMetadata_builder{
							Attributes: nil,
						}.Build(),
					}.Build(),
				},
			}.Build(),
			expectError:   true,
			errorContains: "only supports workflow executions",
		},
		{
			name: "root node with no metadata - should fail",
			mutableState: persistencespb.WorkflowMutableState_builder{
				ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
					VersionHistories: validVersionHistoryForRebuild,
				}.Build(),
				ChasmNodes: map[string]*persistencespb.ChasmNode{
					"": persistencespb.ChasmNode_builder{
						Metadata: nil,
					}.Build(),
				},
			}.Build(),
			expectError:   true,
			errorContains: "only supports workflow executions",
		},
		{
			name: "other archetype ID (e.g., PayloadStore) - should fail",
			mutableState: persistencespb.WorkflowMutableState_builder{
				ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
					VersionHistories: validVersionHistoryForRebuild,
				}.Build(),
				ChasmNodes: map[string]*persistencespb.ChasmNode{
					"": persistencespb.ChasmNode_builder{
						Metadata: persistencespb.ChasmNodeMetadata_builder{
							ComponentAttributes: persistencespb.ChasmComponentAttributes_builder{
								TypeId: uint32(chasm.SchedulerArchetypeID),
							}.Build(),
						}.Build(),
					}.Build(),
				},
			}.Build(),
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
