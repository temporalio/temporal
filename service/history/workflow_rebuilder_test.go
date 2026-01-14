package history

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
)

func TestWorkflowRebuilderImpl_RebuildableCheck(t *testing.T) {
	rebuilder := &workflowRebuilderImpl{}

	tests := []struct {
		name          string
		mutableState  *persistencespb.WorkflowMutableState
		expectError   bool
		errorContains string
	}{
		{
			name: "workflow archetype - should pass",
			mutableState: &persistencespb.WorkflowMutableState{
				ChasmNodes: map[string]*persistencespb.ChasmNode{
					"": {
						Metadata: &persistencespb.ChasmNodeMetadata{
							Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
								ComponentAttributes: &persistencespb.ChasmComponentAttributes{
									TypeId: uint32(chasm.WorkflowArchetypeID),
								},
							},
						},
					},
				},
			},
			expectError: false,
		},
		{
			// maki, todo: unclear logic
			name: "unspecified archetype - should pass",
			mutableState: &persistencespb.WorkflowMutableState{
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
			expectError: true,
		},
		{
			name: "scheduler archetype - should fail",
			mutableState: &persistencespb.WorkflowMutableState{
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
				ChasmNodes: map[string]*persistencespb.ChasmNode{},
			},
			expectError: false,
		},
		{
			name: "nil chasm nodes - should pass (old workflow format)",
			mutableState: &persistencespb.WorkflowMutableState{
				ChasmNodes: nil,
			},
			expectError: false,
		},
		{
			// maki, todo: unclear logic
			// should return a different error type? like data error?
			name: "chasm nodes without root node - should pass",
			mutableState: &persistencespb.WorkflowMutableState{
				ChasmNodes: map[string]*persistencespb.ChasmNode{
					"some-other-key": {
						Metadata: &persistencespb.ChasmNodeMetadata{},
					},
				},
			},
			expectError: true,
		},
		{
			// maki, todo: unclear logic
			// should return a different error type?
			name: "no component attributes - should fail",
			mutableState: &persistencespb.WorkflowMutableState{
				ChasmNodes: map[string]*persistencespb.ChasmNode{
					"": {
						Metadata: &persistencespb.ChasmNodeMetadata{
							Attributes: nil,
						},
					},
				},
			},
			expectError: true,
		},
		{
			name: "no metadata - should pass",
			mutableState: &persistencespb.WorkflowMutableState{
				ChasmNodes: map[string]*persistencespb.ChasmNode{
					"": {
						Metadata: nil,
					},
				},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := rebuilder.rebuildableCheck(tt.mutableState)
			if tt.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errorContains)
				var invalidArgErr *serviceerror.InvalidArgument
				require.ErrorAs(t, err, &invalidArgErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
