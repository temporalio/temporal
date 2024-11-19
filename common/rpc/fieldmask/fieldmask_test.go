package fieldmask

import (
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
)

var (
	emptyOptions            = &workflowpb.WorkflowExecutionOptions{}
	unpinnedOverrideOptions = &workflowpb.WorkflowExecutionOptions{
		VersioningBehaviorOverride: &commonpb.VersioningBehaviorOverride{
			Behavior: enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE,
		},
	}
	pinnedOverrideOptionsA = &workflowpb.WorkflowExecutionOptions{
		VersioningBehaviorOverride: &commonpb.VersioningBehaviorOverride{
			Behavior:         enumspb.VERSIONING_BEHAVIOR_PINNED,
			WorkerDeployment: &commonpb.WorkerDeployment{DeploymentName: "X", BuildId: "A"},
		},
	}
	pinnedOverrideOptionsB = &workflowpb.WorkflowExecutionOptions{
		VersioningBehaviorOverride: &commonpb.VersioningBehaviorOverride{
			Behavior:         enumspb.VERSIONING_BEHAVIOR_PINNED,
			WorkerDeployment: &commonpb.WorkerDeployment{DeploymentName: "X", BuildId: "B"},
		},
	}
)

func TestMergeOptions_VersionOverrideMask(t *testing.T) {
	paths := []string{"versioning_behavior_override"}
	opts := &workflowpb.WorkflowExecutionOptions{}

	// Merge unpinned into empty options
	opts, err := MergeOptions(paths, unpinnedOverrideOptions, opts)
	if err != nil {
		t.Error(err)
	}
	assert.True(t, proto.Equal(unpinnedOverrideOptions, opts))

	// Merge pinned_A into unpinned options
	opts, err = MergeOptions(paths, pinnedOverrideOptionsA, opts)
	if err != nil {
		t.Error(err)
	}
	assert.True(t, proto.Equal(pinnedOverrideOptionsA, opts))

	// Merge pinned_B into pinned_A options
	opts, err = MergeOptions(paths, pinnedOverrideOptionsB, opts)
	if err != nil {
		t.Error(err)
	}
	assert.True(t, proto.Equal(pinnedOverrideOptionsB, opts))

	// Unset versioning override
	opts, err = MergeOptions(paths, emptyOptions, opts)
	if err != nil {
		t.Error(err)
	}
	assert.True(t, proto.Equal(emptyOptions, opts))
}

func TestMergeOptions_AsteriskMask(t *testing.T) {
	_, err := MergeOptions([]string{"*"}, unpinnedOverrideOptions, &workflowpb.WorkflowExecutionOptions{})
	assert.Error(t, err)
}

func TestMergeOptions_FooMask(t *testing.T) {
	_, err := MergeOptions([]string{"foo"}, unpinnedOverrideOptions, &workflowpb.WorkflowExecutionOptions{})
	assert.Error(t, err)
}

func TestMergeOptions_PartialUpdateMask(t *testing.T) {
	_, err := MergeOptions([]string{"versioning_behavior_override.behavior"}, unpinnedOverrideOptions, &workflowpb.WorkflowExecutionOptions{})
	assert.Error(t, err)
}

func TestMergeOptions_EmptyPathsMask(t *testing.T) {
	opts, err := MergeOptions([]string{}, unpinnedOverrideOptions, &workflowpb.WorkflowExecutionOptions{})
	assert.NoError(t, err)
	assert.True(t, proto.Equal(emptyOptions, opts))
}
