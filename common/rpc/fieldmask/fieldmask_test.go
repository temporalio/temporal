package fieldmask

import (
	"github.com/stretchr/testify/assert"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"testing"
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
	mask := &fieldmaskpb.FieldMask{Paths: []string{"versioning_behavior_override"}}
	opts := &workflowpb.WorkflowExecutionOptions{}

	// Merge unpinned into empty options
	opts, err := MergeOptions(mask, unpinnedOverrideOptions, opts)
	if err != nil {
		t.Error(err)
	}
	assert.True(t, proto.Equal(unpinnedOverrideOptions, opts))

	// Merge pinned_A into unpinned options
	opts, err = MergeOptions(mask, pinnedOverrideOptionsA, opts)
	if err != nil {
		t.Error(err)
	}
	assert.True(t, proto.Equal(pinnedOverrideOptionsA, opts))

	// Merge pinned_B into pinned_A options
	opts, err = MergeOptions(mask, pinnedOverrideOptionsB, opts)
	if err != nil {
		t.Error(err)
	}
	assert.True(t, proto.Equal(pinnedOverrideOptionsB, opts))

	// Unset versioning override
	opts, err = MergeOptions(mask, emptyOptions, opts)
	if err != nil {
		t.Error(err)
	}
	assert.True(t, proto.Equal(emptyOptions, opts))
}

func TestMergeOptions_AsteriskMask(t *testing.T) {
	mask := &fieldmaskpb.FieldMask{Paths: []string{"*"}}
	opts := &workflowpb.WorkflowExecutionOptions{}

	_, err := MergeOptions(mask, unpinnedOverrideOptions, opts)
	assert.Error(t, err)
}

func TestMergeOptions_FooMask(t *testing.T) {
	mask := &fieldmaskpb.FieldMask{Paths: []string{"foo"}}
	opts := &workflowpb.WorkflowExecutionOptions{}

	_, err := MergeOptions(mask, unpinnedOverrideOptions, opts)
	assert.Error(t, err)
}

func TestMergeOptions_PartialUpdateMask(t *testing.T) {
	mask := &fieldmaskpb.FieldMask{Paths: []string{"versioning_behavior_override.behavior"}}
	opts := &workflowpb.WorkflowExecutionOptions{}

	_, err := MergeOptions(mask, unpinnedOverrideOptions, opts)
	assert.Error(t, err)
}

func TestMergeOptions_NilMask(t *testing.T) {
	opts := &workflowpb.WorkflowExecutionOptions{}

	_, err := MergeOptions(nil, unpinnedOverrideOptions, opts)
	assert.Error(t, err)
}

func TestMergeOptions_EmptyPathsMask(t *testing.T) {
	mask := &fieldmaskpb.FieldMask{Paths: []string{}}
	opts := &workflowpb.WorkflowExecutionOptions{}

	opts, err := MergeOptions(mask, unpinnedOverrideOptions, opts)
	assert.NoError(t, err)
	assert.True(t, proto.Equal(emptyOptions, opts))
}

func TestMergeOptions_InvalidBehaviorOverride(t *testing.T) {
	invalidOptions := &workflowpb.WorkflowExecutionOptions{
		VersioningBehaviorOverride: &commonpb.VersioningBehaviorOverride{
			Behavior:         enumspb.VERSIONING_BEHAVIOR_PINNED,
			WorkerDeployment: nil,
		},
	}
	// invalid options with a mask that writes that option errors
	_, err := MergeOptions(
		&fieldmaskpb.FieldMask{Paths: []string{"versioning_behavior_override"}},
		invalidOptions,
		&workflowpb.WorkflowExecutionOptions{},
	)
	assert.Error(t, err)

	// invalid options with a mask that does not write that option succeeds
	opts, err := MergeOptions(
		&fieldmaskpb.FieldMask{Paths: []string{}},
		invalidOptions,
		&workflowpb.WorkflowExecutionOptions{},
	)
	assert.NoError(t, err)
	assert.True(t, proto.Equal(emptyOptions, opts))
}
