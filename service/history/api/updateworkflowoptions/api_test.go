package updateworkflowoptions

import (
	"testing"

	"github.com/stretchr/testify/assert"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

var (
	emptyOptions            = &workflowpb.WorkflowExecutionOptions{}
	unpinnedOverrideOptions = &workflowpb.WorkflowExecutionOptions{
		VersioningOverride: &workflowpb.VersioningOverride{
			Behavior: enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE,
		},
	}
	pinnedOverrideOptionsA = &workflowpb.WorkflowExecutionOptions{
		VersioningOverride: &workflowpb.VersioningOverride{
			Behavior:      enumspb.VERSIONING_BEHAVIOR_PINNED,
			PinnedVersion: "X.A",
		},
	}
	pinnedOverrideOptionsB = &workflowpb.WorkflowExecutionOptions{
		VersioningOverride: &workflowpb.VersioningOverride{
			Behavior:      enumspb.VERSIONING_BEHAVIOR_PINNED,
			PinnedVersion: "X.B",
		},
	}
)

func TestMergeOptions_VersionOverrideMask(t *testing.T) {
	updateMask := &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}}
	input := emptyOptions

	// Merge unpinned into empty options
	merged, err := mergeWorkflowExecutionOptions(input, unpinnedOverrideOptions, updateMask)
	if err != nil {
		t.Error(err)
	}
	assert.EqualExportedValues(t, unpinnedOverrideOptions, merged)

	// Merge pinned_A into unpinned options
	merged, err = mergeWorkflowExecutionOptions(input, pinnedOverrideOptionsA, updateMask)
	if err != nil {
		t.Error(err)
	}
	assert.EqualExportedValues(t, pinnedOverrideOptionsA, merged)

	// Merge pinned_B into pinned_A options
	merged, err = mergeWorkflowExecutionOptions(input, pinnedOverrideOptionsB, updateMask)
	if err != nil {
		t.Error(err)
	}
	assert.EqualExportedValues(t, pinnedOverrideOptionsB, merged)

	// Unset versioning override
	merged, err = mergeWorkflowExecutionOptions(input, emptyOptions, updateMask)
	if err != nil {
		t.Error(err)
	}
	assert.EqualExportedValues(t, emptyOptions, merged)
}

func TestMergeOptions_PartialMask(t *testing.T) {
	bothUpdateMask := &fieldmaskpb.FieldMask{Paths: []string{"versioning_override.behavior", "versioning_override.deployment"}}
	behaviorOnlyUpdateMask := &fieldmaskpb.FieldMask{Paths: []string{"versioning_override.behavior"}}
	deploymentOnlyUpdateMask := &fieldmaskpb.FieldMask{Paths: []string{"versioning_override.deployment"}}

	_, err := mergeWorkflowExecutionOptions(emptyOptions, unpinnedOverrideOptions, behaviorOnlyUpdateMask)
	assert.Error(t, err)

	_, err = mergeWorkflowExecutionOptions(emptyOptions, unpinnedOverrideOptions, deploymentOnlyUpdateMask)
	assert.Error(t, err)

	merged, err := mergeWorkflowExecutionOptions(emptyOptions, unpinnedOverrideOptions, bothUpdateMask)
	assert.NoError(t, err)
	assert.EqualExportedValues(t, unpinnedOverrideOptions, merged)
}

func TestMergeOptions_EmptyMask(t *testing.T) {
	emptyUpdateMask := &fieldmaskpb.FieldMask{Paths: []string{}}
	input := pinnedOverrideOptionsB

	// Don't merge anything
	merged, err := mergeWorkflowExecutionOptions(input, pinnedOverrideOptionsA, emptyUpdateMask)
	assert.NoError(t, err)
	assert.EqualExportedValues(t, input, merged)

	// Don't merge anything
	merged, err = mergeWorkflowExecutionOptions(input, nil, emptyUpdateMask)
	assert.NoError(t, err)
	assert.EqualExportedValues(t, input, merged)
}

func TestMergeOptions_AsteriskMask(t *testing.T) {
	asteriskUpdateMask := &fieldmaskpb.FieldMask{Paths: []string{"*"}}
	_, err := mergeWorkflowExecutionOptions(emptyOptions, unpinnedOverrideOptions, asteriskUpdateMask)
	assert.Error(t, err)
}

func TestMergeOptions_FooMask(t *testing.T) {
	fooUpdateMask := &fieldmaskpb.FieldMask{Paths: []string{"foo"}}
	_, err := mergeWorkflowExecutionOptions(emptyOptions, unpinnedOverrideOptions, fooUpdateMask)
	assert.Error(t, err)
}
