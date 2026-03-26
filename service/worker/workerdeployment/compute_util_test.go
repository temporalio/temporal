package workerdeployment

import (
	"testing"

	"github.com/stretchr/testify/assert"
	commonpb "go.temporal.io/api/common/v1"
	computepb "go.temporal.io/api/compute/v1"
	enumspb "go.temporal.io/api/enums/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestBuildUpdatedComputeConfig_UpsertNewGroupToEmptyConfig(t *testing.T) {
	t.Parallel()
	sg := &computepb.ComputeConfigScalingGroup{
		TaskQueueTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
		Provider:       &computepb.ComputeProvider{Type: "aws-lambda"},
	}
	args := &deploymentspb.UpdateVersionComputeConfigArgs{
		UpsertScalingGroups: map[string]*deploymentspb.ScalingGroupUpdate{
			"group1": {ScalingGroup: sg},
		},
	}

	result := buildUpdatedComputeConfig(nil, args)

	assert.Len(t, result.GetScalingGroups(), 1)
	assert.True(t, result.GetScalingGroups()["group1"].Equal(sg))
}

func TestBuildUpdatedComputeConfig_UpsertNewGroupToExistingConfig(t *testing.T) {
	t.Parallel()
	existing := &computepb.ComputeConfig{
		ScalingGroups: map[string]*computepb.ComputeConfigScalingGroup{
			"existing": {Provider: &computepb.ComputeProvider{Type: "existing-type"}},
		},
	}
	args := &deploymentspb.UpdateVersionComputeConfigArgs{
		UpsertScalingGroups: map[string]*deploymentspb.ScalingGroupUpdate{
			"new": {ScalingGroup: &computepb.ComputeConfigScalingGroup{
				Provider: &computepb.ComputeProvider{Type: "new-type"},
			}},
		},
	}

	result := buildUpdatedComputeConfig(existing, args)

	assert.Len(t, result.GetScalingGroups(), 2)
	assert.Equal(t, "existing-type", result.GetScalingGroups()["existing"].GetProvider().GetType())
	assert.Equal(t, "new-type", result.GetScalingGroups()["new"].GetProvider().GetType())
}

func TestBuildUpdatedComputeConfig_UpsertExistingGroupEmptyMask_NoOp(t *testing.T) {
	t.Parallel()
	existing := &computepb.ComputeConfig{
		ScalingGroups: map[string]*computepb.ComputeConfigScalingGroup{
			"group1": {
				Provider: &computepb.ComputeProvider{Type: "original"},
			},
		},
	}
	args := &deploymentspb.UpdateVersionComputeConfigArgs{
		UpsertScalingGroups: map[string]*deploymentspb.ScalingGroupUpdate{
			"group1": {
				ScalingGroup: &computepb.ComputeConfigScalingGroup{
					Provider: &computepb.ComputeProvider{Type: "should-not-apply"},
				},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{}},
			},
		},
	}

	result := buildUpdatedComputeConfig(existing, args)

	assert.Equal(t, "original", result.GetScalingGroups()["group1"].GetProvider().GetType())
}

func TestBuildUpdatedComputeConfig_UpsertExistingGroupWithFieldMask(t *testing.T) {
	t.Parallel()
	existing := &computepb.ComputeConfig{
		ScalingGroups: map[string]*computepb.ComputeConfigScalingGroup{
			"group1": {
				TaskQueueTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
				Provider:       &computepb.ComputeProvider{Type: "original-provider"},
				Scaler:         &computepb.ComputeScaler{Type: "original-scaler"},
			},
		},
	}
	args := &deploymentspb.UpdateVersionComputeConfigArgs{
		UpsertScalingGroups: map[string]*deploymentspb.ScalingGroupUpdate{
			"group1": {
				ScalingGroup: &computepb.ComputeConfigScalingGroup{
					Provider: &computepb.ComputeProvider{Type: "new-provider"},
					Scaler:   &computepb.ComputeScaler{Type: "new-scaler"},
				},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"provider"}},
			},
		},
	}

	result := buildUpdatedComputeConfig(existing, args)

	g := result.GetScalingGroups()["group1"]
	assert.Equal(t, "new-provider", g.GetProvider().GetType())
	assert.Equal(t, "original-scaler", g.GetScaler().GetType(), "scaler should be unchanged")
	assert.Equal(t, []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW}, g.GetTaskQueueTypes(), "task queue types should be unchanged")
}

func TestBuildUpdatedComputeConfig_RemoveExistingGroup(t *testing.T) {
	t.Parallel()
	existing := &computepb.ComputeConfig{
		ScalingGroups: map[string]*computepb.ComputeConfigScalingGroup{
			"group1": {Provider: &computepb.ComputeProvider{Type: "type1"}},
			"group2": {Provider: &computepb.ComputeProvider{Type: "type2"}},
		},
	}
	args := &deploymentspb.UpdateVersionComputeConfigArgs{
		RemoveScalingGroups: []string{"group1"},
	}

	result := buildUpdatedComputeConfig(existing, args)

	assert.Len(t, result.GetScalingGroups(), 1)
	assert.Nil(t, result.GetScalingGroups()["group1"])
	assert.Equal(t, "type2", result.GetScalingGroups()["group2"].GetProvider().GetType())
}

func TestBuildUpdatedComputeConfig_RemoveNonExistentGroup_NoOp(t *testing.T) {
	t.Parallel()
	existing := &computepb.ComputeConfig{
		ScalingGroups: map[string]*computepb.ComputeConfigScalingGroup{
			"group1": {Provider: &computepb.ComputeProvider{Type: "type1"}},
		},
	}
	args := &deploymentspb.UpdateVersionComputeConfigArgs{
		RemoveScalingGroups: []string{"nonexistent"},
	}

	result := buildUpdatedComputeConfig(existing, args)

	assert.Len(t, result.GetScalingGroups(), 1)
	assert.Equal(t, "type1", result.GetScalingGroups()["group1"].GetProvider().GetType())
}

func TestBuildUpdatedComputeConfig_CombinedUpsertAndRemove(t *testing.T) {
	t.Parallel()
	existing := &computepb.ComputeConfig{
		ScalingGroups: map[string]*computepb.ComputeConfigScalingGroup{
			"keep":   {Provider: &computepb.ComputeProvider{Type: "keep-type"}},
			"remove": {Provider: &computepb.ComputeProvider{Type: "remove-type"}},
		},
	}
	args := &deploymentspb.UpdateVersionComputeConfigArgs{
		UpsertScalingGroups: map[string]*deploymentspb.ScalingGroupUpdate{
			"new": {ScalingGroup: &computepb.ComputeConfigScalingGroup{
				Provider: &computepb.ComputeProvider{Type: "new-type"},
			}},
		},
		RemoveScalingGroups: []string{"remove"},
	}

	result := buildUpdatedComputeConfig(existing, args)

	assert.Len(t, result.GetScalingGroups(), 2)
	assert.Equal(t, "keep-type", result.GetScalingGroups()["keep"].GetProvider().GetType())
	assert.Equal(t, "new-type", result.GetScalingGroups()["new"].GetProvider().GetType())
	assert.Nil(t, result.GetScalingGroups()["remove"])
}

func TestBuildUpdatedComputeConfig_DoesNotMutateOriginal(t *testing.T) {
	t.Parallel()
	existing := &computepb.ComputeConfig{
		ScalingGroups: map[string]*computepb.ComputeConfigScalingGroup{
			"group1": {Provider: &computepb.ComputeProvider{Type: "original"}},
		},
	}
	args := &deploymentspb.UpdateVersionComputeConfigArgs{
		UpsertScalingGroups: map[string]*deploymentspb.ScalingGroupUpdate{
			"group1": {
				ScalingGroup: &computepb.ComputeConfigScalingGroup{
					Provider: &computepb.ComputeProvider{Type: "modified"},
				},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"provider"}},
			},
		},
	}

	buildUpdatedComputeConfig(existing, args)

	assert.Equal(t, "original", existing.GetScalingGroups()["group1"].GetProvider().GetType(), "original config should not be mutated")
}

func TestBuildUpdatedComputeConfig_NilCurrentConfig(t *testing.T) {
	t.Parallel()
	args := &deploymentspb.UpdateVersionComputeConfigArgs{
		UpsertScalingGroups: map[string]*deploymentspb.ScalingGroupUpdate{
			"group1": {ScalingGroup: &computepb.ComputeConfigScalingGroup{
				Provider: &computepb.ComputeProvider{Type: "new"},
			}},
		},
	}

	result := buildUpdatedComputeConfig(nil, args)

	assert.Len(t, result.GetScalingGroups(), 1)
	assert.Equal(t, "new", result.GetScalingGroups()["group1"].GetProvider().GetType())
}

// applyFieldMask tests

func TestApplyFieldMask_TaskQueueTypes(t *testing.T) {
	t.Parallel()
	dst := &computepb.ComputeConfigScalingGroup{
		TaskQueueTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
	}
	src := &computepb.ComputeConfigScalingGroup{
		TaskQueueTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_ACTIVITY},
	}

	applyFieldMask(dst, src, []string{"task_queue_types"})

	assert.Equal(t, []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_ACTIVITY}, dst.TaskQueueTypes)
}

func TestApplyFieldMask_TopLevelProvider(t *testing.T) {
	t.Parallel()
	dst := &computepb.ComputeConfigScalingGroup{
		Provider: &computepb.ComputeProvider{Type: "old", Details: &commonpb.Payload{Data: []byte("old-details")}},
		Scaler:   &computepb.ComputeScaler{Type: "keep-me"},
	}
	src := &computepb.ComputeConfigScalingGroup{
		Provider: &computepb.ComputeProvider{Type: "new", Details: &commonpb.Payload{Data: []byte("new-details")}},
	}

	applyFieldMask(dst, src, []string{"provider"})

	assert.Equal(t, "new", dst.Provider.Type)
	assert.Equal(t, []byte("new-details"), dst.Provider.Details.Data)
	assert.Equal(t, "keep-me", dst.Scaler.Type, "scaler should be untouched")
}

func TestApplyFieldMask_TopLevelScaler(t *testing.T) {
	t.Parallel()
	dst := &computepb.ComputeConfigScalingGroup{
		Provider: &computepb.ComputeProvider{Type: "keep-me"},
		Scaler:   &computepb.ComputeScaler{Type: "old"},
	}
	src := &computepb.ComputeConfigScalingGroup{
		Scaler: &computepb.ComputeScaler{Type: "new"},
	}

	applyFieldMask(dst, src, []string{"scaler"})

	assert.Equal(t, "new", dst.Scaler.Type)
	assert.Equal(t, "keep-me", dst.Provider.Type, "provider should be untouched")
}

func TestApplyFieldMask_NestedProviderType(t *testing.T) {
	t.Parallel()
	dst := &computepb.ComputeConfigScalingGroup{
		Provider: &computepb.ComputeProvider{
			Type:    "old-type",
			Details: &commonpb.Payload{Data: []byte("keep-details")},
		},
	}
	src := &computepb.ComputeConfigScalingGroup{
		Provider: &computepb.ComputeProvider{Type: "new-type"},
	}

	applyFieldMask(dst, src, []string{"provider.type"})

	assert.Equal(t, "new-type", dst.Provider.Type)
	assert.Equal(t, []byte("keep-details"), dst.Provider.Details.Data, "details should be preserved")
}

func TestApplyFieldMask_NestedProviderDetails(t *testing.T) {
	t.Parallel()
	dst := &computepb.ComputeConfigScalingGroup{
		Provider: &computepb.ComputeProvider{
			Type:    "keep-type",
			Details: &commonpb.Payload{Data: []byte("old")},
		},
	}
	src := &computepb.ComputeConfigScalingGroup{
		Provider: &computepb.ComputeProvider{
			Details: &commonpb.Payload{Data: []byte("new")},
		},
	}

	applyFieldMask(dst, src, []string{"provider.details"})

	assert.Equal(t, "keep-type", dst.Provider.Type, "type should be preserved")
	assert.Equal(t, []byte("new"), dst.Provider.Details.Data)
}

func TestApplyFieldMask_NestedProviderNexusEndpoint(t *testing.T) {
	t.Parallel()
	dst := &computepb.ComputeConfigScalingGroup{
		Provider: &computepb.ComputeProvider{
			Type:           "keep-type",
			NexusEndpoint:  "old-endpoint",
		},
	}
	src := &computepb.ComputeConfigScalingGroup{
		Provider: &computepb.ComputeProvider{NexusEndpoint: "new-endpoint"},
	}

	applyFieldMask(dst, src, []string{"provider.nexus_endpoint"})

	assert.Equal(t, "keep-type", dst.Provider.Type, "type should be preserved")
	assert.Equal(t, "new-endpoint", dst.Provider.NexusEndpoint)
}

func TestApplyFieldMask_NestedScalerType(t *testing.T) {
	t.Parallel()
	dst := &computepb.ComputeConfigScalingGroup{
		Scaler: &computepb.ComputeScaler{
			Type:    "old-type",
			Details: &commonpb.Payload{Data: []byte("keep-details")},
		},
	}
	src := &computepb.ComputeConfigScalingGroup{
		Scaler: &computepb.ComputeScaler{Type: "new-type"},
	}

	applyFieldMask(dst, src, []string{"scaler.type"})

	assert.Equal(t, "new-type", dst.Scaler.Type)
	assert.Equal(t, []byte("keep-details"), dst.Scaler.Details.Data, "details should be preserved")
}

func TestApplyFieldMask_NestedScalerDetails(t *testing.T) {
	t.Parallel()
	dst := &computepb.ComputeConfigScalingGroup{
		Scaler: &computepb.ComputeScaler{
			Type:    "keep-type",
			Details: &commonpb.Payload{Data: []byte("old")},
		},
	}
	src := &computepb.ComputeConfigScalingGroup{
		Scaler: &computepb.ComputeScaler{
			Details: &commonpb.Payload{Data: []byte("new")},
		},
	}

	applyFieldMask(dst, src, []string{"scaler.details"})

	assert.Equal(t, "keep-type", dst.Scaler.Type, "type should be preserved")
	assert.Equal(t, []byte("new"), dst.Scaler.Details.Data)
}

func TestApplyFieldMask_NestedFieldOnNilParent(t *testing.T) {
	t.Parallel()
	dst := &computepb.ComputeConfigScalingGroup{}
	src := &computepb.ComputeConfigScalingGroup{
		Provider: &computepb.ComputeProvider{Type: "new-type"},
	}

	applyFieldMask(dst, src, []string{"provider.type"})

	assert.NotNil(t, dst.Provider, "provider should be initialized")
	assert.Equal(t, "new-type", dst.Provider.Type)
}

func TestApplyFieldMask_UnknownPathIgnored(t *testing.T) {
	t.Parallel()
	dst := &computepb.ComputeConfigScalingGroup{
		Provider: &computepb.ComputeProvider{Type: "original"},
	}
	src := &computepb.ComputeConfigScalingGroup{
		Provider: &computepb.ComputeProvider{Type: "new"},
	}

	applyFieldMask(dst, src, []string{"unknown_field"})

	assert.Equal(t, "original", dst.Provider.Type, "nothing should change")
}

func TestApplyFieldMask_MultiplePaths(t *testing.T) {
	t.Parallel()
	dst := &computepb.ComputeConfigScalingGroup{
		TaskQueueTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
		Provider:       &computepb.ComputeProvider{Type: "old-provider"},
		Scaler:         &computepb.ComputeScaler{Type: "old-scaler"},
	}
	src := &computepb.ComputeConfigScalingGroup{
		TaskQueueTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_ACTIVITY},
		Provider:       &computepb.ComputeProvider{Type: "new-provider"},
		Scaler:         &computepb.ComputeScaler{Type: "new-scaler"},
	}

	applyFieldMask(dst, src, []string{"task_queue_types", "scaler"})

	assert.Equal(t, []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_ACTIVITY}, dst.TaskQueueTypes)
	assert.Equal(t, "old-provider", dst.Provider.Type, "provider should be untouched")
	assert.Equal(t, "new-scaler", dst.Scaler.Type)
}
