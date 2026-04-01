package workerdeployment

import (
	"testing"

	"github.com/stretchr/testify/assert"
	computepb "go.temporal.io/api/compute/v1"
	enumspb "go.temporal.io/api/enums/v1"
	wciiface "go.temporal.io/auto-scaled-workers/wci/workflow/iface"
)

func TestComputeConfigScalingGroupsToWCISpec_EmptyGroups(t *testing.T) {
	t.Parallel()
	result := computeConfigScalingGroupsToWCISpec(nil)
	assert.NotNil(t, result)
	assert.Empty(t, result.ScalingGroupSpecs)

	result = computeConfigScalingGroupsToWCISpec(map[string]*computepb.ComputeConfigScalingGroup{})
	assert.NotNil(t, result)
	assert.Empty(t, result.ScalingGroupSpecs)
}

func TestComputeConfigScalingGroupsToWCISpec_WithComputeAndScaling(t *testing.T) {
	t.Parallel()
	groups := map[string]*computepb.ComputeConfigScalingGroup{
		"group1": {
			TaskQueueTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
			Provider:       &computepb.ComputeProvider{Type: "aws-lambda"},
			Scaler:         &computepb.ComputeScaler{Type: "rate-based"},
		},
		"group2": {
			TaskQueueTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_ACTIVITY},
			Provider:       &computepb.ComputeProvider{Type: "aws-ecs"},
		},
	}

	result := computeConfigScalingGroupsToWCISpec(groups)

	assert.Len(t, result.ScalingGroupSpecs, 2)

	g1 := result.ScalingGroupSpecs["group1"]
	assert.Equal(t, []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW}, g1.TaskTypes)
	assert.Equal(t, wciiface.ComputeProviderType("aws-lambda"), g1.Compute.ProviderType)
	assert.NotNil(t, g1.Scaling)
	assert.Equal(t, wciiface.ScalingAlgorithmType("rate-based"), g1.Scaling.ScalingAlgorithm)

	g2 := result.ScalingGroupSpecs["group2"]
	assert.Equal(t, []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_ACTIVITY}, g2.TaskTypes)
	assert.Equal(t, wciiface.ComputeProviderType("aws-ecs"), g2.Compute.ProviderType)
	assert.Nil(t, g2.Scaling, "no scaler means nil scaling spec")
}
