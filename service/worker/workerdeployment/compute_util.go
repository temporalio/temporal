package workerdeployment

import (
	computepb "go.temporal.io/api/compute/v1"
	wciiface "go.temporal.io/auto-scaled-workers/wci/workflow/iface"
	"go.temporal.io/sdk/workflow"
)

func computeConfigScalingGroupsToWCISpec(scalingGroups map[string]*computepb.ComputeConfigScalingGroup) *wciiface.WorkerControllerInstanceSpec {
	specs := make(map[string]wciiface.ScalingGroupSpec, len(scalingGroups))
	for name, sg := range scalingGroups {
		groupSpec := wciiface.ScalingGroupSpec{
			TaskTypes: sg.GetTaskQueueTypes(),
			Compute: wciiface.ComputeProviderSpec{
				ProviderType: wciiface.ComputeProviderType(sg.GetProvider().GetType()),
			},
		}
		if scaler := sg.GetScaler(); scaler != nil {
			groupSpec.Scaling = &wciiface.ScalingAlgorithmSpec{
				ScalingAlgorithm: wciiface.ScalingAlgorithmType(scaler.GetType()),
			}
		}
		specs[name] = groupSpec
	}
	return &wciiface.WorkerControllerInstanceSpec{
		ScalingGroupSpecs: specs,
	}
}

func scalingGroupUpdatesToWCI(updates map[string]*computepb.ComputeConfigScalingGroupUpdate) map[string]wciiface.ScalingGroupSpecUpdate {
	result := make(map[string]wciiface.ScalingGroupSpecUpdate, len(updates))
	for name, update := range updates {
		sg := update.GetScalingGroup()
		spec := wciiface.ScalingGroupSpec{
			TaskTypes: sg.GetTaskQueueTypes(),
			Compute: wciiface.ComputeProviderSpec{
				ProviderType: wciiface.ComputeProviderType(sg.GetProvider().GetType()),
				Config:       sg.GetProvider().GetDetails(),
			},
		}
		if scaler := sg.GetScaler(); scaler != nil {
			spec.Scaling = &wciiface.ScalingAlgorithmSpec{
				ScalingAlgorithm: wciiface.ScalingAlgorithmType(scaler.GetType()),
				Config:           scaler.GetDetails(),
			}
		}
		result[name] = wciiface.ScalingGroupSpecUpdate{
			Spec:       spec,
			UpdateMask: update.GetUpdateMask().GetPaths(),
		}
	}
	return result
}

func wciSpecToComputeConfig(spec *wciiface.WorkerControllerInstanceSpec) *computepb.ComputeConfig {
	if spec == nil || len(spec.ScalingGroupSpecs) == 0 {
		return nil
	}
	groups := make(map[string]*computepb.ComputeConfigScalingGroup, len(spec.ScalingGroupSpecs))
	for name, sg := range spec.ScalingGroupSpecs {
		group := &computepb.ComputeConfigScalingGroup{
			TaskQueueTypes: sg.TaskTypes,
			Provider: &computepb.ComputeProvider{
				Type:    string(sg.Compute.ProviderType),
				Details: sg.Compute.Config,
			},
		}
		if sg.Scaling != nil {
			group.Scaler = &computepb.ComputeScaler{
				Type:    string(sg.Scaling.ScalingAlgorithm),
				Details: sg.Scaling.Config,
			}
		}
		groups[name] = group
	}
	return &computepb.ComputeConfig{ScalingGroups: groups}
}

func scalingGroupsToUpsertUpdates(scalingGroups map[string]*computepb.ComputeConfigScalingGroup) map[string]*computepb.ComputeConfigScalingGroupUpdate {
	updates := make(map[string]*computepb.ComputeConfigScalingGroupUpdate, len(scalingGroups))
	names := workflow.DeterministicKeys(scalingGroups)
	for _, name := range names {
		sg := scalingGroups[name]
		updates[name] = &computepb.ComputeConfigScalingGroupUpdate{
			ScalingGroup: sg,
		}
	}
	return updates
}
