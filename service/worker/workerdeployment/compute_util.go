package workerdeployment

import (
	computepb "go.temporal.io/api/compute/v1"
	wciiface "go.temporal.io/auto-scaled-workers/wci/workflow/iface"
	"go.temporal.io/sdk/workflow"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
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

func wciSpecToComputeConfigSummary(spec *wciiface.WorkerControllerInstanceSpec) *computepb.ComputeConfigSummary {
	if spec == nil || len(spec.ScalingGroupSpecs) == 0 {
		return nil
	}
	groups := make(map[string]*computepb.ComputeConfigScalingGroupSummary, len(spec.ScalingGroupSpecs))
	names := workflow.DeterministicKeys(spec.ScalingGroupSpecs)
	for _, name := range names {
		sg := spec.ScalingGroupSpecs[name]
		groups[name] = &computepb.ComputeConfigScalingGroupSummary{
			TaskQueueTypes: sg.TaskTypes,
			ProviderType:   string(sg.Compute.ProviderType),
		}
	}
	return &computepb.ComputeConfigSummary{ScalingGroups: groups}
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

// computeValidationSummary aggregates WCI validation status counts across all versions
// that have compute config configured.
func computeValidationSummary(versions map[string]*deploymentspb.WorkerDeploymentVersionSummary) *deploymentspb.WorkerDeploymentValidationSummary {
	summary := &deploymentspb.WorkerDeploymentValidationSummary{}
	for _, v := range versions {
		if v.GetComputeConfig() == nil || len(v.GetComputeConfig().GetScalingGroups()) == 0 {
			continue
		}
		switch v.GetValidationStatus().GetHealth() {
		case computepb.WorkerDeploymentVersionValidationStatus_HEALTH_OK:
			summary.ConnectedCount++
		case computepb.WorkerDeploymentVersionValidationStatus_HEALTH_ERROR:
			summary.FailedCount++
		default:
			summary.UnknownCount++
		}
	}
	return summary
}

// DeploymentValidationSummaryToProto converts the internal deployment validation summary
// to the public proto type for use in API responses.
func DeploymentValidationSummaryToProto(s *deploymentspb.WorkerDeploymentValidationSummary) *computepb.WorkerDeploymentValidationSummary {
	if s == nil {
		return nil
	}
	return &computepb.WorkerDeploymentValidationSummary{
		ConnectedCount: s.ConnectedCount,
		FailedCount:    s.FailedCount,
		UnknownCount:   s.UnknownCount,
	}
}

// wciValidationStatusToProto converts a WCI ValidationStatus to the public proto type.
func wciValidationStatusToProto(vs *wciiface.ValidationStatus) *computepb.WorkerDeploymentVersionValidationStatus {
	if vs == nil {
		return nil
	}
	s := &computepb.WorkerDeploymentVersionValidationStatus{
		LastCheckTime: timestamppb.New(vs.LastValidationTime),
	}
	switch vs.Status {
	case wciiface.ValidationResultSuccess:
		s.Health = computepb.WorkerDeploymentVersionValidationStatus_HEALTH_OK
	case wciiface.ValidationResultFailed:
		s.Health = computepb.WorkerDeploymentVersionValidationStatus_HEALTH_ERROR
		s.ErrorMessage = vs.ErrMessage
	}
	return s
}
