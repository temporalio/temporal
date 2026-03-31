package workerdeployment

import (
	"go.temporal.io/api/serviceerror"

	computepb "go.temporal.io/api/compute/v1"
	wciiface "go.temporal.io/auto-scaled-workers/wci/workflow/iface"
	"go.temporal.io/sdk/workflow"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"google.golang.org/protobuf/proto"
)

// buildUpdatedComputeConfig creates a new ComputeConfig by applying the upsert and
// remove operations from the update args to a copy of the current config.
func buildUpdatedComputeConfig(current *computepb.ComputeConfig, args *deploymentspb.UpdateComputeConfigArgs) (*computepb.ComputeConfig, error) {
	// Start with a copy of existing scaling groups.
	newGroups := make(map[string]*computepb.ComputeConfigScalingGroup)
	for name, sg := range current.GetScalingGroups() {
		cloned, ok := proto.Clone(sg).(*computepb.ComputeConfigScalingGroup)
		if !ok {
			continue
		}
		newGroups[name] = cloned
	}

	// Apply upserts.
	for _, name := range workflow.DeterministicKeys(args.GetUpsertScalingGroups()) {
		update := args.GetUpsertScalingGroups()[name]
		existing, exists := newGroups[name]
		paths := update.GetUpdateMask().GetPaths()
		if !exists {
			// New group: set it entirely.
			newGroups[name] = proto.Clone(update.GetScalingGroup()).(*computepb.ComputeConfigScalingGroup)
		} else if len(paths) == 0 {
			// Empty mask on existing group: no-op, keep existing unchanged.
		} else {
			// Apply only the fields specified in the mask to the cloned existing group.
			if err := applyFieldMask(existing, update.GetScalingGroup(), paths); err != nil {
				return nil, err
			}
		}
	}

	// Apply removes.
	for _, name := range args.GetRemoveScalingGroups() {
		delete(newGroups, name)
	}

	return &computepb.ComputeConfig{ScalingGroups: newGroups}, nil
}

// applyFieldMask copies only the specified field paths from src to dst.
// Supports top-level fields and one level of nesting (e.g. "provider.type").
// Returns an error for unsupported field paths.
func applyFieldMask(dst, src *computepb.ComputeConfigScalingGroup, paths []string) error {
	for _, path := range paths {
		switch path {
		case "task_queue_types":
			dst.TaskQueueTypes = src.GetTaskQueueTypes()
		case "provider":
			dst.Provider = src.GetProvider()
		case "provider.type":
			if dst.Provider == nil {
				dst.Provider = &computepb.ComputeProvider{}
			}
			dst.Provider.Type = src.GetProvider().GetType()
		case "provider.details":
			if dst.Provider == nil {
				dst.Provider = &computepb.ComputeProvider{}
			}
			dst.Provider.Details = src.GetProvider().GetDetails()
		case "provider.nexus_endpoint":
			if dst.Provider == nil {
				dst.Provider = &computepb.ComputeProvider{}
			}
			dst.Provider.NexusEndpoint = src.GetProvider().GetNexusEndpoint()
		case "scaler":
			dst.Scaler = src.GetScaler()
		case "scaler.type":
			if dst.Scaler == nil {
				dst.Scaler = &computepb.ComputeScaler{}
			}
			dst.Scaler.Type = src.GetScaler().GetType()
		case "scaler.details":
			if dst.Scaler == nil {
				dst.Scaler = &computepb.ComputeScaler{}
			}
			dst.Scaler.Details = src.GetScaler().GetDetails()
		default:
			return serviceerror.NewInvalidArgumentf("unsupported field mask path: %q", path)
		}
	}
	return nil
}

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
