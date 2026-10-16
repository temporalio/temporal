// Package migration converts WCI state between the workflow and CHASM representations.
package migration

import (
	"fmt"
	"maps"
	"slices"

	"go.temporal.io/auto-scaled-workers/wci/workflow/iface"
	wcipb "go.temporal.io/server/chasm/lib/wci/gen/wcipb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/sdk"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// AppliedSizeUnknown marks a group whose applied size the workflow never recorded.
const AppliedSizeUnknown = -1

// WorkflowStateToCHASM converts workflow args into a CreateFromMigrationStateRequest. instanceID is
// the routing business ID; region_ids are dropped (rejected by Validate until supported).
func WorkflowStateToCHASM(instanceID string, args *iface.WorkerControllerInstanceWorkflowArgs) (*wcipb.CreateFromMigrationStateRequest, error) {
	local := args.State
	if local == nil {
		local = &iface.WorkerControllerInstanceLocalState{}
	}

	state := &wcipb.WorkerControllerInstanceState{
		DeploymentName:       args.DeploymentName,
		BuildId:              args.BuildId,
		Status:               wcipb.WCI_STATUS_RUNNING,
		CreateTime:           common.CloneProto(local.CreateTime),
		ConflictToken:        slices.Clone(local.ConflictToken),
		LastModifierIdentity: local.LastModifierIdentity,
		NextPollTime:         common.CloneProto(local.NextPollTime),
	}
	if local.Spec != nil {
		state.ScalingGroupSpecs = make(map[string]*wcipb.ScalingGroupSpec, len(local.Spec.ScalingGroupSpecs))
		for id, spec := range local.Spec.ScalingGroupSpecs {
			state.ScalingGroupSpecs[id] = specToCHASM(spec)
		}
	}
	validation, err := validationToCHASM(local.ValidationStatus)
	if err != nil {
		return nil, err
	}
	state.ValidationStatus = validation

	// One group per spec or status entry, so a status without a spec still round-trips.
	groupIDs := slices.Concat(slices.Collect(maps.Keys(state.ScalingGroupSpecs)), slices.Collect(maps.Keys(local.ScalingStatus)))
	groups := make(map[string]*wcipb.ScalingGroupState, len(groupIDs))
	for _, id := range groupIDs {
		if _, ok := groups[id]; ok {
			continue
		}
		group := &wcipb.ScalingGroupState{
			Status:      wcipb.SCALING_GROUP_STATUS_IDLE,
			AppliedSize: AppliedSizeUnknown,
		}
		if status, ok := local.ScalingStatus[id]; ok {
			payload, err := sdk.PreferProtoDataConverter.ToPayload(status)
			if err != nil {
				return nil, fmt.Errorf("encode scaling status %q: %w", id, err)
			}
			group.AlgorithmStatus = payload
		}
		groups[id] = group
	}

	pending := make([]*wcipb.TaskAddEvent, 0, len(local.PendingTaskAddSignals))
	for _, req := range local.PendingTaskAddSignals {
		if req != nil {
			pending = append(pending, TaskAddToCHASM(req))
		}
	}

	return &wcipb.CreateFromMigrationStateRequest{
		NamespaceId:     args.NamespaceId,
		InstanceId:      instanceID,
		State:           state,
		ScalingGroups:   groups,
		PendingTaskAdds: pending,
	}, nil
}

// CHASMStateToWorkflow converts CHASM state into workflow args. Group runtime fields (status,
// attempts, pending invokes) have no workflow equivalent and are dropped.
func CHASMStateToWorkflow(
	namespaceName string,
	namespaceID string,
	state *wcipb.WorkerControllerInstanceState,
	groups map[string]*wcipb.ScalingGroupState,
) (*iface.WorkerControllerInstanceWorkflowArgs, error) {
	local := &iface.WorkerControllerInstanceLocalState{
		CreateTime:           common.CloneProto(state.GetCreateTime()),
		ConflictToken:        slices.Clone(state.GetConflictToken()),
		LastModifierIdentity: state.GetLastModifierIdentity(),
		NextPollTime:         common.CloneProto(state.GetNextPollTime()),
	}
	if state.GetScalingGroupSpecs() != nil {
		local.Spec = &iface.WorkerControllerInstanceSpec{
			ScalingGroupSpecs: make(map[string]iface.ScalingGroupSpec, len(state.GetScalingGroupSpecs())),
		}
		for id, spec := range state.GetScalingGroupSpecs() {
			local.Spec.ScalingGroupSpecs[id] = SpecToWorkflow(spec)
		}
	}
	validation, err := validationToWorkflow(state.GetValidationStatus())
	if err != nil {
		return nil, err
	}
	local.ValidationStatus = validation

	for id, group := range groups {
		if group.GetAlgorithmStatus() == nil {
			continue
		}
		var status iface.ScalingAlgorithmStatus
		if err := sdk.PreferProtoDataConverter.FromPayload(group.GetAlgorithmStatus(), &status); err != nil {
			return nil, fmt.Errorf("decode scaling status %q: %w", id, err)
		}
		if local.ScalingStatus == nil {
			local.ScalingStatus = make(map[string]iface.ScalingAlgorithmStatus, len(groups))
		}
		local.ScalingStatus[id] = status
	}

	return &iface.WorkerControllerInstanceWorkflowArgs{
		NamespaceName:  namespaceName,
		NamespaceId:    namespaceID,
		DeploymentName: state.GetDeploymentName(),
		BuildId:        state.GetBuildId(),
		State:          local,
	}, nil
}

// TaskAddToCHASM converts a workflow task-add signal; also used by RecordTaskAdd callers.
func TaskAddToCHASM(req *iface.SignalTaskAddRequest) *wcipb.TaskAddEvent {
	return &wcipb.TaskAddEvent{
		TaskQueueName:               req.TaskQueueName,
		TaskQueueType:               req.TaskQueueType,
		IsSyncMatch:                 req.IsSyncMatch,
		SyncMatchSignalsSinceLast:   int32(req.SyncMatchSignalsSinceLast),
		NoSyncMatchSignalsSinceLast: int32(req.NoSyncMatchSignalsSinceLast),
	}
}

// TaskAddToWorkflow is the inverse of TaskAddToCHASM.
func TaskAddToWorkflow(event *wcipb.TaskAddEvent) *iface.SignalTaskAddRequest {
	return &iface.SignalTaskAddRequest{
		TaskQueueName:               event.GetTaskQueueName(),
		TaskQueueType:               event.GetTaskQueueType(),
		IsSyncMatch:                 event.GetIsSyncMatch(),
		SyncMatchSignalsSinceLast:   int(event.GetSyncMatchSignalsSinceLast()),
		NoSyncMatchSignalsSinceLast: int(event.GetNoSyncMatchSignalsSinceLast()),
	}
}

func specToCHASM(spec iface.ScalingGroupSpec) *wcipb.ScalingGroupSpec {
	out := &wcipb.ScalingGroupSpec{
		TaskTypes: slices.Clone(spec.TaskTypes),
		Compute: &wcipb.ComputeProviderSpec{
			ProviderType:  string(spec.Compute.ProviderType),
			Config:        common.CloneProto(spec.Compute.Config),
			NexusEndpoint: spec.Compute.NexusEndpoint,
		},
	}
	if spec.Scaling != nil {
		out.Scaling = &wcipb.ScalingAlgorithmSpec{
			ScalingAlgorithm: string(spec.Scaling.ScalingAlgorithm),
			Config:           common.CloneProto(spec.Scaling.Config),
		}
	}
	return out
}

// SpecToWorkflow converts a CHASM group spec to the iface type the scaling algorithms use.
func SpecToWorkflow(spec *wcipb.ScalingGroupSpec) iface.ScalingGroupSpec {
	out := iface.ScalingGroupSpec{
		TaskTypes: slices.Clone(spec.GetTaskTypes()),
		Compute: iface.ComputeProviderSpec{
			ProviderType:  iface.ComputeProviderType(spec.GetCompute().GetProviderType()),
			Config:        common.CloneProto(spec.GetCompute().GetConfig()),
			NexusEndpoint: spec.GetCompute().GetNexusEndpoint(),
		},
	}
	if spec.GetScaling() != nil {
		out.Scaling = &iface.ScalingAlgorithmSpec{
			ScalingAlgorithm: iface.ScalingAlgorithmType(spec.GetScaling().GetScalingAlgorithm()),
			Config:           common.CloneProto(spec.GetScaling().GetConfig()),
		}
	}
	return out
}

func validationToCHASM(v *iface.ValidationStatus) (*wcipb.ValidationStatus, error) {
	if v == nil {
		return nil, nil
	}
	out := &wcipb.ValidationStatus{ErrMessage: v.ErrMessage}
	if !v.LastValidationTime.IsZero() {
		out.LastValidationTime = timestamppb.New(v.LastValidationTime)
	}
	switch v.Status {
	case "":
		out.Result = wcipb.VALIDATION_RESULT_UNSPECIFIED
	case iface.ValidationResultSuccess:
		out.Result = wcipb.VALIDATION_RESULT_SUCCESS
	case iface.ValidationResultFailed:
		out.Result = wcipb.VALIDATION_RESULT_FAILED
	default:
		return nil, fmt.Errorf("unknown validation result %q", v.Status)
	}
	return out, nil
}

func validationToWorkflow(v *wcipb.ValidationStatus) (*iface.ValidationStatus, error) {
	if v == nil {
		return nil, nil
	}
	out := &iface.ValidationStatus{ErrMessage: v.GetErrMessage()}
	if v.GetLastValidationTime() != nil {
		out.LastValidationTime = v.GetLastValidationTime().AsTime()
	}
	switch v.GetResult() {
	case wcipb.VALIDATION_RESULT_UNSPECIFIED:
		out.Status = ""
	case wcipb.VALIDATION_RESULT_SUCCESS:
		out.Status = iface.ValidationResultSuccess
	case wcipb.VALIDATION_RESULT_FAILED:
		out.Status = iface.ValidationResultFailed
	default:
		return nil, fmt.Errorf("unknown validation result %v", v.GetResult())
	}
	return out, nil
}
