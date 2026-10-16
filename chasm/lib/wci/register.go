package wci

import (
	"context"
	"errors"

	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/auto-scaled-workers/wci/compute"
	"go.temporal.io/auto-scaled-workers/wci/workflow/iface"
	scalingalgorithm "go.temporal.io/auto-scaled-workers/wci/workflow/scaling_algorithm"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

// registration is what a group's task-queue registration leaves to persist.
type registration struct {
	status iface.ScalingAlgorithmStatus
	size   *int32
}

// registerTaskQueues starts workers for groups whose task queues are not registered yet, as the
// workflow's InvokeWorkersToRegisterTaskQueues does. Runs without the execution lock.
func registerTaskQueues(
	ctx context.Context,
	algoCtx context.Context,
	frontend workflowservice.WorkflowServiceClient,
	client ComputeClient,
	logger log.Logger,
	rc compute.RequestContext,
	spec *iface.WorkerControllerInstanceSpec,
	statuses map[string]iface.ScalingAlgorithmStatus,
) (map[string]registration, error) {
	registered, err := registeredTaskQueueTypes(ctx, frontend, rc)
	if err != nil {
		var exhausted *serviceerror.ResourceExhausted
		if errors.As(err, &exhausted) {
			return nil, err
		}
		logger.Warn("Could not determine registered task queues; invoking workers unconditionally", tag.Error(err))
	}

	out := map[string]registration{}
	for id, group := range spec.ScalingGroupSpecs {
		if allRegistered(spec.EffectiveTaskTypesForGroup(id), registered) {
			continue
		}
		algo, config, err := algorithmFor(algoCtx, group)
		if err != nil {
			return nil, serviceerror.NewInvalidArgumentf("%s: %v", id, err)
		}
		resp, err := algo.TaskQueueRegistrationActions(algoCtx, config, statuses[id])
		if err != nil {
			return nil, serviceerror.NewInvalidArgumentf("%s: %v", id, err)
		}
		reg := registration{status: resp.Status}
		for _, action := range resp.Actions {
			switch action.Action {
			case scalingalgorithm.ActionTypeInvokeWorker:
				err = client.InvokeWorker(ctx, rc, &group.Compute)
			case scalingalgorithm.ActionTypeUpdateWorkerSetSize:
				if action.Count == nil {
					return nil, serviceerror.NewInternalf("%s: registration action missing count", id)
				}
				err = client.UpdateWorkerSetSize(ctx, rc, &group.Compute, *action.Count)
				reg.size = action.Count
			default:
				return nil, serviceerror.NewInternalf("%s: unexpected registration action %q", id, action.Action)
			}
			if err != nil {
				return nil, providerError(id, err)
			}
		}
		out[id] = reg
	}
	return out, nil
}

func registeredTaskQueueTypes(ctx context.Context, frontend workflowservice.WorkflowServiceClient, rc compute.RequestContext) (map[enumspb.TaskQueueType]struct{}, error) {
	resp, err := frontend.DescribeWorkerDeploymentVersion(ctx, &workflowservice.DescribeWorkerDeploymentVersionRequest{
		Namespace:         rc.NamespaceName,
		DeploymentVersion: &deploymentpb.WorkerDeploymentVersion{DeploymentName: rc.DeploymentName, BuildId: rc.DeploymentBuildID},
	})
	if err != nil {
		return nil, err
	}
	out := map[enumspb.TaskQueueType]struct{}{}
	for _, tq := range resp.GetVersionTaskQueues() {
		out[tq.GetType()] = struct{}{}
	}
	return out, nil
}

func allRegistered(want []enumspb.TaskQueueType, registered map[enumspb.TaskQueueType]struct{}) bool {
	for _, t := range want {
		if _, ok := registered[t]; !ok {
			return false
		}
	}
	return true
}

// providerError maps a provider call failure onto the error the workflow's update returned.
func providerError(id string, err error) error {
	if errors.Is(err, ErrProviderNotSupportedOnCHASM) {
		return err
	}
	return serviceerror.NewFailedPreconditionf("%s: %v", id, err)
}
