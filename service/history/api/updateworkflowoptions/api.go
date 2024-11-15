package updateworkflowoptions

import (
	"context"
	fieldmask_utils "github.com/mennanov/fieldmask-utils"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"google.golang.org/protobuf/proto"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

// A function that maps field mask field names to the names used in Go structs.
// This can be replaced by a generic CamelCase function at some point.
// Using this for now to be simple.
func naming(s string) string {
	if s == "versioning_behavior_override" {
		return "VersioningBehaviorOverride"
	}
	return s
}

func Invoke(
	ctx context.Context,
	request *historyservice.UpdateWorkflowExecutionOptionsRequest,
	shard shard.Context,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (*historyservice.UpdateWorkflowExecutionOptionsResponse, error) {
	ns, err := api.GetActiveNamespace(shard, namespace.ID(request.GetNamespaceId()))
	if err != nil {
		return nil, err
	}

	req := request.GetUpdateRequest()
	err = api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		definition.NewWorkflowKey(
			ns.ID().String(),
			req.GetWorkflowExecution().GetWorkflowId(),
			req.GetWorkflowExecution().GetRunId(),
		),
		func(workflowLease api.WorkflowLease) (_ *api.UpdateWorkflowAction, updateError error) {
			mutableState := workflowLease.GetMutableState()
			defer func() { workflowLease.GetReleaseFn()(updateError) }()

			// todo carly: dedupe by requestID?
			_ = req.GetRequestId()

			// Merge the requested fields mentioned in the field mask with the current fields
			mergedOpts := workflowExecutionOptionsFromMutableState(mutableState)
			mask, _ := fieldmask_utils.MaskFromPaths(req.GetUpdateMask().GetPaths(), naming)
			_ = fieldmask_utils.StructToStruct(mask, req.GetWorkflowExecutionOptions(), mergedOpts)

			// Compare with current options
			currOpts := workflowExecutionOptionsFromMutableState(mutableState)
			if proto.Equal(currOpts, mergedOpts) {
				// nothing to update
				return &api.UpdateWorkflowAction{
					Noop:               true,
					CreateWorkflowTask: false,
				}, nil
			}

			_, err := mutableState.AddWorkflowExecutionOptionsUpdatedEvent(req.GetWorkflowExecutionOptions(), req.GetUpdateMask())
			if err != nil {
				return nil, err
			}

			if sameDeployment(mergedOpts, mutableState) {
				applyWorkflowExecutionOptionsToMutableState(mergedOpts, mutableState)
				return &api.UpdateWorkflowAction{
					Noop:               false,
					CreateWorkflowTask: false, // todo carly: I dont think we want to create a WFT?
				}, nil
			}

			// Deployment has changed due to update
			applyWorkflowExecutionOptionsToMutableState(mergedOpts, mutableState)
			// todo carly: handle deployment change (ie. pending tasks)
			// todo carly part 2: handle safe deployment change

			return &api.UpdateWorkflowAction{
				Noop:               false,
				CreateWorkflowTask: false, // todo carly: I dont think we want to create a WFT?
			}, nil
		},
		nil,
		shard,
		workflowConsistencyChecker,
	)
	if err != nil {
		return nil, err
	}

	/*
		0. Do some verifications
		1. Add the event to the history
		2. Apply the event, which does the side effects
			- update MS (we will make this automatically update visibility)
			- create wf tasks? i.e. if there is a pending wf task / transient wf task, possibly need to recreate and reschedule it, invalidate old one
	*/
	return nil, nil
}

func workflowExecutionOptionsFromMutableState(ms workflow.MutableState) *workflowpb.WorkflowExecutionOptions {
	opts := &workflowpb.WorkflowExecutionOptions{}
	if versioningInfo := ms.GetExecutionInfo().GetVersioningInfo(); versioningInfo != nil {
		if behaviorOverride := versioningInfo.GetBehaviorOverride(); behaviorOverride != enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED {
			opts.VersioningBehaviorOverride = &commonpb.VersioningBehaviorOverride{
				Behavior:         behaviorOverride,
				WorkerDeployment: versioningInfo.GetDeploymentOverride(),
			}
		}
	}
	return opts
}

func sameDeployment(mergedOpts *workflowpb.WorkflowExecutionOptions, ms workflow.MutableState) bool {
	// todo carly
	return false
}

func applyWorkflowExecutionOptionsToMutableState(opts *workflowpb.WorkflowExecutionOptions, ms workflow.MutableState) workflow.MutableState {
	ms.GetExecutionInfo().VersioningInfo.BehaviorOverride = opts.VersioningBehaviorOverride.Behavior
	ms.GetExecutionInfo().VersioningInfo.DeploymentOverride = opts.VersioningBehaviorOverride.WorkerDeployment
	return ms
}
