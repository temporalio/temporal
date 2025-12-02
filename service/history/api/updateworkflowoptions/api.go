package updateworkflowoptions

import (
	"context"

	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func Invoke(
	ctx context.Context,
	request *historyservice.UpdateWorkflowExecutionOptionsRequest,
	shardCtx historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (*historyservice.UpdateWorkflowExecutionOptionsResponse, error) {
	ns, err := api.GetActiveNamespace(shardCtx, namespace.ID(request.GetNamespaceId()))
	if err != nil {
		return nil, err
	}
	req := request.GetUpdateRequest()
	ret := &historyservice.UpdateWorkflowExecutionOptionsResponse{}

	err = api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		definition.NewWorkflowKey(
			ns.ID().String(),
			req.GetWorkflowExecution().GetWorkflowId(),
			req.GetWorkflowExecution().GetRunId(),
		),
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			mutableState := workflowLease.GetMutableState()
			if !mutableState.IsWorkflowExecutionRunning() {
				// in-memory mutable state is still clean, let updateError=nil in the defer func()
				// to prevent clearing and reloading mutable state while releasing the lock
				return nil, consts.ErrWorkflowCompleted
			}

			mergedOpts, hasChanges, err := MergeAndApply(mutableState, req.GetWorkflowExecutionOptions(), req.GetUpdateMask(), req.GetIdentity())
			if err != nil {
				return nil, err
			}
			// Set options for gRPC response
			ret.WorkflowExecutionOptions = mergedOpts

			// If there is no mutable state change at all, return with no new history event and Noop=true
			if !hasChanges {
				return &api.UpdateWorkflowAction{
					Noop:               true,
					CreateWorkflowTask: false,
				}, nil
			}

			// TODO (carly) part 2: handle safe deployment change --> CreateWorkflowTask=true
			return &api.UpdateWorkflowAction{
				Noop:               false,
				CreateWorkflowTask: false,
			}, nil
		},
		nil,
		shardCtx,
		workflowConsistencyChecker,
	)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

// MergeAndApply merges the requested options mentioned in the field mask with the current options in the mutable state
// and applies the changes to the mutable state. Returns the merged options and a boolean indicating if there were any changes.
func MergeAndApply(
	ms historyi.MutableState,
	opts *workflowpb.WorkflowExecutionOptions,
	updateMask *fieldmaskpb.FieldMask,
	identity string,
) (*workflowpb.WorkflowExecutionOptions, bool, error) {
	// Merge the requested options mentioned in the field mask with the current options in the mutable state
	mergedOpts, err := mergeWorkflowExecutionOptions(
		getOptionsFromMutableState(ms),
		opts,
		updateMask,
	)
	if err != nil {
		return nil, false, serviceerror.NewInvalidArgumentf("error applying update_options: %v", err)
	}

	// If there is no mutable state change at all, return with no new history event and Noop=true
	hasChanges := !proto.Equal(mergedOpts, getOptionsFromMutableState(ms))
	if !hasChanges {
		return mergedOpts, false, nil
	}

	unsetOverride := false
	if mergedOpts.GetVersioningOverride() == nil {
		unsetOverride = true
	}
	_, err = ms.AddWorkflowExecutionOptionsUpdatedEvent(mergedOpts.GetVersioningOverride(), unsetOverride, "", nil, nil, identity)
	if err != nil {
		return nil, hasChanges, err
	}
	return mergedOpts, hasChanges, nil
}

func getOptionsFromMutableState(ms historyi.MutableState) *workflowpb.WorkflowExecutionOptions {
	opts := &workflowpb.WorkflowExecutionOptions{}
	if versioningInfo := ms.GetExecutionInfo().GetVersioningInfo(); versioningInfo != nil {
		override, ok := proto.Clone(versioningInfo.GetVersioningOverride()).(*workflowpb.VersioningOverride)
		if !ok {
			return nil
		}
		opts.VersioningOverride = override
	}
	return opts
}

// mergeWorkflowExecutionOptions copies the given paths in `src` struct to `dst` struct
func mergeWorkflowExecutionOptions(
	mergeInto, mergeFrom *workflowpb.WorkflowExecutionOptions,
	updateMask *fieldmaskpb.FieldMask,
) (*workflowpb.WorkflowExecutionOptions, error) {
	_, err := fieldmaskpb.New(mergeInto, updateMask.GetPaths()...)
	if err != nil { // errors if any paths are not valid for the struct we are merging into
		return nil, err
	}
	updateFields := util.ParseFieldMask(updateMask)
	if _, ok := updateFields["versioningOverride"]; ok {
		mergeInto.VersioningOverride = mergeFrom.GetVersioningOverride()
	}

	if _, ok := updateFields["versioningOverride.deployment"]; ok {
		if _, ok := updateFields["versioningOverride.behavior"]; !ok {
			return nil, serviceerror.NewInvalidArgument("versioning_override fields must be updated together")
		}
		mergeInto.VersioningOverride = mergeFrom.GetVersioningOverride()
	}

	if _, ok := updateFields["versioningOverride.behavior"]; ok {
		if _, ok := updateFields["versioningOverride.deployment"]; !ok {
			return nil, serviceerror.NewInvalidArgument("versioning_override fields must be updated together")
		}
		mergeInto.VersioningOverride = mergeFrom.GetVersioningOverride()
	}
	return mergeInto, nil
}
