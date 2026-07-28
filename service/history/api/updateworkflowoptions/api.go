package updateworkflowoptions

import (
	"context"
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func Invoke(
	ctx context.Context,
	request *historyservice.UpdateWorkflowExecutionOptionsRequest,
	shardCtx historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	matchingClient matchingservice.MatchingServiceClient,
	versionCache worker_versioning.VersionMembershipAndReactivationStatusCache,
	reactivationSignaler api.VersionReactivationSignalerFn,
) (*historyservice.UpdateWorkflowExecutionOptionsResponse, error) {
	req := request.GetUpdateRequest()
	ns, err := api.GetActiveNamespace(shardCtx, namespace.ID(request.GetNamespaceId()), req.GetWorkflowExecution().GetWorkflowId())
	if err != nil {
		return nil, err
	}
	ret := &historyservice.UpdateWorkflowExecutionOptionsResponse{}

	// Store versioning override to send reactivation signal after successful persistence
	var versioningOverrideForReactivation *workflowpb.VersioningOverride
	var shouldSkipReactivation bool
	var revisionNumber int64

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

			// If the requested override is pinned and omitted optional pinned version, fill in the current pinned version if it exists,
			// or error if no pinned version exists.
			// Clone the requested options to avoid mutating the original request but only do the cloning work if needed.
			requestedOptions := req.GetWorkflowExecutionOptions()
			if requestedOptions.GetVersioningOverride().GetPinned().GetBehavior() != workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_UNSPECIFIED &&
				requestedOptions.GetVersioningOverride().GetPinned().GetVersion() == nil {
				currentVersion := worker_versioning.ExternalWorkerDeploymentVersionFromDeployment(workflow.GetEffectiveDeployment(mutableState.GetExecutionInfo().GetVersioningInfo()))
				if effectiveBevior := workflow.GetEffectiveVersioningBehavior(mutableState.GetExecutionInfo().GetVersioningInfo()); effectiveBevior != enumspb.VERSIONING_BEHAVIOR_PINNED {
					return nil, serviceerror.NewFailedPreconditionf("must specify a specific pinned override version because workflow with id %v has behavior %s and is not yet pinned to any version",
						mutableState.GetExecutionInfo().GetWorkflowId(),
						effectiveBevior.String(),
					)
				}
				var ok bool
				requestedOptions, ok = proto.Clone(requestedOptions).(*workflowpb.WorkflowExecutionOptions)
				if !ok { // this will never happen, but linter wants me to check the casting, so do it just in case
					return nil, serviceerror.NewInternalf("failed to copy workflow options to workflow options: %+v", requestedOptions)
				}
				requestedOptions.GetVersioningOverride().GetPinned().Version = currentVersion
			}

			// Validate versioning override, if any.
			shouldSkipReactivation, revisionNumber, err = worker_versioning.ValidateVersioningOverrideAndGetReactivationEligibility(ctx, requestedOptions.GetVersioningOverride(), matchingClient, versionCache, mutableState.GetExecutionInfo().GetTaskQueue(), enumspb.TASK_QUEUE_TYPE_WORKFLOW, ns.ID().String())
			if err != nil {
				return nil, err
			}
			mergedOpts, hasChanges, err := MergeAndApply(mutableState, requestedOptions, req.GetUpdateMask(), req.GetIdentity())
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

			ret.UpdateTime = timestamppb.New(mutableState.Now())

			// Store versioning override to send reactivation signal after successful persistence
			versioningOverrideForReactivation = mergedOpts.GetVersioningOverride()

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

	// Notify version workflow if we're pinning to a potentially drained version.
	// This is done after successful persistence to avoid signaling if the update fails.
	api.ReactivateVersionWorkflowIfPinned(ctx, ns, versioningOverrideForReactivation, reactivationSignaler, shardCtx.GetConfig().EnableVersionReactivationSignals(), shouldSkipReactivation, revisionNumber)

	return ret, nil
}

// OptionsToReapply is used to track options that need to be reapplied on every update,
// whose changes cannot be detected simply by options equality check.
type OptionsToReapply struct {
	timeSkippingConfigHasChanged bool
}

func (u OptionsToReapply) hasChanges() bool {
	return u.timeSkippingConfigHasChanged
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
	mergedOpts, optionsToReapply, err := mergeWorkflowExecutionOptions(
		getOptionsFromMutableState(ms),
		opts,
		updateMask,
	)
	if err != nil {
		return nil, false, serviceerror.NewInvalidArgumentf("error applying update_options: %v", err)
	}

	hasChanges := !proto.Equal(mergedOpts, getOptionsFromMutableState(ms)) || optionsToReapply.hasChanges()
	if !hasChanges {
		return mergedOpts, false, nil
	}

	unsetOverride := mergedOpts.GetVersioningOverride() == nil
	_, err = ms.AddWorkflowExecutionOptionsUpdatedEvent(
		mergedOpts.GetVersioningOverride(), unsetOverride, "", nil, nil, identity, mergedOpts.GetPriority(),
		mergedOpts.GetTimeSkippingConfig(),
		optionsToReapply.timeSkippingConfigHasChanged, nil)
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
	if priority := ms.GetExecutionInfo().GetPriority(); priority != nil {
		if cloned, ok := proto.Clone(priority).(*commonpb.Priority); ok {
			opts.Priority = cloned
		}
	}
	if tsInfo := ms.GetExecutionInfo().GetTimeSkippingInfo(); tsInfo != nil {
		if cloned, ok := proto.Clone(tsInfo.GetConfig()).(*commonpb.TimeSkippingConfig); ok {
			opts.TimeSkippingConfig = cloned
		}
	}
	return opts
}

// mergeWorkflowExecutionOptions copies the given paths in `src` struct to `dst` struct and returns
// the merged opts and the options to reapply
func mergeWorkflowExecutionOptions(
	mergeInto, mergeFrom *workflowpb.WorkflowExecutionOptions,
	updateMask *fieldmaskpb.FieldMask,
) (*workflowpb.WorkflowExecutionOptions, OptionsToReapply, error) {
	_, err := fieldmaskpb.New(mergeInto, updateMask.GetPaths()...)
	if err != nil { // errors if any paths are not valid for the struct we are merging into
		return nil, OptionsToReapply{}, err
	}
	updateFields := util.ParseFieldMask(updateMask)

	if _, ok := updateFields["versioningOverride"]; ok {
		mergeInto.VersioningOverride = mergeFrom.GetVersioningOverride()
	}

	if _, ok := updateFields["versioningOverride.deployment"]; ok {
		if _, ok := updateFields["versioningOverride.behavior"]; !ok {
			return nil, OptionsToReapply{}, serviceerror.NewInvalidArgument("versioning_override fields must be updated together")
		}
		mergeInto.VersioningOverride = mergeFrom.GetVersioningOverride()
	}

	if _, ok := updateFields["versioningOverride.behavior"]; ok {
		if _, ok := updateFields["versioningOverride.deployment"]; !ok {
			return nil, OptionsToReapply{}, serviceerror.NewInvalidArgument("versioning_override fields must be updated together")
		}
		mergeInto.VersioningOverride = mergeFrom.GetVersioningOverride()
	}

	// Keep the deprecated behavior/deployment pair above for compatibility, but require v0.32
	// oneof overrides to be replaced atomically instead of pretending nested masks are partial updates.
	for key := range updateFields {
		if strings.HasPrefix(key, "versioningOverride.") &&
			key != "versioningOverride.deployment" &&
			key != "versioningOverride.behavior" {
			return nil, OptionsToReapply{}, serviceerror.NewInvalidArgument(
				"versioning_override doesn't support partial updates")
		}
	}

	// ==== Priority

	if _, ok := updateFields["priority"]; ok {
		mergeInto.Priority = mergeFrom.GetPriority()
	}

	if _, ok := updateFields["priority.priorityKey"]; ok {
		if mergeInto.Priority == nil {
			mergeInto.Priority = &commonpb.Priority{}
		}
		mergeInto.Priority.PriorityKey = mergeFrom.GetPriority().GetPriorityKey()
	}

	if _, ok := updateFields["priority.fairnessKey"]; ok {
		if mergeInto.Priority == nil {
			mergeInto.Priority = &commonpb.Priority{}
		}
		mergeInto.Priority.FairnessKey = mergeFrom.Priority.GetFairnessKey()
	}

	if _, ok := updateFields["priority.fairnessWeight"]; ok {
		if mergeInto.Priority == nil {
			mergeInto.Priority = &commonpb.Priority{}
		}
		mergeInto.Priority.FairnessWeight = mergeFrom.Priority.GetFairnessWeight()
	}

	// ==== Time Skipping Config
	// - fast forward need reapplication so equality check is not enough;
	// - We only support validating and updating the TSC as a whole;
	var originalTSC *commonpb.TimeSkippingConfig
	if tsc := mergeInto.GetTimeSkippingConfig(); tsc != nil {
		if cloned, ok := proto.Clone(tsc).(*commonpb.TimeSkippingConfig); ok {
			originalTSC = cloned
		}
	}

	var fastForwardSet bool
	if _, ok := updateFields["timeSkippingConfig"]; ok {
		mergeInto.TimeSkippingConfig = mergeFrom.GetTimeSkippingConfig()
		if mergeFrom.GetTimeSkippingConfig().GetFastForward() != nil {
			fastForwardSet = true
		}
	}

	for key := range updateFields {
		if strings.HasPrefix(key, "timeSkippingConfig.") {
			return nil, OptionsToReapply{}, serviceerror.NewInvalidArgument(
				"time_skipping_config doesn't support partial update")
		}
	}

	var optionsToReapply OptionsToReapply
	optionsToReapply.timeSkippingConfigHasChanged = fastForwardSet || (!proto.Equal(mergeInto.GetTimeSkippingConfig(), originalTSC))
	return mergeInto, optionsToReapply, nil
}
