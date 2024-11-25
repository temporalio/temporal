// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package updateworkflowoptions

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
	"google.golang.org/protobuf/proto"
)

func Invoke(
	ctx context.Context,
	request *historyservice.UpdateWorkflowExecutionOptionsRequest,
	shardCtx shard.Context,
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
		func(workflowLease api.WorkflowLease) (_ *api.UpdateWorkflowAction, updateError error) {
			mutableState := workflowLease.GetMutableState()
			defer func() { workflowLease.GetReleaseFn()(updateError) }()

			if !mutableState.IsWorkflowExecutionRunning() {
				// in-memory mutable state is still clean, let updateError=nil in the defer func()
				// to prevent clearing and reloading mutable state while releasing the lock
				return nil, consts.ErrWorkflowCompleted
			}

			// Merge the requested options mentioned in the field mask with the current options in the mutable state
			mergedOpts, updateError := applyWorkflowExecutionOptions(
				getOptionsFromMutableState(mutableState),
				req.GetWorkflowExecutionOptions(),
				req.GetUpdateMask(),
			)
			if updateError != nil {
				return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("error parsing update_options: %v", updateError))
			}

			// Set options for gRPC response
			ret.WorkflowExecutionOptions = mergedOpts

			// If there is no mutable state change at all, return with no new history event and Noop=true
			if proto.Equal(mergedOpts, getOptionsFromMutableState(mutableState)) {
				return &api.UpdateWorkflowAction{
					Noop:               true,
					CreateWorkflowTask: false,
				}, nil
			}

			_, updateError = mutableState.AddWorkflowExecutionOptionsUpdatedEvent(req.GetWorkflowExecutionOptions().GetVersioningOverride())
			if updateError != nil {
				return nil, updateError
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

func getOptionsFromMutableState(ms workflow.MutableState) *workflowpb.WorkflowExecutionOptions {
	opts := &workflowpb.WorkflowExecutionOptions{}
	if versioningInfo := ms.GetExecutionInfo().GetVersioningInfo(); versioningInfo != nil {
		opts.VersioningOverride = versioningInfo.GetVersioningOverride()
	}
	return opts
}

// applyWorkflowExecutionOptions copies the given paths in `src` struct to `dst` struct
func applyWorkflowExecutionOptions(
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