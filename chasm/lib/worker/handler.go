package worker

import (
	"context"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
)

type handler struct {
	workerstatepb.UnimplementedWorkerServiceServer

	namespaceRegistry   namespace.Registry
	enabled             dynamicconfig.BoolPropertyFnWithNamespaceFilter
	minHeartbeatTimeout dynamicconfig.DurationPropertyFnWithNamespaceFilter
	logger              log.Logger
}

func newHandler(dc *dynamicconfig.Collection, namespaceRegistry namespace.Registry, logger log.Logger) *handler {
	return &handler{
		namespaceRegistry:   namespaceRegistry,
		enabled:             dynamicconfig.EnableDurableWorkerHeartbeats.Get(dc),
		minHeartbeatTimeout: dynamicconfig.MinWorkerHeartbeatTimeout.Get(dc),
		logger:              logger,
	}
}

// RecordHeartbeat creates or renews the shared lease for the group.
//
//   - Heartbeats update the existing open run.
//   - When the lease expires, we close the current run.
//   - When we receive a heartbeat later, it reuses the grouping key to create a new run.
func (h *handler) RecordHeartbeat(ctx context.Context, req *workerstatepb.RecordHeartbeatRequest) (_ *workerstatepb.RecordHeartbeatResponse, retErr error) {
	defer log.CapturePanic(h.logger, &retErr)

	if req.GetNamespaceId() == "" || req.GetWorkerGroupingKey() == "" {
		return nil, serviceerror.NewInvalidArgument("namespace ID and worker grouping key are required")
	}
	ns, err := h.namespaceRegistry.GetNamespaceByID(namespace.ID(req.GetNamespaceId()))
	if err != nil {
		return nil, err
	}
	if !h.enabled(ns.Name().String()) {
		return &workerstatepb.RecordHeartbeatResponse{FrontendResponse: &workflowservice.RecordWorkerHeartbeatResponse{}}, nil
	}
	// TODO: Read the heartbeat timeout from the request once SDKs send it,
	// and enforce the minimum allowed timeout. Use the minimum for now.
	heartbeatTimeout := h.minHeartbeatTimeout(ns.Name().String())
	var createdGroup *WorkerGroup
	// TODO: Once activity binding tracking is added, avoid creating or renewing
	// runs for groups with no tracked activities.
	_, err = chasm.UpdateWithStartExecution(
		ctx,
		chasm.ExecutionKey{NamespaceID: req.GetNamespaceId(), BusinessID: req.GetWorkerGroupingKey()},
		func(mctx chasm.MutableContext, request *workerstatepb.RecordHeartbeatRequest) (*WorkerGroup, error) {
			group, err := newWorkerGroup(request.GetWorkerGroupingKey())
			if err != nil {
				return nil, err
			}
			// TODO: Remove this call once CHASM saves children added in the update callback.
			if err := group.recordHeartbeat(mctx, request.GetFrontendRequest().GetWorkerHeartbeat(), heartbeatTimeout); err != nil {
				return nil, err
			}
			createdGroup = group
			return group, nil
		},
		func(group *WorkerGroup, mctx chasm.MutableContext, request *workerstatepb.RecordHeartbeatRequest) (chasm.NoValue, error) {
			// The first heartbeat was already applied in startFn.
			// TODO: Remove this check once the first heartbeat is handled only in the update callback.
			if group == createdGroup {
				return nil, nil
			}
			return nil, group.recordHeartbeat(mctx, request.GetFrontendRequest().GetWorkerHeartbeat(), heartbeatTimeout)
		},
		req,
		chasm.WithBusinessIDPolicy(chasm.BusinessIDReusePolicyAllowDuplicate, chasm.BusinessIDConflictPolicyUseExisting),
	)
	if err != nil {
		return nil, err
	}
	return &workerstatepb.RecordHeartbeatResponse{FrontendResponse: &workflowservice.RecordWorkerHeartbeatResponse{}}, nil
}
