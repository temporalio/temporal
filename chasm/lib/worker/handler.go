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

	namespaceRegistry namespace.Registry
	enabled           dynamicconfig.BoolPropertyFnWithNamespaceFilter
	leaseDuration     dynamicconfig.DurationPropertyFnWithNamespaceFilter
	logger            log.Logger
}

func newHandler(dc *dynamicconfig.Collection, namespaceRegistry namespace.Registry, logger log.Logger) *handler {
	return &handler{
		namespaceRegistry: namespaceRegistry,
		enabled:           dynamicconfig.EnableWorkerStateTracking.Get(dc),
		leaseDuration:     dynamicconfig.WorkerStateTrackingLeaseDuration.Get(dc),
		logger:            logger,
	}
}

// RecordHeartbeat creates or renews the group addressed by the SDK grouping key.
// CHASM updates an open run or creates a new run if the previous lease expired.
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
		return nil, serviceerror.NewFailedPrecondition("durable worker heartbeats are disabled")
	}
	leaseDuration := h.leaseDuration(ns.Name().String())
	var createdGroup *WorkerGroup
	_, err = chasm.UpdateWithStartExecution(
		ctx,
		chasm.ExecutionKey{NamespaceID: req.GetNamespaceId(), BusinessID: req.GetWorkerGroupingKey()},
		func(mctx chasm.MutableContext, request *workerstatepb.RecordHeartbeatRequest) (*WorkerGroup, error) {
			var err error
			createdGroup, err = newWorkerGroup(mctx, request.GetWorkerGroupingKey(), request.GetFrontendRequest().GetWorkerHeartbeat(), leaseDuration)
			return createdGroup, err
		},
		func(group *WorkerGroup, mctx chasm.MutableContext, request *workerstatepb.RecordHeartbeatRequest) (chasm.NoValue, error) {
			// CHASM also invokes this callback for a newly created component.
			// Its constructor already applied the first heartbeat and expiry task.
			if group == createdGroup {
				return nil, nil
			}
			return nil, group.recordHeartbeat(mctx, request.GetFrontendRequest().GetWorkerHeartbeat(), leaseDuration)
		},
		req,
		chasm.WithBusinessIDPolicy(chasm.BusinessIDReusePolicyAllowDuplicate, chasm.BusinessIDConflictPolicyUseExisting),
	)
	if err != nil {
		return nil, err
	}
	return &workerstatepb.RecordHeartbeatResponse{FrontendResponse: &workflowservice.RecordWorkerHeartbeatResponse{}}, nil
}
