package worker

import (
	"context"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
)

type handler struct {
	workerstatepb.UnimplementedWorkerServiceServer

	namespaceRegistry namespace.Registry
	config            *Config
	logger            log.Logger
}

func newHandler(config *Config, namespaceRegistry namespace.Registry, logger log.Logger) *handler {
	return &handler{
		namespaceRegistry: namespaceRegistry,
		config:            config,
		logger:            logger,
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
	// TODO: Read the heartbeat timeout from the request once SDKs send it,
	// and enforce the minimum allowed timeout. Use the minimum for now.
	heartbeatTimeout := h.config.MinHeartbeatTimeout(ns.Name().String())
	// TODO: Once activity binding tracking is added, avoid creating or renewing
	// runs for groups with no tracked activities.
	_, err = chasm.UpdateWithStartExecution(
		ctx,
		chasm.ExecutionKey{NamespaceID: req.GetNamespaceId(), BusinessID: req.GetWorkerGroupingKey()},
		func(chasm.MutableContext, *workerstatepb.RecordHeartbeatRequest) (*WorkerGroup, error) {
			return newWorkerGroup(), nil
		},
		func(group *WorkerGroup, mctx chasm.MutableContext, request *workerstatepb.RecordHeartbeatRequest) (chasm.NoValue, error) {
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
