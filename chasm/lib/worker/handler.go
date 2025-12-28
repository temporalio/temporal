package worker

import (
	"context"
	"fmt"

	"go.temporal.io/server/chasm"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
)

type handler struct {
	workerstatepb.UnimplementedWorkerServiceServer
}

func newHandler() *handler {
	return &handler{}
}

func (h *handler) RecordHeartbeat(ctx context.Context, req *workerstatepb.RecordHeartbeatRequest) (*workerstatepb.RecordHeartbeatResponse, error) {
	// Validate that exactly one worker heartbeat is present
	frontendReq := req.GetFrontendRequest()
	if frontendReq == nil || len(frontendReq.GetWorkerHeartbeat()) != 1 {
		return nil, fmt.Errorf("exactly one worker heartbeat must be present in the request")
	}

	workerHeartbeat := frontendReq.GetWorkerHeartbeat()[0]

	executionKey := chasm.ExecutionKey{
		NamespaceID: req.NamespaceId,
		BusinessID:  workerHeartbeat.WorkerInstanceKey,
	}

	// Try to update existing worker, or create new one if not found
	resp, _, _, _, err := chasm.UpdateWithNewExecution(
		ctx,
		executionKey,
		// newFn: called if worker doesn't exist
		func(ctx chasm.MutableContext, req *workerstatepb.RecordHeartbeatRequest) (*Worker, *workerstatepb.RecordHeartbeatResponse, error) {
			w := NewWorker()
			resp, err := w.recordHeartbeat(ctx, req)
			return w, resp, err
		},
		// updateFn: called if worker exists
		(*Worker).recordHeartbeat,
		req,
	)

	if err != nil {
		return nil, err
	}

	return resp, nil
}
