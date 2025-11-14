package worker

import (
	"context"
	"errors"
	"fmt"

	"go.temporal.io/api/serviceerror"
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

	// Try to update existing worker first
	resp, _, err := chasm.UpdateComponent(
		ctx,
		chasm.NewComponentRef[*Worker](
			chasm.EntityKey{
				NamespaceID: req.NamespaceId,
				BusinessID:  workerHeartbeat.WorkerInstanceKey,
			},
		),
		(*Worker).recordHeartbeat,
		req,
	)

	// If worker doesn't exist, create it
	if err != nil {
		if errors.As(err, new(*serviceerror.NotFound)) {
			resp, _, _, err = chasm.NewEntity(
				ctx,
				chasm.EntityKey{
					NamespaceID: req.NamespaceId,
					BusinessID:  workerHeartbeat.WorkerInstanceKey,
				},
				func(ctx chasm.MutableContext, req *workerstatepb.RecordHeartbeatRequest) (*Worker, *workerstatepb.RecordHeartbeatResponse, error) {
					w := NewWorker()
					resp, err := w.recordHeartbeat(ctx, req)
					return w, resp, err
				},
				req,
			)
			if err != nil {
				return nil, err
			}
			return resp, nil
		}
		return nil, err
	}

	return resp, nil
}
