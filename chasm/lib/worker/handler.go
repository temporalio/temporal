package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
)

const (
	// Default lease duration for worker heartbeats.
	defaultLeaseDuration = 1 * time.Minute
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
		return nil, errors.New("exactly one worker heartbeat must be present in the request")
	}

	workerHeartbeat := frontendReq.GetWorkerHeartbeat()[0]

	executionKey := chasm.ExecutionKey{
		NamespaceID: req.NamespaceId,
		BusinessID:  workerHeartbeat.WorkerInstanceKey,
	}

	// TODO: Get lease duration from request once the proto supports it.
	leaseDuration := defaultLeaseDuration

	// Try to update existing worker, or create new one if not found
	_, _, _, _, err := chasm.UpdateWithNewExecution(
		ctx,
		executionKey,
		// newFn: called if worker doesn't exist
		func(ctx chasm.MutableContext, _ *workerstatepb.RecordHeartbeatRequest) (*Worker, []byte, error) {
			w := NewWorker()
			token, err := w.recordHeartbeat(ctx, workerHeartbeat, nil, leaseDuration)
			return w, token, err
		},
		// updateFn: called if worker exists
		func(w *Worker, ctx chasm.MutableContext, _ *workerstatepb.RecordHeartbeatRequest) ([]byte, error) {
			// TODO: Extract token from request once the proto supports it.
			return w.recordHeartbeat(ctx, workerHeartbeat, nil, leaseDuration)
		},
		req,
	)

	if err != nil {
		if _, ok := err.(*WorkerInactiveError); ok {
			return nil, serviceerror.NewFailedPrecondition(err.Error())
		}
		if tokenErr, ok := err.(*TokenMismatchError); ok {
			return nil, serviceerror.NewFailedPrecondition(fmt.Sprintf("%s: current token %x", err.Error(), tokenErr.CurrentToken))
		}
		return nil, err
	}

	return &workerstatepb.RecordHeartbeatResponse{}, nil
}
