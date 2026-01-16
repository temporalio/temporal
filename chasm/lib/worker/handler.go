package worker

import (
	"context"
	"errors"
	"fmt"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

type handler struct {
	workerstatepb.UnimplementedWorkerServiceServer
	metricsHandler metrics.Handler
	logger         log.Logger
}

func newHandler(metricsHandler metrics.Handler, logger log.Logger) *handler {
	return &handler{
		metricsHandler: metricsHandler,
		logger:         logger,
	}
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

	// Try to update existing worker, or create new one if not found
	// newResp is set if a new worker is created, updateResp is set if an existing worker is updated
	newResp, updateResp, _, _, err := chasm.UpdateWithNewExecution(
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
		if _, ok := err.(*WorkerInactiveError); ok {
			return nil, serviceerror.NewFailedPrecondition(err.Error())
		}
		if tokenErr, ok := err.(*TokenMismatchError); ok {
			return nil, serviceerror.NewFailedPrecondition(fmt.Sprintf("%s: current token %x", err.Error(), tokenErr.CurrentToken))
		}
		return nil, err
	}

	// Return the response from whichever path was taken
	if newResp != nil {
		metrics.ChasmWorkerCreated.With(h.metricsHandler).Record(1)
		return newResp, nil
	}
	return updateResp, nil
}
