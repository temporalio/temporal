package testcore

import (
	"context"
	"sync"

	"go.temporal.io/server/common/contextutil"
	"google.golang.org/grpc"
)

// ContextMetadataRecord captures the context metadata state after a successful
// gRPC handler invocation. This mirrors what saas-temporal's metering
// interceptor reads from the context to attribute actions to workflows.
type ContextMetadataRecord struct {
	Method   string
	Metadata map[string]any
}

// ContextMetadataRecorder is a gRPC server interceptor that captures context
// metadata written by downstream services (via TrailerToContextMetadataInterceptor)
// after each successful request. Tests use this to verify that metadata like
// workflow-type and workflow-task-queue are correctly propagated through the
// frontend, matching the expectations of saas-temporal's metering layer.
type ContextMetadataRecorder struct {
	mu      sync.Mutex
	records []ContextMetadataRecord
}

// Intercept implements grpc.UnaryServerInterceptor. It runs the handler, then
// snapshots the context metadata map (the same map that the metering
// interceptor in saas-temporal would read post-handler).
func (r *ContextMetadataRecorder) Intercept(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	resp, err := handler(ctx, req)
	if err != nil {
		return resp, err
	}

	r.mu.Lock()
	r.records = append(r.records, ContextMetadataRecord{
		Method:   info.FullMethod,
		Metadata: contextutil.ContextMetadataGetAll(ctx),
	})
	r.mu.Unlock()

	return resp, err
}

// Records returns all captured metadata records and clears the internal buffer.
func (r *ContextMetadataRecorder) Records() []ContextMetadataRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.records
	r.records = nil
	return out
}

// LastRecord returns the most recently captured record matching the given
// method suffix (e.g. "DeleteSchedule") and whether one was found.
// It does not drain the buffer.
func (r *ContextMetadataRecorder) LastRecord(methodSuffix string) (ContextMetadataRecord, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := len(r.records) - 1; i >= 0; i-- {
		rec := r.records[i]
		if len(rec.Method) >= len(methodSuffix) &&
			rec.Method[len(rec.Method)-len(methodSuffix):] == methodSuffix {
			return rec, true
		}
	}
	return ContextMetadataRecord{}, false
}
