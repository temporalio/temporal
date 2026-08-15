// Package monitor defines the Umpire Monitor contract used by the functional test harness.
package monitor

import (
	"context"
	"encoding/json"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/umpire"
	"google.golang.org/grpc"
)

// Monitor observes a functional cluster and checks the model state collected for each namespace.
type Monitor interface {
	sdktrace.SpanProcessor
	umpire.FactRecorder
	umpire.ResponseRecorder
	umpire.RejectionRecorder

	SetNamespaceID(name, id string)
	CheckNamespaceSafety(context.Context, string) []umpire.Violation
	CheckNamespace(context.Context, string) []umpire.Violation
	PurgeNamespace(string)
	Snapshot(string) umpire.Snapshot
	Observed(string, umpire.ObservationQuery) bool
	ArtifactFacts(string) ([]json.RawMessage, error)
	ObservationSummary(string) string
	PassedKeys(string) []string
	UnaryServerInterceptor(umpire.FaultInjector) grpc.UnaryServerInterceptor
}

// Factory constructs a Monitor for a functional test cluster.
type Factory func(log.Logger) (Monitor, error)
