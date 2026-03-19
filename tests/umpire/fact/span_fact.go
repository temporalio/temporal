package fact

import (
	"go.opentelemetry.io/otel/attribute"
	"go.temporal.io/server/common/testing/umpire"
)

// SpanFact is a Fact that can be decoded from OTEL span event attributes.
// The OTEL event name must match the fact's Name() (which must match the struct name).
type SpanFact interface {
	umpire.Fact
	// ImportSpanEvent populates the fact from OTEL attributes.
	// Returns true if the fact is valid and should be emitted.
	ImportSpanEvent(attrs attribute.Set) bool
}

// RequestFact is a Fact that can be decoded from a gRPC request.
// ImportRequest uses a type assertion to check if the request is the expected
// type, returning false if not.
type RequestFact interface {
	umpire.Fact
	// ImportRequest populates the fact from a gRPC request.
	// Returns true if the request was the expected type and the fact is valid.
	ImportRequest(request any) bool
}
