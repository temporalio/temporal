package fact

import (
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/testing/umpire"
	"google.golang.org/grpc/codes"
)

// NexusOperationRejected is the fact for a StartNexusOperationExecution the server rejected
// synchronously — an invalid request that produced no operation and no telemetry (UMPIRE_ERR.md).
// Unlike the other Nexus facts (decoded from spans), it is synthesized from the request + error
// outcome by the decoder's ImportRejection. It routes to a NexusOperation entity keyed by the
// operation's request id under its namespace, so a rejection reaches the `rejected` terminal and is
// judged by the same Classify/Reconcile machinery as any other transition.
type NexusOperationRejected struct {
	RequestID   string // the operation's stable identity (the field the Oracle keys on)
	OperationID string // informational; may be empty when it was the mutated/invalid field
	NamespaceID string
	Code        string // the gRPC status-code name (e.g. "NotFound")
	EntityPath  *umpire.EntityPath
}

func (e *NexusOperationRejected) Name() string { return "NexusOperationRejected" }

func (e *NexusOperationRejected) TargetEntity() *umpire.EntityPath { return e.EntityPath }

// ImportRejection populates the fact from a rejected StartNexusOperationExecution (request + error
// + resolved namespace id). It returns false — no fact — unless the request is a Nexus start with a
// routable request id AND the error is a client-error class: a non-client failure (Internal/…) is
// deliberately not modeled as a clean rejection, so its absent transition surfaces as drift rather
// than a pass.
func (e *NexusOperationRejected) ImportRejection(req any, err error, namespaceID string) bool {
	r, ok := req.(*workflowservice.StartNexusOperationExecutionRequest)
	if !ok || r == nil || r.GetRequestId() == "" {
		return false
	}
	code, isClient := RejectionCode(err)
	if !isClient {
		return false
	}
	e.RequestID = r.GetRequestId()
	e.OperationID = r.GetOperationId()
	e.NamespaceID = namespaceID
	e.Code = code
	e.EntityPath = nsPath(namespaceID, umpire.NewEntityID(NexusOperationType, e.RequestID))
	return true
}

// clientErrorCodes are the gRPC status classes a well-formed rejection may carry: the request was
// refused for its content or the target's state, not because the server failed.
var clientErrorCodes = map[codes.Code]bool{
	codes.InvalidArgument:    true,
	codes.NotFound:           true,
	codes.AlreadyExists:      true,
	codes.FailedPrecondition: true,
	codes.PermissionDenied:   true,
	codes.Unauthenticated:    true,
	codes.OutOfRange:         true,
}

// RejectionCode returns the status-code name of err and whether it is a client-error class.
// serviceerror.ToStatus is Temporal's canonical error→gRPC-status conversion; it works on the
// in-process client's raw serviceerror, which grpc's status.FromError does not.
func RejectionCode(err error) (string, bool) {
	code := serviceerror.ToStatus(err).Code()
	return code.String(), clientErrorCodes[code]
}
