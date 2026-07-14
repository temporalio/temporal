package callbacks

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type chasmInvocation struct {
	nexus      *persistencespb.Callback_Nexus
	attempt    int32
	completion nexusrpc.CompleteOperationOptions
	requestID  string
}

func (c chasmInvocation) WrapError(result invocationResult, err error) error {
	return result.error()
}

// logInternalError emits a log statement for internalMsg, tagged with both
// internalErr and a reference-id. An opaque error containing the reference-id is
// returned. Intended to be used to hide internal errors from end users.
func logInternalError(logger log.Logger, internalMsg string, internalErr error) error {
	referenceID := uuid.NewString()
	logger.Error(internalMsg, tag.Error(internalErr), tag.String("reference-id", referenceID))
	return fmt.Errorf("internal error, reference-id: %v", referenceID)
}

func (c chasmInvocation) Invoke(ctx context.Context, ns *namespace.Namespace, e taskExecutor, task InvocationTask) invocationResult {
	// Get back the component ref and (optional) request ID from the callback token in the header.
	header := nexus.Header(c.nexus.GetHeader())
	if header == nil {
		header = nexus.Header{}
	}

	encodedToken := header.Get(commonnexus.CallbackTokenHeader)
	if encodedToken == "" {
		return invocationResultFail{logInternalError(e.Logger, "callback missing token", nil)}
	}

	ref, requestID, err := chasm.UnpackNexusCallbackToken(encodedToken)
	if err != nil {
		return invocationResultFail{logInternalError(e.Logger, "failed to decode CHASM callback token", err)}
	}
	// Older tokens don't carry a request ID; fall back to the one on the callback state machine.
	if requestID == "" {
		requestID = c.requestID
	}

	request, err := c.getHistoryRequest(ref, requestID)
	if err != nil {
		return invocationResultFail{logInternalError(e.Logger, "failed to build history request: %v", err)}
	}

	// RPC to History for cross-shard completion delivery.
	_, err = e.HistoryClient.CompleteNexusOperationChasm(ctx, request)
	if err != nil {
		redactedErr := logInternalError(e.Logger, "failed to complete Nexus operation: %v", err)
		if isRetryableRPCResponse(err) {
			return invocationResultRetry{redactedErr}
		}
		return invocationResultFail{redactedErr}
	}

	return invocationResultOK{}
}

func (c chasmInvocation) getHistoryRequest(
	ref []byte,
	requestID string,
) (*historyservice.CompleteNexusOperationChasmRequest, error) {
	var req *historyservice.CompleteNexusOperationChasmRequest

	completion := &tokenspb.NexusOperationCompletion{
		ComponentRef: ref,
		RequestId:    requestID,
	}

	if c.completion.Error == nil {
		var payload *commonpb.Payload
		if c.completion.Result != nil {
			var ok bool
			payload, ok = c.completion.Result.(*commonpb.Payload)
			if !ok {
				return nil, fmt.Errorf("invalid result, expected a payload, got: %T", c.completion.Result)
			}
		}

		req = &historyservice.CompleteNexusOperationChasmRequest{
			Outcome: &historyservice.CompleteNexusOperationChasmRequest_Success{
				Success: payload,
			},
			CloseTime:  timestamppb.New(c.completion.CloseTime),
			Completion: completion,
		}
	} else {
		failure, err := nexusrpc.DefaultFailureConverter().ErrorToFailure(c.completion.Error)
		if err != nil {
			return nil, fmt.Errorf("failed to convert error to failure: %w", err)
		}
		// Unwrap the operation error since it's not meant to be sent for Temporal->Temporal completions.
		if failure.Cause != nil {
			failure = *failure.Cause
		}
		apiFailure, err := commonnexus.NexusFailureToTemporalFailure(failure)
		if err != nil {
			return nil, fmt.Errorf("failed to convert failure type: %w", err)
		}

		req = &historyservice.CompleteNexusOperationChasmRequest{
			Completion: completion,
			Outcome: &historyservice.CompleteNexusOperationChasmRequest_Failure{
				Failure: apiFailure,
			},
			CloseTime: timestamppb.New(c.completion.CloseTime),
		}
	}

	return req, nil
}

func isRetryableRPCResponse(err error) bool {
	var st *status.Status
	stGetter, ok := err.(interface{ Status() *status.Status })
	if ok {
		st = stGetter.Status()
	} else {
		st, ok = status.FromError(err)
		if !ok {
			// Not a gRPC induced error
			return false
		}
	}
	// nolint:exhaustive
	switch st.Code() {
	case codes.Canceled,
		codes.Unknown,
		codes.Unavailable,
		codes.DeadlineExceeded,
		codes.ResourceExhausted,
		codes.Aborted,
		codes.Internal:
		return true
	default:
		return false
	}
}
