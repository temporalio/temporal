package callback

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/service/history/queues"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type chasmInvocation struct {
	nexus      *callbackspb.Callback_Nexus
	attempt    int32
	completion nexusrpc.OperationCompletion
	requestID  string
}

func (c chasmInvocation) WrapError(result invocationResult, err error) error {
	if failure, ok := result.(invocationResultFail); ok {
		return queues.NewUnprocessableTaskError(failure.err.Error())
	}

	return result.error()
}

// logInternalError emits a log statement for internalMsg, tagged with both
// internalErr and a reference-id. An opaque error containing the reference-id is
// returned. Intended to be used to hide internal errors from end users.
func logInternalError(logger log.Logger, internalMsg string, internalErr error) error {
	referenceID := uuid.NewString()
	logger.Error(internalMsg, tag.Error(internalErr), tag.NewStringTag("reference-id", referenceID))
	return fmt.Errorf("internal error, reference-id: %v", referenceID)
}

func (c chasmInvocation) Invoke(
	ctx context.Context,
	ns *namespace.Namespace,
	e InvocationTaskExecutor,
	task *callbackspb.InvocationTask,
	taskAttr chasm.TaskAttributes,
) invocationResult {
	// Get back the base64-encoded ComponentRef from the header.
	encodedRef, ok := c.nexus.GetHeader()[commonnexus.CallbackTokenHeader]
	if !ok {
		return invocationResultFail{logInternalError(e.Logger, "callback missing token", nil)}
	}

	decodedRef, err := base64.RawURLEncoding.DecodeString(encodedRef)
	if err != nil {
		return invocationResultFail{logInternalError(e.Logger, "failed to decode CHASM ComponentRef", err)}
	}

	// Validate that the bytes are a valid ChasmComponentRef
	ref := &persistencespb.ChasmComponentRef{}
	err = proto.Unmarshal(decodedRef, ref)
	if err != nil {
		return invocationResultFail{logInternalError(e.Logger, "failed to unmarshal CHASM ComponentRef", err)}
	}

	request, err := c.getHistoryRequest(decodedRef)
	if err != nil {
		return invocationResultFail{logInternalError(e.Logger, "failed to build history request: %v", err)}
	}

	// RPC to History for cross-shard completion delivery.
	_, err = e.HistoryClient.CompleteNexusOperationChasm(ctx, request)
	if err != nil {
		msg := logInternalError(e.Logger, "failed to complete Nexus operation: %v", err)
		if isRetryableRPCResponse(err) {
			return invocationResultRetry{err: msg, retryPolicy: e.Config.RetryPolicy()}
		}
		return invocationResultFail{msg}
	}

	return invocationResultOK{}
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

func (c chasmInvocation) getHistoryRequest(
	refBytes []byte,
) (*historyservice.CompleteNexusOperationChasmRequest, error) {
	var req *historyservice.CompleteNexusOperationChasmRequest

	completion := &tokenspb.NexusOperationCompletion{
		ComponentRef: refBytes,
		RequestId:    c.requestID,
	}

	switch op := c.completion.(type) {
	case *nexusrpc.OperationCompletionSuccessful:
		payloadBody, err := io.ReadAll(op.Reader)
		if err != nil {
			return nil, fmt.Errorf("failed to read payload: %v", err)
		}

		var payload commonpb.Payload
		if payloadBody != nil {
			err := proto.Unmarshal(payloadBody, &payload)
			if err != nil {
				return nil, fmt.Errorf("failed to unmarshal payload body: %v", err)
			}
		}

		req = &historyservice.CompleteNexusOperationChasmRequest{
			Outcome: &historyservice.CompleteNexusOperationChasmRequest_Success{
				Success: &payload,
			},
			CloseTime:  timestamppb.New(op.CloseTime),
			Completion: completion,
		}
	case *nexusrpc.OperationCompletionUnsuccessful:
		apiFailure, err := commonnexus.NexusFailureToAPIFailure(op.Failure, true)
		if err != nil {
			return nil, fmt.Errorf("failed to convert failure type: %v", err)
		}

		req = &historyservice.CompleteNexusOperationChasmRequest{
			Completion: completion,
			Outcome: &historyservice.CompleteNexusOperationChasmRequest_Failure{
				Failure: apiFailure,
			},
			CloseTime: timestamppb.New(op.CloseTime),
		}
	default:
		return nil, fmt.Errorf("unexpected nexus.OperationCompletion: %v", completion)
	}

	return req, nil
}
