package callbacks

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/service/history/queues"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type chasmInvocation struct {
	nexus      *persistencespb.Callback_Nexus
	attempt    int32
	completion nexus.OperationCompletion
	requestID  string
}

var ErrUnimplementedHandler = serviceerror.NewUnimplemented("component does not implement NexusCompletionHandler")

func (c chasmInvocation) WrapError(result invocationResult, err error) error {
	if failure, ok := result.(invocationResultFail); ok {
		return queues.NewUnprocessableTaskError(failure.err.Error())
	}

	if retry, ok := result.(invocationResultRetry); ok {
		return queues.NewDestinationDownError(retry.err.Error(), err)
	}

	return err
}

func (c chasmInvocation) Invoke(ctx context.Context, ns *namespace.Namespace, e taskExecutor, task InvocationTask) invocationResult {
	// Get back the base64-encoded ComponentRef from the header.
	encodedRef, ok := c.nexus.GetHeader()[commonnexus.CallbackTokenHeader]
	if !ok {
		return invocationResultFail{errors.New("callback missing token")}
	}

	decodedRef, err := base64.RawURLEncoding.DecodeString(encodedRef)
	if err != nil {
		return invocationResultFail{fmt.Errorf("failed to decode CHASM ComponentRef: %v", err)}
	}

	ref := &persistencespb.ChasmComponentRef{}
	err = proto.Unmarshal(decodedRef, ref)
	if err != nil {
		return invocationResultFail{fmt.Errorf("failed to unmarshal CHASM ComponentRef: %v", err)}
	}

	request, err := c.getHistoryRequest(ref)
	if err != nil {
		return invocationResultFail{fmt.Errorf("failed to build history request: %v", err)}
	}

	// RPC to History for cross-shard completion delivery.
	_, err = e.HistoryClient.CompleteNexusOperationChasm(ctx, request)
	if err != nil {
		msg := fmt.Errorf("failed to complete Nexus operation: %v", err)
		if isRetryableRpcResponse(err) {
			return invocationResultRetry{msg}
		}
		return invocationResultFail{msg}
	}

	return invocationResultOK{}
}

func (c chasmInvocation) getHistoryRequest(
	ref *persistencespb.ChasmComponentRef,
) (*historyservice.CompleteNexusOperationChasmRequest, error) {
	var req *historyservice.CompleteNexusOperationChasmRequest

	token := &tokenspb.NexusOperationChasmCompletion{
		Ref:       ref,
		RequestId: c.requestID,
	}

	switch op := c.completion.(type) {
	case *nexus.OperationCompletionSuccessful:
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
			Completion: token,
		}
	case *nexus.OperationCompletionUnsuccessful:
		apiFailure, err := commonnexus.NexusFailureToAPIFailure(op.Failure, true)
		if err != nil {
			return nil, fmt.Errorf("failed to convert failure type: %v", err)
		}

		req = &historyservice.CompleteNexusOperationChasmRequest{
			Completion: token,
			Outcome: &historyservice.CompleteNexusOperationChasmRequest_Failure{
				Failure: apiFailure,
			},
			CloseTime: timestamppb.New(op.CloseTime),
		}
	default:
		return nil, fmt.Errorf("unexpected nexus.OperationCompletion: %v", token)
	}

	return req, nil
}
