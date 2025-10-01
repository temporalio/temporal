package callbacks

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/api/common/v1"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/components/nexusoperations"
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
	encodedRef, ok := c.nexus.GetHeader()[chasm.NexusComponentRefHeader]
	if !ok {
		return invocationResultFail{errors.New("callback missing CHASM header")}
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
		return invocationResultRetry{fmt.Errorf("failed to complete Nexus operation: %v", err)}
	}

	return invocationResultOK{}
}

func (c chasmInvocation) getHistoryRequest(
	ref *persistencespb.ChasmComponentRef,
) (*historyservice.CompleteNexusOperationChasmRequest, error) {
	var req *historyservice.CompleteNexusOperationChasmRequest

	token := &tokenspb.NexusOperationChasmCompletion{
		NamespaceId: ref.NamespaceId,
		BusinessId:  ref.BusinessId,
		EntityId:    ref.EntityId,
		Ref:         ref,
		RequestId:   c.requestID,
	}

	switch op := c.completion.(type) {
	case *nexus.OperationCompletionSuccessful:
		links, err := convertNexusLinksToAPILinks(op.Links)
		if err != nil {
			return nil, fmt.Errorf("failed to convert Nexus links: %v", err)
		}

		payload, err := io.ReadAll(op.Reader)
		if err != nil {
			return nil, fmt.Errorf("failed to read payload: %v", err)
		}

		req = &historyservice.CompleteNexusOperationChasmRequest{
			State: string(nexus.OperationStateSucceeded),
			Outcome: &historyservice.CompleteNexusOperationChasmRequest_Success{
				Success: &common.Payload{
					Data: payload,
				},
			},
			OperationToken: op.OperationToken,
			StartTime:      timestamppb.New(op.StartTime),
			CloseTime:      timestamppb.New(op.CloseTime),
			Links:          links,
			Completion:     token,
		}
	case *nexus.OperationCompletionUnsuccessful:
		links, err := convertNexusLinksToAPILinks(op.Links)
		if err != nil {
			return nil, fmt.Errorf("failed to convert Nexus links: %v", err)
		}

		req = &historyservice.CompleteNexusOperationChasmRequest{
			Completion: token,
			State:      string(op.State),
			Outcome: &historyservice.CompleteNexusOperationChasmRequest_Failure{
				Failure: commonnexus.NexusFailureToProtoFailure(op.Failure),
			},
			OperationToken: op.OperationToken,
			StartTime:      timestamppb.New(op.StartTime),
			CloseTime:      timestamppb.New(op.CloseTime),
			Links:          links,
		}
	default:
		return nil, fmt.Errorf("unexpected nexus.OperationCompletion: %v", token)
	}

	return req, nil
}

func convertNexusLinksToAPILinks(nexusLinks []nexus.Link) (links []*commonpb.Link, err error) {
	for _, nexusLink := range nexusLinks {
		link, err := nexusoperations.ConvertNexusLinkToLinkWorkflowEvent(nexusLink)
		if err != nil {
			return nil, err
		}

		links = append(links, &commonpb.Link{
			Variant: &commonpb.Link_WorkflowEvent_{
				WorkflowEvent: link,
			},
		})
	}
	return links, nil
}
